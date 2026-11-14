//! The free `devReplay` provider: replays `showcase_transcript.ndjson` (the same
//! ACP transcript the TS dev-replay agent serves) directly into domain events,
//! honoring each entry's `afterMs` delay.

use std::collections::HashMap;
use std::sync::Arc;

use serde_json::{json, Map, Value};
use tokio_util::sync::CancellationToken;

use crate::domain;
use crate::store::RunContext;
use crate::AppState;

const DRIVER: &str = "acpRegistry";

struct Turn {
    ctx: RunContext,
    next_ordinal: i64,
    /// item_id -> ordinal (allocated at creation, reused on updates).
    item_ordinals: HashMap<String, i64>,
    reasoning_item: Option<String>,
    assistant_item: Option<String>,
    assistant_message_id: Option<String>,
    assistant_text: String,
    reasoning_text: String,
    plan_id: Option<String>,
    /// toolCallId -> (itemId, nodeId).
    tool_items: HashMap<String, (String, String)>,
}

impl Turn {
    fn base_item(&mut self, node_id: &str, ty: &str) -> Map<String, Value> {
        let now = domain::now_iso();
        let id = domain::new_id();
        let ordinal = self.next_ordinal;
        self.next_ordinal += 1;
        self.item_ordinals.insert(id.clone(), ordinal);
        let mut o = Map::new();
        o.insert("id".into(), json!(id));
        o.insert("threadId".into(), json!(self.ctx.thread_id));
        o.insert("runId".into(), json!(self.ctx.run_id));
        o.insert("nodeId".into(), json!(node_id));
        o.insert("providerThreadId".into(), json!(self.ctx.provider_thread_id));
        o.insert("providerTurnId".into(), json!(self.ctx.provider_turn_id));
        o.insert("nativeItemRef".into(), Value::Null);
        o.insert("parentItemId".into(), Value::Null);
        o.insert("ordinal".into(), json!(ordinal));
        o.insert("status".into(), json!("running"));
        o.insert("title".into(), Value::Null);
        o.insert("startedAt".into(), json!(now));
        o.insert("completedAt".into(), Value::Null);
        o.insert("updatedAt".into(), json!(now));
        o.insert("type".into(), json!(ty));
        o
    }

    fn refresh_item(&self, item: &mut Value) {
        let now = domain::now_iso();
        if let Some(id) = item.get("id").and_then(Value::as_str) {
            if let Some(&ord) = self.item_ordinals.get(id) {
                item["ordinal"] = json!(ord);
            }
        }
        item["updatedAt"] = json!(now);
    }

    async fn commit(&self, state: &AppState, events: Vec<Value>) -> anyhow::Result<()> {
        let committed = state
            .store
            .commit_events(&self.ctx.thread_id, None, events)
            .await
            .map_err(anyhow::Error::msg)?;
        for ev in committed {
            let _ = state.events_tx.send(ev);
        }
        Ok(())
    }

    fn ev(&self, ty: &str, payload: Value, node_id: Option<&str>) -> Value {
        domain::thread_event(
            &self.ctx.thread_id,
            ty,
            payload,
            Some(&self.ctx.run_id),
            node_id,
            Some(DRIVER),
            Some(&self.ctx.provider_instance_id),
            Some(&domain::new_id()),
        )
    }

    fn tool_node(&self, node_id: &str, tool_call_id: &str, status: &str, done_at: Option<&str>) -> Value {
        json!({
            "id": node_id,
            "threadId": self.ctx.thread_id,
            "runId": self.ctx.run_id,
            "parentNodeId": self.ctx.root_node_id,
            "rootNodeId": self.ctx.root_node_id,
            "kind": "tool_call",
            "status": status,
            "countsForRun": true,
            "providerThreadId": self.ctx.provider_thread_id,
            "providerTurnId": self.ctx.provider_turn_id,
            "nativeItemRef": { "driver": DRIVER, "nativeId": tool_call_id, "strength": "strong" },
            "runtimeRequestId": null,
            "checkpointScopeId": null,
            "startedAt": done_at.map(|_| Value::Null).unwrap_or(json!(domain::now_iso())),
            "completedAt": done_at.map(Value::from).unwrap_or(Value::Null),
        })
    }
}

pub async fn run(
    state: &Arc<AppState>,
    ctx: &RunContext,
    cancel: &CancellationToken,
) -> anyhow::Result<()> {
    let path = state.replay_dir.join("showcase_transcript.ndjson");
    let transcript = std::fs::read_to_string(&path)
        .map_err(|e| anyhow::anyhow!("read replay transcript {}: {e}", path.display()))?;

    let start_ordinal = state
        .store
        .thread_projection(&ctx.thread_id)
        .await
        .and_then(|p| p.get("turnItems").cloned())
        .and_then(|v| v.as_array().cloned())
        .map(|items| {
            items
                .iter()
                .filter_map(|i| i.get("ordinal").and_then(Value::as_i64))
                .max()
                .unwrap_or(-1)
                + 1
        })
        .unwrap_or(0);

    let mut turn = Turn {
        ctx: ctx.clone(),
        next_ordinal: start_ordinal,
        item_ordinals: HashMap::new(),
        reasoning_item: None,
        assistant_item: None,
        assistant_message_id: None,
        assistant_text: String::new(),
        reasoning_text: String::new(),
        plan_id: None,
        tool_items: HashMap::new(),
    };

    for line in transcript.lines() {
        if cancel.is_cancelled() {
            return finish_turn(state, &mut turn, "interrupted").await;
        }
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let entry: Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(_) => continue,
        };
        if entry.get("type").and_then(Value::as_str) != Some("emit_inbound") {
            continue;
        }
        if let Some(ms) = entry.get("afterMs").and_then(Value::as_u64) {
            tokio::select! {
                _ = tokio::time::sleep(std::time::Duration::from_millis(ms)) => {}
                _ = cancel.cancelled() => return finish_turn(state, &mut turn, "interrupted").await,
            }
        }
        let frame = entry.get("frame").cloned().unwrap_or(Value::Null);
        match frame.get("method").and_then(Value::as_str).unwrap_or("") {
            "session/update" => {
                let update = frame.pointer("/params/update").cloned().unwrap_or(Value::Null);
                handle_session_update(state, &mut turn, &update).await?;
            }
            "session/request_permission" => {
                handle_permission(state, &mut turn, &frame).await?;
            }
            "session/prompt" => {
                return finish_turn(state, &mut turn, "completed").await;
            }
            _ => {}
        }
    }
    finish_turn(state, &mut turn, "completed").await
}

async fn handle_session_update(
    state: &Arc<AppState>,
    turn: &mut Turn,
    update: &Value,
) -> anyhow::Result<()> {
    match update.get("sessionUpdate").and_then(Value::as_str).unwrap_or("") {
        "agent_thought_chunk" => {
            let text = update.pointer("/content/text").and_then(Value::as_str).unwrap_or("");
            turn.reasoning_text.push_str(text);
            let mut item = if let Some(id) = turn.reasoning_item.clone() {
                let mut stored = state
                    .store
                    .get_turn_item(&turn.ctx.thread_id, &id)
                    .await
                    .and_then(|v| v.as_object().cloned())
                    .unwrap_or_default();
                stored.insert("text".into(), json!(turn.reasoning_text));
                stored.insert("streaming".into(), json!(true));
                Value::Object(stored)
            } else {
                let root = turn.ctx.root_node_id.clone();
                let mut item = turn.base_item(&root, "reasoning");
                let id = item.get("id").and_then(Value::as_str).unwrap_or("").to_string();
                turn.reasoning_item = Some(id);
                item.insert("text".into(), json!(turn.reasoning_text));
                item.insert("streaming".into(), json!(true));
                Value::Object(item)
            };
            turn.refresh_item(&mut item);
            turn.commit(
                state,
                vec![turn.ev("turn-item.updated", item, Some(&turn.ctx.root_node_id))],
            )
            .await?;
        }
        "agent_message_chunk" => {
            let text = update.pointer("/content/text").and_then(Value::as_str).unwrap_or("");
            turn.assistant_text.push_str(text);
            let is_new = turn.assistant_item.is_none();
            let msg_id = turn
                .assistant_message_id
                .clone()
                .unwrap_or_else(domain::new_id);
            let mut item = if !is_new {
                let id = turn.assistant_item.clone().unwrap();
                state
                    .store
                    .get_turn_item(&turn.ctx.thread_id, &id)
                    .await
                    .unwrap_or(Value::Null)
            } else {
                Value::Null
            };
            if item.is_null() {
                let root = turn.ctx.root_node_id.clone();
                let mut o = turn.base_item(&root, "assistant_message");
                let id = o.get("id").and_then(Value::as_str).unwrap_or("").to_string();
                turn.assistant_item = Some(id);
                turn.assistant_message_id = Some(msg_id.clone());
                o.insert("messageId".into(), json!(msg_id));
                o.insert("attachments".into(), json!([]));
                item = Value::Object(o);
            }
            item["text"] = json!(turn.assistant_text);
            item["streaming"] = json!(true);
            turn.refresh_item(&mut item);
            let message = json!({
                "createdBy": "agent",
                "creationSource": "provider",
                "id": msg_id,
                "threadId": turn.ctx.thread_id,
                "runId": turn.ctx.run_id,
                "nodeId": turn.ctx.root_node_id,
                "role": "assistant",
                "text": turn.assistant_text,
                "attachments": [],
                "streaming": true,
                "createdAt": domain::now_iso(),
                "updatedAt": domain::now_iso(),
            });
            turn.commit(
                state,
                vec![
                    turn.ev("message.updated", message, Some(&turn.ctx.root_node_id)),
                    turn.ev("turn-item.updated", item, Some(&turn.ctx.root_node_id)),
                ],
            )
            .await?;
        }
        "plan" => {
            let plan_id = turn.plan_id.clone().unwrap_or_else(domain::new_id);
            turn.plan_id = Some(plan_id.clone());
            let steps: Vec<Value> = update
                .get("entries")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default()
                .iter()
                .enumerate()
                .map(|(i, e)| {
                    json!({
                        "id": format!("step-{i}"),
                        "text": e.get("content").and_then(Value::as_str).unwrap_or(""),
                        "status": match e.get("status").and_then(Value::as_str).unwrap_or("pending") {
                            "in_progress" => "running",
                            s => s,
                        },
                    })
                })
                .collect();
            // Upsert the single todo_list item for this plan.
            let mut item = if let Some(existing) = find_plan_item(state, turn, &plan_id).await {
                existing
            } else {
                let root = turn.ctx.root_node_id.clone();
                let mut o = turn.base_item(&root, "todo_list");
                o.insert("planId".into(), json!(plan_id));
                Value::Object(o)
            };
            item["status"] = json!("completed");
            item["steps"] = Value::Array(steps.clone());
            turn.refresh_item(&mut item);
            let plan = json!({
                "id": plan_id,
                "threadId": turn.ctx.thread_id,
                "runId": turn.ctx.run_id,
                "nodeId": turn.ctx.root_node_id,
                "status": "active",
                "kind": "todo_list",
                "steps": steps,
            });
            turn.commit(
                state,
                vec![
                    turn.ev("plan.updated", plan, Some(&turn.ctx.root_node_id)),
                    turn.ev("turn-item.updated", item, Some(&turn.ctx.root_node_id)),
                ],
            )
            .await?;
        }
        "tool_call" => {
            let tool_call_id = update
                .get("toolCallId")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let title = update.get("title").and_then(Value::as_str).unwrap_or("");
            let tool_kind = update.get("kind").and_then(Value::as_str).unwrap_or("other");
            let raw_input = update.get("rawInput").cloned().unwrap_or(Value::Null);
            let status = map_acp_status(update.get("status").and_then(Value::as_str));
            let now = domain::now_iso();

            let node_id = domain::new_id();
            let mut node = turn.tool_node(&node_id, &tool_call_id, status, None);
            node["startedAt"] = json!(now);

            let mut item = turn.base_item(&node_id, "dynamic_tool");
            let item_id = item.get("id").and_then(Value::as_str).unwrap_or("").to_string();
            item.insert("status".into(), json!(status));
            item.insert("title".into(), json!(title));
            item.insert(
                "nativeItemRef".into(),
                json!({ "driver": DRIVER, "nativeId": tool_call_id, "strength": "strong" }),
            );
            match tool_kind {
                "execute" | "terminal" => {
                    item.insert("type".into(), json!("command_execution"));
                    item.insert(
                        "input".into(),
                        raw_input.get("command").cloned().unwrap_or(json!(title)),
                    );
                }
                "edit" | "write" | "move" | "delete" => {
                    item.insert("type".into(), json!("file_change"));
                    let diff = update
                        .get("content")
                        .and_then(Value::as_array)
                        .and_then(|a| {
                            a.iter()
                                .find(|c| c.get("type").and_then(Value::as_str) == Some("diff"))
                        })
                        .cloned();
                    let path = raw_input
                        .get("path")
                        .and_then(Value::as_str)
                        .or_else(|| {
                            diff.as_ref()
                                .and_then(|d| d.get("path").and_then(Value::as_str))
                        })
                        .unwrap_or(title);
                    item.insert("fileName".into(), json!(path));
                    if let Some(d) = diff {
                        if let Some(old) = d.get("oldText").and_then(Value::as_str) {
                            item.insert("oldStr".into(), json!(old));
                        }
                        if let Some(new) = d.get("newText").and_then(Value::as_str) {
                            item.insert("newStr".into(), json!(new));
                        }
                    }
                }
                _ => {
                    item.insert("toolName".into(), json!(tool_kind));
                    item.insert("input".into(), raw_input);
                }
            }
            turn.tool_items.insert(tool_call_id, (item_id, node_id.clone()));
            turn.commit(
                state,
                vec![
                    turn.ev("node.updated", node, Some(&node_id)),
                    turn.ev("turn-item.updated", Value::Object(item), Some(&node_id)),
                ],
            )
            .await?;
        }
        "tool_call_update" => {
            let tool_call_id = update
                .get("toolCallId")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let status = map_acp_status(update.get("status").and_then(Value::as_str));
            let Some((item_id, node_id)) = turn.tool_items.get(&tool_call_id).cloned() else {
                return Ok(());
            };
            let mut output_text = String::new();
            let mut diff: Option<Value> = None;
            for c in update
                .get("content")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default()
            {
                match c.get("type").and_then(Value::as_str) {
                    Some("content") => {
                        if let Some(t) = c.pointer("/content/text").and_then(Value::as_str) {
                            output_text.push_str(t);
                        }
                    }
                    Some("diff") => diff = Some(c),
                    _ => {}
                }
            }
            let now = domain::now_iso();
            let done = matches!(status, "completed" | "failed" | "cancelled");
            let mut item = state
                .store
                .get_turn_item(&turn.ctx.thread_id, &item_id)
                .await
                .unwrap_or(Value::Null);
            if item.is_null() {
                return Ok(());
            }
            item["status"] = json!(status);
            item["updatedAt"] = json!(now);
            if done {
                item["completedAt"] = json!(now);
            }
            match item.get("type").and_then(Value::as_str) {
                Some("command_execution") | Some("dynamic_tool") => {
                    if !output_text.is_empty() {
                        item["output"] = json!(output_text);
                    }
                }
                Some("file_change") => {
                    if let Some(d) = &diff {
                        if let Some(old) = d.get("oldText").and_then(Value::as_str) {
                            item["oldStr"] = json!(old);
                        }
                        if let Some(new) = d.get("newText").and_then(Value::as_str) {
                            item["newStr"] = json!(new);
                        }
                    }
                }
                _ => {}
            }
            turn.refresh_item(&mut item);
            let node = turn.tool_node(&node_id, &tool_call_id, status, done.then_some(now.as_str()));
            turn.commit(
                state,
                vec![
                    turn.ev("node.updated", node, Some(&node_id)),
                    turn.ev("turn-item.updated", item, Some(&node_id)),
                ],
            )
            .await?;
        }
        _ => {}
    }
    Ok(())
}

async fn find_plan_item(state: &Arc<AppState>, turn: &Turn, plan_id: &str) -> Option<Value> {
    let projection = state.store.thread_projection(&turn.ctx.thread_id).await?;
    projection
        .get("turnItems")
        .and_then(Value::as_array)?
        .iter()
        .find(|i| {
            i.get("type").and_then(Value::as_str) == Some("todo_list")
                && i.get("planId").and_then(Value::as_str) == Some(plan_id)
        })
        .cloned()
        .map(|mut item| {
            if let Some(id) = item.get("id").and_then(Value::as_str) {
                if let Some(&ord) = turn.item_ordinals.get(id) {
                    item["ordinal"] = json!(ord);
                }
            }
            item
        })
}

fn map_acp_status(s: Option<&str>) -> &'static str {
    match s.unwrap_or("pending") {
        "in_progress" => "running",
        "completed" => "completed",
        "failed" => "failed",
        "cancelled" | "canceled" => "cancelled",
        _ => "pending",
    }
}

async fn handle_permission(
    state: &Arc<AppState>,
    turn: &mut Turn,
    frame: &Value,
) -> anyhow::Result<()> {
    let tool_call = frame.pointer("/params/toolCall").cloned().unwrap_or(json!({}));
    let options = frame
        .pointer("/params/options")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let tool_call_id = tool_call
        .get("toolCallId")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let kind = tool_call.get("kind").and_then(Value::as_str).unwrap_or("other");
    let title = tool_call.get("title").and_then(Value::as_str).unwrap_or("");

    let request_kind = match kind {
        "execute" | "terminal" => "command",
        "edit" | "write" | "move" | "delete" => "file-change",
        "read" | "search" | "fetch" => "file-read",
        _ => "permission",
    };
    let opts: Vec<Value> = options
        .iter()
        .map(|o| {
            let k = o.get("kind").and_then(Value::as_str).unwrap_or("");
            let decision = match k {
                "allow_once" => "accept",
                "allow_always" => "acceptForSession",
                "reject_once" => "decline",
                "reject_always" => "cancel",
                _ => "cancel",
            };
            json!({
                "decision": decision,
                "label": o.get("name").and_then(Value::as_str).unwrap_or(decision),
            })
        })
        .collect();

    let request_id = domain::new_id();
    let node_id = domain::new_id();
    let now = domain::now_iso();

    let node = json!({
        "id": node_id,
        "threadId": turn.ctx.thread_id,
        "runId": turn.ctx.run_id,
        "parentNodeId": turn.ctx.root_node_id,
        "rootNodeId": turn.ctx.root_node_id,
        "kind": "approval_request",
        "status": "waiting",
        "countsForRun": true,
        "providerThreadId": turn.ctx.provider_thread_id,
        "providerTurnId": turn.ctx.provider_turn_id,
        "nativeItemRef": null,
        "runtimeRequestId": request_id,
        "checkpointScopeId": null,
        "startedAt": now,
        "completedAt": null,
    });
    let provider_turn_id = turn.ctx.provider_turn_id.clone();
    let request_payload = |status: &str, resolved_at: Value, decision: Value| {
        json!({
            "id": request_id,
            "nodeId": node_id,
            "providerTurnId": provider_turn_id.clone(),
            "nativeRequestRef": { "driver": DRIVER, "nativeId": tool_call_id, "strength": "weak" },
            "kind": request_kind,
            "status": status,
            "responseCapability": { "type": "not_resumable", "reason": "devReplay auto-answers" },
            "createdAt": now,
            "resolvedAt": resolved_at,
            "decision": decision,
        })
    };
    let mut item = turn.base_item(&node_id, "approval_request");
    item.insert("status".into(), json!("waiting"));
    item.insert("requestId".into(), json!(request_id));
    item.insert("requestKind".into(), json!(request_kind));
    item.insert("prompt".into(), json!(title));
    item.insert("options".into(), Value::Array(opts));

    turn.commit(
        state,
        vec![
            turn.ev("node.updated", node.clone(), Some(&node_id)),
            turn.ev(
                "runtime-request.updated",
                request_payload("pending", Value::Null, Value::Null),
                Some(&node_id),
            ),
            turn.ev("turn-item.updated", Value::Object(item.clone()), Some(&node_id)),
        ],
    )
    .await?;

    // devReplay auto-accepts the first allow_* option.
    let resolved_at = domain::now_iso();
    let resolved = request_payload("resolved", json!(resolved_at), json!("accept"));
    let mut node_done = node;
    node_done["status"] = json!("completed");
    node_done["completedAt"] = json!(resolved_at);
    let mut item_done = item;
    item_done["status"] = json!("completed");
    item_done["completedAt"] = json!(resolved_at);
    item_done["updatedAt"] = json!(resolved_at);
    turn.commit(
        state,
        vec![
            turn.ev("runtime-request.updated", resolved, Some(&node_id)),
            turn.ev("node.updated", node_done, Some(&node_id)),
            turn.ev("turn-item.updated", Value::Object(item_done), Some(&node_id)),
        ],
    )
    .await?;
    Ok(())
}

/// Close streaming items and mark run/attempt/root node/provider turn done.
async fn finish_turn(state: &AppState, turn: &mut Turn, status: &str) -> anyhow::Result<()> {
    let now = domain::now_iso();
    let mut events: Vec<Value> = vec![];
    let ctx = &turn.ctx;

    if let Some(item_id) = turn.reasoning_item.take() {
        if let Some(mut item) = state
            .store
            .get_turn_item(&ctx.thread_id, &item_id)
            .await
        {
            item["status"] = json!("completed");
            item["completedAt"] = json!(now);
            item["streaming"] = json!(false);
            item["updatedAt"] = json!(now);
            events.push(turn.ev("turn-item.updated", item, Some(&ctx.root_node_id)));
        }
    }
    if let (Some(item_id), Some(msg_id)) = (turn.assistant_item.take(), turn.assistant_message_id.take())
    {
        if let Some(mut item) = state
            .store
            .get_turn_item(&ctx.thread_id, &item_id)
            .await
        {
            item["status"] = json!("completed");
            item["completedAt"] = json!(now);
            item["streaming"] = json!(false);
            item["updatedAt"] = json!(now);
            events.push(turn.ev("turn-item.updated", item, Some(&ctx.root_node_id)));
        }
        let message = json!({
            "createdBy": "agent",
            "creationSource": "provider",
            "id": msg_id,
            "threadId": ctx.thread_id,
            "runId": ctx.run_id,
            "nodeId": ctx.root_node_id,
            "role": "assistant",
            "text": turn.assistant_text,
            "attachments": [],
            "streaming": false,
            "createdAt": domain::now_iso(),
            "updatedAt": now,
        });
        events.push(turn.ev("message.updated", message, Some(&ctx.root_node_id)));
    }

    // Tool-call nodes that never got a terminal update.
    let open_nodes: Vec<(String, String)> = turn
        .tool_items
        .iter()
        .map(|(tc, (_, n))| (tc.clone(), n.clone()))
        .collect();
    for (tool_call_id, node_id) in open_nodes {
        let node = turn.tool_node(&node_id, &tool_call_id, status, Some(&now));
        events.push(turn.ev("node.updated", node, Some(&node_id)));
    }

    let root_node = json!({
        "id": ctx.root_node_id,
        "threadId": ctx.thread_id,
        "runId": ctx.run_id,
        "parentNodeId": null,
        "rootNodeId": ctx.root_node_id,
        "kind": "root_turn",
        "status": status,
        "countsForRun": true,
        "providerThreadId": ctx.provider_thread_id,
        "providerTurnId": ctx.provider_turn_id,
        "nativeItemRef": null,
        "runtimeRequestId": null,
        "checkpointScopeId": null,
        "startedAt": null,
        "completedAt": now,
    });
    let attempt = json!({
        "id": ctx.attempt_id,
        "runId": ctx.run_id,
        "attemptOrdinal": 1,
        "rootNodeId": ctx.root_node_id,
        "providerInstanceId": ctx.provider_instance_id,
        "providerThreadId": ctx.provider_thread_id,
        "providerTurnId": ctx.provider_turn_id,
        "reason": "initial",
        "status": status,
        "startedAt": null,
        "completedAt": now,
    });
    let provider_turn = json!({
        "id": ctx.provider_turn_id,
        "providerThreadId": ctx.provider_thread_id,
        "nodeId": ctx.root_node_id,
        "runAttemptId": ctx.attempt_id,
        "nativeTurnRef": null,
        "ordinal": 1,
        "status": status,
        "startedAt": null,
        "completedAt": now,
    });
    let run = state
        .store
        .get_run(&ctx.thread_id, &ctx.run_id)
        .await
        .map(|mut r| {
            r["status"] = json!(status);
            r["completedAt"] = json!(now);
            r
        })
        .unwrap_or_else(|| {
            json!({
                "id": ctx.run_id,
                "threadId": ctx.thread_id,
                "status": status,
                "completedAt": now,
            })
        });
    let provider_thread = json!({
        "id": ctx.provider_thread_id,
        "driver": DRIVER,
        "providerInstanceId": ctx.provider_instance_id,
        "providerSessionId": null,
        "appThreadId": ctx.thread_id,
        "ownerNodeId": null,
        "nativeThreadRef": null,
        "nativeConversationHeadRef": null,
        "status": "idle",
        "firstRunOrdinal": null,
        "lastRunOrdinal": null,
        "handoffIds": [],
        "pendingBackgroundTasks": [],
        "contextUsage": null,
        "nativeMetadata": null,
        "goal": null,
        "createdAt": now,
        "updatedAt": now,
    });

    events.push(turn.ev("node.updated", root_node, Some(&ctx.root_node_id)));
    events.push(turn.ev("run-attempt.updated", attempt, Some(&ctx.root_node_id)));
    events.push(turn.ev("provider-turn.updated", provider_turn, Some(&ctx.root_node_id)));
    events.push(turn.ev("provider-thread.updated", provider_thread, None));
    events.push(turn.ev("run.updated", run, Some(&ctx.root_node_id)));

    turn.commit(state, events).await
}
