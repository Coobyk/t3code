//! Real provider: `codex exec --json` — one subprocess per turn. The CLI emits
//! NDJSON on stdout (`thread.started`, `turn.started`, `item.started/updated/
//! completed`, `turn.completed`, `turn.failed`, `error`); we translate each
//! into persisted domain events. `thread.stop` kills the child and finishes
//! the run as `interrupted`.

use std::collections::HashMap;
use std::iter::FromIterator;
use std::process::Stdio;
use std::sync::Arc;

use anyhow::Context;
use serde_json::{json, Map, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio_util::sync::CancellationToken;

use crate::domain;
use crate::store::RunContext;
use crate::AppState;

const DRIVER: &str = "codex";

struct Turn {
    ctx: RunContext,
    next_ordinal: i64,
    /// provider item id -> (turn item id, node id).
    items: HashMap<String, (String, String)>,
    assistant_message_id: Option<String>,
    assistant_text: String,
}

impl Turn {
    fn ev(&self, ty: &str, payload: Value, node_id: Option<&str>) -> Value {
        domain::thread_event(
            &self.ctx.thread_id,
            ty,
            payload,
            Some(&self.ctx.run_id),
            node_id,
            Some(DRIVER),
            Some(&self.ctx.provider_instance_id),
            None,
        )
    }

    /// Build a turn-item update keyed by the provider's item id. If the item
    /// already exists in the store, its original ordinal/startedAt are kept.
    async fn item(
        &mut self,
        state: &AppState,
        native_id: &str,
        kind: &str,
        status: &str,
        extra: Map<String, Value>,
    ) -> (String, Value) {
        let now = domain::now_iso();
        let (item_id, node_id) = self
            .items
            .entry(native_id.to_string())
            .or_insert_with(|| (domain::new_id(), domain::new_id()))
            .clone();
        let mut prev = state
            .store
            .get_turn_item(&self.ctx.thread_id, &item_id)
            .await;
        let ordinal = prev
            .as_ref()
            .and_then(|p| p.get("ordinal"))
            .and_then(Value::as_i64)
            .unwrap_or_else(|| {
                let o = self.next_ordinal;
                self.next_ordinal += 1;
                o
            });
        let started_at = prev
            .as_mut()
            .and_then(|p| p.get("startedAt").cloned())
            .unwrap_or_else(|| json!(now));
        let mut o = prev
            .and_then(|p| p.as_object().cloned())
            .unwrap_or_default();
        o.insert("id".into(), json!(item_id));
        o.insert("threadId".into(), json!(self.ctx.thread_id));
        o.insert("runId".into(), json!(self.ctx.run_id));
        o.insert("nodeId".into(), json!(node_id));
        o.insert("providerThreadId".into(), json!(self.ctx.provider_thread_id));
        o.insert("providerTurnId".into(), json!(self.ctx.provider_turn_id));
        o.insert("nativeItemRef".into(), json!(native_id));
        o.insert("parentItemId".into(), Value::Null);
        o.insert("ordinal".into(), json!(ordinal));
        o.insert("status".into(), json!(status));
        o.insert("title".into(), Value::Null);
        o.insert("startedAt".into(), started_at);
        if matches!(status, "completed" | "failed" | "cancelled" | "interrupted") {
            o.insert("completedAt".into(), json!(now));
        }
        o.insert("updatedAt".into(), json!(now));
        o.insert("type".into(), json!(kind));
        for (k, v) in extra {
            o.insert(k, v);
        }
        (node_id, Value::Object(o))
    }

    fn node(&self, native_id: &str, kind: &str, status: &str) -> Option<Value> {
        let (_, node_id) = self.items.get(native_id)?.clone();
        Some(json!({
            "id": node_id,
            "threadId": self.ctx.thread_id,
            "runId": self.ctx.run_id,
            "parentNodeId": self.ctx.root_node_id,
            "rootNodeId": self.ctx.root_node_id,
            "kind": kind,
            "status": status,
            "countsForRun": true,
            "providerThreadId": self.ctx.provider_thread_id,
            "providerTurnId": self.ctx.provider_turn_id,
            "nativeItemRef": native_id,
            "runtimeRequestId": null,
            "checkpointScopeId": null,
            "startedAt": null,
            "completedAt": domain::now_iso(),
        }))
    }
}

/// Spawn `codex exec --json` and stream its NDJSON events.
pub async fn run(
    state: &Arc<AppState>,
    ctx: &RunContext,
    cancel: &CancellationToken,
) -> anyhow::Result<()> {
    let model = ctx
        .model_selection
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or("");

    let mut cmd = Command::new("codex");
    cmd.arg("exec")
        .arg("--json")
        .arg("--skip-git-repo-check")
        .arg("-C")
        .arg(&ctx.workspace_root)
        .arg("-s")
        .arg("workspace-write")
        .arg("-")
        .current_dir(&ctx.workspace_root)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    if !model.is_empty() && model != "dev-replay" {
        cmd.arg("-m").arg(model);
    }
    let mut child = cmd.spawn().context("spawn codex exec")?;
    if let Some(mut stdin) = child.stdin.take() {
        let _ = stdin.write_all(ctx.prompt.as_bytes()).await;
    }
    let stdout = child.stdout.take().context("codex exec stdout")?;
    let mut lines = BufReader::new(stdout).lines();

    let mut turn = Turn {
        ctx: ctx.clone(),
        next_ordinal: 1, // ordinal 0 is the user message
        items: HashMap::new(),
        assistant_message_id: None,
        assistant_text: String::new(),
    };
    let mut events: Vec<Value> = Vec::new();
    let mut finished = false;

    loop {
        let line = tokio::select! {
            _ = cancel.cancelled() => {
                let _ = child.kill().await;
                flush(state, &mut events).await?;
                finish(state, &mut turn, "interrupted").await?;
                return Ok(());
            }
            line = lines.next_line() => match line {
                Ok(Some(l)) => l,
                _ => break,
            },
        };
        let Ok(ev) = serde_json::from_str::<Value>(&line) else {
            continue; // non-JSON noise on stdout
        };
        match ev.get("type").and_then(Value::as_str) {
            Some("thread.started") => {
                let external = ev.get("thread_id").and_then(Value::as_str).unwrap_or("");
                events.push(turn.ev(
                    "provider-thread.updated",
                    json!({
                        "id": ctx.provider_thread_id,
                        "driver": DRIVER,
                        "providerInstanceId": ctx.provider_instance_id,
                        "providerSessionId": null,
                        "appThreadId": ctx.thread_id,
                        "ownerNodeId": null,
                        "externalThreadId": external,
                        "nativeThreadRef": external,
                        "status": "active",
                        "title": null,
                    }),
                    Some(&ctx.root_node_id),
                ));
            }
            Some("turn.started") => {
                events.push(turn.ev(
                    "provider-turn.updated",
                    json!({
                        "id": ctx.provider_turn_id,
                        "providerThreadId": ctx.provider_thread_id,
                        "nodeId": ctx.root_node_id,
                        "runAttemptId": ctx.attempt_id,
                        "nativeTurnRef": null,
                        "ordinal": 1,
                        "status": "running",
                        "startedAt": domain::now_iso(),
                        "completedAt": null,
                    }),
                    Some(&ctx.root_node_id),
                ));
            }
            Some("item.started") | Some("item.updated") | Some("item.completed") => {
                if let Some(item) = ev.get("item") {
                    events.extend(item_event(state, &mut turn, item).await);
                }
            }
            Some("turn.completed") => {
                finished = true;
                flush(state, &mut events).await?;
                finish(state, &mut turn, "completed").await?;
            }
            Some("turn.failed") => {
                finished = true;
                let msg = ev
                    .pointer("/error/message")
                    .and_then(Value::as_str)
                    .unwrap_or("codex turn failed")
                    .to_string();
                flush(state, &mut events).await?;
                emit_error_item(state, &mut turn, &msg).await;
                finish(state, &mut turn, "failed").await?;
            }
            Some("error") => {
                // Reconnect warnings etc. — surface as a system notice, keep going.
                let msg = ev.get("message").and_then(Value::as_str).unwrap_or("codex error");
                let node_id = turn.ctx.root_node_id.clone();
                let (_nid, item) = turn
                    .item(
                        state,
                        &format!("notice-{}", turn.next_ordinal),
                        "system_notice",
                        "completed",
                        Map::from_iter([(
                            "notice".into(),
                            json!({ "level": "warning", "message": msg }),
                        )]),
                    )
                    .await;
                events.push(turn.ev("turn-item.updated", item, Some(&node_id)));
            }
            _ => {}
        }
        if events.len() >= 8 {
            flush(state, &mut events).await?;
        }
    }
    flush(state, &mut events).await?;

    let status = child.wait().await?;
    if !finished {
        finish(
            state,
            &mut turn,
            if status.success() { "completed" } else { "failed" },
        )
        .await?;
    }
    Ok(())
}

/// Map a codex exec item to turn-item/node/message domain events.
async fn item_event(state: &AppState, turn: &mut Turn, item: &Value) -> Vec<Value> {
    let native_id = item
        .get("id")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    if native_id.is_empty() {
        return vec![];
    }
    let ty = item
        .get("item_type")
        .and_then(Value::as_str)
        .or_else(|| item.get("type").and_then(Value::as_str))
        .unwrap_or("");
    let status = item
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("running");
    let node_status = match status {
        "completed" => "completed",
        "failed" => "failed",
        "declined" => "cancelled",
        _ => "running",
    };
    let mut out = Vec::new();

    let (node_kind, extra): (&str, Map<String, Value>) = match ty {
        "reasoning" => (
            "reasoning",
            Map::from_iter([
                ("text".into(), item.get("text").cloned().unwrap_or(Value::Null)),
                ("detailFormat".into(), json!("text")),
            ]),
        ),
        "agent_message" => {
            let text = item.get("text").and_then(Value::as_str).unwrap_or("").to_string();
            turn.assistant_text = text.clone();
            if turn.assistant_message_id.is_none() {
                turn.assistant_message_id = Some(domain::new_id());
            }
            (
                "assistant_message",
                Map::from_iter([
                    ("messageId".into(), json!(turn.assistant_message_id)),
                    ("text".into(), json!(text)),
                    ("streaming".into(), json!(status == "in_progress")),
                ]),
            )
        }
        "command_execution" => (
            "tool_call",
            Map::from_iter([
                (
                    "command".into(),
                    item.get("command").cloned().unwrap_or(Value::Null),
                ),
                (
                    "result".into(),
                    json!({
                        "output": item.get("aggregated_output").cloned().unwrap_or(Value::Null),
                        "exitCode": item.get("exit_code").cloned().unwrap_or(Value::Null),
                    }),
                ),
            ]),
        ),
        "file_change" => (
            "tool_call",
            Map::from_iter([(
                "changes".into(),
                item.get("changes").cloned().unwrap_or_else(|| json!([])),
            )]),
        ),
        "mcp_tool_call" | "dynamic_tool_call" | "custom_tool_call" | "collab_agent_tool_call"
        | "collab_tool_call" => (
            "tool_call",
            Map::from_iter([
                (
                    "server".into(),
                    item.get("server").cloned().unwrap_or(Value::Null),
                ),
                (
                    "tool".into(),
                    item.get("tool")
                        .or_else(|| item.get("name"))
                        .cloned()
                        .unwrap_or(json!(ty)),
                ),
                (
                    "arguments".into(),
                    item.get("arguments").cloned().unwrap_or(Value::Null),
                ),
                (
                    "result".into(),
                    item.get("result").cloned().unwrap_or(Value::Null),
                ),
            ]),
        ),
        "web_search" => (
            "tool_call",
            Map::from_iter([(
                "query".into(),
                item.get("query").cloned().unwrap_or(Value::Null),
            )]),
        ),
        "todo_list" => (
            "todo_list",
            Map::from_iter([(
                "items".into(),
                item.get("items").cloned().unwrap_or_else(|| json!([])),
            )]),
        ),
        "error" => (
            "system",
            Map::from_iter([(
                "failure".into(),
                json!({
                    "class": "provider_error",
                    "message": item.get("message").and_then(Value::as_str).unwrap_or("codex error"),
                    "code": "codex_item_error",
                    "retryable": false,
                }),
            )]),
        ),
        _ => (
            "tool_call",
            Map::from_iter([
                ("raw".into(), item.clone()),
                ("tool".into(), json!(ty)),
            ]),
        ),
    };

    let kind = match ty {
        "agent_message" => "assistant_message",
        "reasoning" => "reasoning",
        "todo_list" => "todo_list",
        "command_execution" => "command_execution",
        "file_change" => "file_change",
        "web_search" => "web_search",
        "error" => "error",
        _ => "dynamic_tool",
    };

    let (node_id, ti) = turn.item(state, &native_id, kind, status, extra).await;
    out.push(turn.ev("turn-item.updated", ti, Some(&node_id)));
    if let Some(node) = turn.node(&native_id, node_kind, node_status) {
        out.push(turn.ev("node.updated", node, Some(&node_id)));
    }
    if ty == "agent_message" {
        if let Some(msg_id) = turn.assistant_message_id.clone() {
            let text = turn.assistant_text.clone();
            out.push(turn.ev(
                "message.updated",
                json!({
                    "createdBy": "agent",
                    "creationSource": "provider",
                    "id": msg_id,
                    "threadId": turn.ctx.thread_id,
                    "runId": turn.ctx.run_id,
                    "nodeId": turn.ctx.root_node_id,
                    "role": "assistant",
                    "text": text,
                    "attachments": [],
                    "streaming": status == "in_progress",
                    "createdAt": domain::now_iso(),
                    "updatedAt": domain::now_iso(),
                }),
                Some(&turn.ctx.root_node_id),
            ));
        }
    }
    out
}

async fn emit_error_item(state: &AppState, turn: &mut Turn, message: &str) {
    let node_id = turn.ctx.root_node_id.clone();
    let (_nid, item) = turn
        .item(
            state,
            &format!("err-{}", turn.next_ordinal),
            "error",
            "failed",
            Map::from_iter([(
                "failure".into(),
                json!({
                    "class": "provider_error",
                    "message": message,
                    "code": "codex_turn_failed",
                    "retryable": false,
                }),
            )]),
        )
        .await;
    let mut events = vec![turn.ev("turn-item.updated", item, Some(&node_id))];
    let _ = flush(state, &mut events).await;
}

async fn flush(state: &AppState, events: &mut Vec<Value>) -> anyhow::Result<()> {
    if events.is_empty() {
        return Ok(());
    }
    let batch = std::mem::take(events);
    let committed = state
        .store
        .commit_events(&turn_thread(&batch), None, batch)
        .await
        .map_err(anyhow::Error::msg)?;
    for ev in committed {
        let _ = state.events_tx.send(ev);
    }
    Ok(())
}

fn turn_thread(batch: &[Value]) -> String {
    batch
        .first()
        .and_then(|e| e.get("threadId"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string()
}

/// Close out the turn: open message, root node, attempt, provider turn, run.
async fn finish(state: &AppState, turn: &mut Turn, status: &str) -> anyhow::Result<()> {
    let now = domain::now_iso();
    let ctx = &turn.ctx;
    let mut events: Vec<Value> = vec![];

    // Interrupt any items still open so they don't render as spinning forever.
    for native_id in turn.items.keys().cloned().collect::<Vec<_>>() {
        if let Some((item_id, node_id)) = turn.items.get(&native_id).cloned() {
            if let Some(mut item) = state.store.get_turn_item(&ctx.thread_id, &item_id).await {
                if !matches!(
                    item.get("status").and_then(Value::as_str),
                    Some("completed" | "failed" | "cancelled" | "interrupted")
                ) {
                    item["status"] = json!(status);
                    item["completedAt"] = json!(now);
                    item["updatedAt"] = json!(now);
                    item["streaming"] = json!(false);
                    events.push(turn.ev("turn-item.updated", item, Some(&node_id)));
                }
            }
            if let Some(mut node) = turn.node(&native_id, "tool_call", status) {
                node["status"] = json!(status);
                events.push(turn.ev("node.updated", node, Some(&node_id)));
            }
        }
    }

    if let Some(msg_id) = turn.assistant_message_id.take() {
        let text = std::mem::take(&mut turn.assistant_text);
        events.push(turn.ev(
            "message.updated",
            json!({
                "createdBy": "agent",
                "creationSource": "provider",
                "id": msg_id,
                "threadId": ctx.thread_id,
                "runId": ctx.run_id,
                "nodeId": ctx.root_node_id,
                "role": "assistant",
                "text": text,
                "attachments": [],
                "streaming": false,
                "createdAt": now,
                "updatedAt": now,
            }),
            Some(&ctx.root_node_id),
        ));
    }

    events.push(turn.ev(
        "node.updated",
        json!({
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
        }),
        Some(&ctx.root_node_id),
    ));
    events.push(turn.ev(
        "run-attempt.updated",
        json!({
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
        }),
        Some(&ctx.root_node_id),
    ));
    events.push(turn.ev(
        "provider-turn.updated",
        json!({
            "id": ctx.provider_turn_id,
            "providerThreadId": ctx.provider_thread_id,
            "nodeId": ctx.root_node_id,
            "runAttemptId": ctx.attempt_id,
            "nativeTurnRef": null,
            "ordinal": 1,
            "status": status,
            "startedAt": null,
            "completedAt": now,
        }),
        Some(&ctx.root_node_id),
    ));
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
    events.push(turn.ev("run.updated", run, Some(&ctx.root_node_id)));
    events.push(turn.ev(
        "provider-thread.updated",
        json!({
            "id": ctx.provider_thread_id,
            "driver": DRIVER,
            "providerInstanceId": ctx.provider_instance_id,
            "providerSessionId": null,
            "appThreadId": ctx.thread_id,
            "ownerNodeId": null,
            "externalThreadId": null,
            "status": "idle",
        }),
        Some(&ctx.root_node_id),
    ));

    let committed = state
        .store
        .commit_events(&ctx.thread_id, None, events)
        .await
        .map_err(anyhow::Error::msg)?;
    for ev in committed {
        let _ = state.events_tx.send(ev);
    }
    Ok(())
}
