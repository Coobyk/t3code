//! Real provider: `devin acp --model <model>` — one subprocess per turn.
//! Speaks ACP (NDJSON JSON-RPC over stdio): initialize -> session/new ->
//! session/prompt, then translates session/update notifications into domain
//! events using the same machinery as the replay provider. thread.stop sends
//! session/cancel and kills the child; prompt resolution ends the turn.

use std::process::Stdio;
use std::sync::Arc;

use anyhow::Context;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{ChildStdin, Command};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::domain;
use crate::provider::replay::{finish_turn, handle_session_update, Turn};
use crate::store::RunContext;
use crate::AppState;

const DRIVER: &str = "devin";

/// JSON-RPC writer: each call is one newline-terminated frame on stdin.
struct RpcWriter {
    stdin: Mutex<ChildStdin>,
}

impl RpcWriter {
    async fn send(&self, frame: Value) -> anyhow::Result<()> {
        let mut stdin = self.stdin.lock().await;
        stdin.write_all(frame.to_string().as_bytes()).await?;
        stdin.write_all(b"\n").await?;
        stdin.flush().await?;
        Ok(())
    }

    async fn request(&self, id: i64, method: &str, params: Value) -> anyhow::Result<()> {
        self.send(json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        }))
        .await
    }

    async fn respond(&self, id: &Value, result: Value) -> anyhow::Result<()> {
        self.send(json!({ "jsonrpc": "2.0", "id": id, "result": result }))
            .await
    }

    async fn respond_err(&self, id: &Value, code: i64, message: &str) -> anyhow::Result<()> {
        self.send(json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": { "code": code, "message": message },
        }))
        .await
    }
}

/// Spawn `devin acp` and run the turn.
pub async fn run(
    state: &Arc<AppState>,
    ctx: &RunContext,
    cancel: &CancellationToken,
) -> anyhow::Result<()> {
    let model = ctx
        .model_selection
        .get("model")
        .and_then(Value::as_str)
        .filter(|m| !m.is_empty())
        .unwrap_or("swe-2-high");

    let mut child = Command::new("devin")
        .args(["acp", "--model", model])
        .current_dir(&ctx.workspace_root)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .context("spawn devin acp")?;
    let stdin = child.stdin.take().context("devin stdin")?;
    let stdout = child.stdout.take().context("devin stdout")?;
    let writer = RpcWriter {
        stdin: Mutex::new(stdin),
    };
    let mut lines = BufReader::new(stdout).lines();

    let mut turn = Turn::new(ctx.clone(), DRIVER, 1);
    let mut session_id = String::new();

    // RPC ids: 1=initialize, 2=session/new, 3=session/prompt.
    writer
        .request(
            1,
            "initialize",
            json!({
                "protocolVersion": 1,
                "clientCapabilities": {
                    "fs": { "readTextFile": true, "writeTextFile": true },
                    "terminal": false,
                },
            }),
        )
        .await?;
    let mut prompt_sent = false;
    let mut finished = false;

    loop {
        let msg = tokio::select! {
            _ = cancel.cancelled() => {
                if !session_id.is_empty() {
                    let _ = writer
                        .send(json!({
                            "jsonrpc": "2.0",
                            "method": "session/cancel",
                            "params": { "sessionId": session_id },
                        }))
                        .await;
                }
                let _ = child.kill().await;
                return finish_turn(state, &mut turn, "interrupted").await;
            }
            l = lines.next_line() => match l {
                Ok(Some(l)) => l,
                _ => break,
            },
        };
        let Ok(msg) = serde_json::from_str::<Value>(&msg) else {
            continue;
        };
        let method = msg.get("method").and_then(Value::as_str);
        let id = msg.get("id");

        // Server -> client request (has id + method).
        if let (Some(id), Some(method)) = (id, method) {
            let params = msg.get("params").cloned().unwrap_or(Value::Null);
            match method {
                "session/request_permission" => {
                    handle_permission(state, &mut turn, &writer, id, &params).await?;
                }
                "fs/read_text_file" => {
                    let path = params.get("path").and_then(Value::as_str).unwrap_or("");
                    match tokio::fs::read_to_string(path).await {
                        Ok(content) => {
                            let _ = writer.respond(id, json!({ "content": content })).await;
                        }
                        Err(_) => {
                            let _ = writer.respond_err(id, -32602, "file not found").await;
                        }
                    }
                }
                "fs/write_text_file" => {
                    let path = params.get("path").and_then(Value::as_str).unwrap_or("");
                    let content = params.get("content").and_then(Value::as_str).unwrap_or("");
                    match tokio::fs::write(path, content).await {
                        Ok(_) => {
                            let _ = writer.respond(id, json!({})).await;
                        }
                        Err(e) => {
                            let _ = writer.respond_err(id, -32603, &e.to_string()).await;
                        }
                    }
                }
                m if m.starts_with("terminal/") => {
                    let _ = writer
                        .respond_err(id, -32601, "terminal capability not supported")
                        .await;
                }
                _ => {
                    let _ = writer.respond_err(id, -32601, "unimplemented").await;
                }
            }
            continue;
        }

        // Response to one of our requests (has id, no method).
        if let Some(rid) = id.and_then(Value::as_i64) {
            match rid {
                1 => {
                    writer
                        .request(
                            2,
                            "session/new",
                            json!({ "cwd": ctx.workspace_root, "mcpServers": [] }),
                        )
                        .await?;
                }
                2 => {
                    session_id = msg
                        .pointer("/result/sessionId")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string();
                    if session_id.is_empty() {
                        anyhow::bail!("session/new returned no sessionId: {msg}");
                    }
                    writer
                        .request(
                            3,
                            "session/prompt",
                            json!({
                                "sessionId": session_id,
                                "prompt": [{ "type": "text", "text": ctx.prompt }],
                            }),
                        )
                        .await?;
                    prompt_sent = true;
                }
                3 => {
                    // session/prompt resolved: the turn is over.
                    let status = match msg
                        .pointer("/result/stopReason")
                        .and_then(Value::as_str)
                    {
                        Some("end_turn") => "completed",
                        Some("cancelled") => "interrupted",
                        Some("refusal") | Some("max_tokens") | Some("max_turn_requests") => "failed",
                        _ => "completed",
                    };
                    if let Some(err) = msg.get("error") {
                        tracing::warn!(%err, "devin session/prompt failed");
                        finish_turn(state, &mut turn, "failed").await?;
                    } else {
                        finish_turn(state, &mut turn, status).await?;
                    }
                    finished = true;
                    break;
                }
                _ => {}
            }
            continue;
        }

        // Notification.
        if method == Some("session/update") {
            let update = msg.pointer("/params/update").cloned().unwrap_or(Value::Null);
            handle_session_update(state, &mut turn, &update).await?;
        }
    }

    let status = child.wait().await?;
    if !finished {
        if prompt_sent && status.success() {
            finish_turn(state, &mut turn, "completed").await?;
        } else {
            finish_turn(state, &mut turn, "failed").await?;
        }
    }
    Ok(())
}

/// Emit approval_request events (same shape as the replay provider) and answer
/// the agent's permission request with the most permissive allow option, so
/// unattended dev turns can proceed; decisions are persisted in the stream.
async fn handle_permission(
    state: &AppState,
    turn: &mut Turn,
    writer: &RpcWriter,
    rpc_id: &Value,
    params: &Value,
) -> anyhow::Result<()> {
    let tool_call = params.get("toolCall").cloned().unwrap_or(json!({}));
    let options = params
        .get("options")
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
            "responseCapability": { "type": "not_resumable", "reason": "auto-approved for unattended turn" },
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

    // Auto-accept the most permissive allow option (allow_always > allow_once > first).
    let chosen = options
        .iter()
        .find(|o| o.get("kind").and_then(Value::as_str) == Some("allow_always"))
        .or_else(|| {
            options
                .iter()
                .find(|o| o.get("kind").and_then(Value::as_str) == Some("allow_once"))
        })
        .or_else(|| options.first())
        .cloned();
    let option_id = chosen
        .as_ref()
        .and_then(|o| o.get("optionId").and_then(Value::as_str))
        .unwrap_or("");
    let decision = match chosen
        .as_ref()
        .and_then(|o| o.get("kind").and_then(Value::as_str))
    {
        Some("allow_always") => "acceptForSession",
        _ => "accept",
    };
    writer
        .respond(
            rpc_id,
            json!({ "outcome": { "outcome": "selected", "optionId": option_id } }),
        )
        .await?;

    let resolved_at = domain::now_iso();
    let resolved = request_payload("resolved", json!(resolved_at), json!(decision));
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
