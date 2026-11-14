//! Real provider path. For now, providers other than `devReplay` fail with a
//! visible error item; an ACP stdio client lands next.

use std::sync::Arc;

use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;

use crate::domain;
use crate::store::RunContext;
use crate::AppState;

const DRIVER: &str = "acpRegistry";

pub async fn run(
    state: &Arc<AppState>,
    ctx: &RunContext,
    _cancel: &CancellationToken,
) -> anyhow::Result<()> {
    // Emit an error turn item and fail the run so the UI shows something real.
    let now = domain::now_iso();
    let item = json!({
        "id": domain::new_id(),
        "threadId": ctx.thread_id,
        "runId": ctx.run_id,
        "nodeId": ctx.root_node_id,
        "providerThreadId": ctx.provider_thread_id,
        "providerTurnId": ctx.provider_turn_id,
        "nativeItemRef": null,
        "parentItemId": null,
        "ordinal": 0,
        "status": "failed",
        "title": null,
        "startedAt": now,
        "completedAt": now,
        "updatedAt": now,
        "type": "error",
        "failure": {
            "class": "provider_error",
            "message": format!("provider '{}' is not supported by the Rust server yet", ctx.provider_instance_id),
            "code": "unsupported_provider",
            "retryable": false,
        },
    });
    let run = state
        .store
        .get_run(&ctx.thread_id, &ctx.run_id)
        .await
        .map(|mut r| {
            r["status"] = json!("failed");
            r["completedAt"] = json!(now);
            r
        })
        .unwrap_or_else(|| json!({ "id": ctx.run_id, "threadId": ctx.thread_id, "status": "failed", "completedAt": now }));
    let events = vec![
        domain::thread_event(&ctx.thread_id, "turn-item.updated", item, Some(&ctx.run_id), Some(&ctx.root_node_id), Some(DRIVER), Some(&ctx.provider_instance_id), None),
        domain::thread_event(&ctx.thread_id, "run.updated", run, Some(&ctx.run_id), Some(&ctx.root_node_id), Some(DRIVER), Some(&ctx.provider_instance_id), None),
    ];
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

/// Make a placeholder `Value` happy in unused paths.
#[allow(dead_code)]
fn _unused(_v: Value) {}
