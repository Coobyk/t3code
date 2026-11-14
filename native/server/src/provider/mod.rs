//! Provider turn runners: translate a provider's output into committed domain
//! events. `thread.stop` cancels the per-thread task via `AppState.turns`.

use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use crate::store::RunContext;
use crate::AppState;

pub mod codex;
pub mod devin;
pub mod replay;

/// Begin the provider turn for a freshly created run. Registers a cancellation
/// token so `thread.stop` can interrupt it.
pub fn spawn_turn(state: Arc<AppState>, ctx: RunContext) {
    if ctx.dispatch_queued {
        return;
    }
    let cancel = CancellationToken::new();
    {
        let mut map = state.turns.lock().unwrap();
        map.entry(ctx.thread_id.clone())
            .or_default()
            .push(cancel.clone());
    }
    let st = state.clone();
    tokio::spawn(async move {
        let result = match ctx.provider_instance_id.as_str() {
            "devReplay" | "replay" => replay::run(&st, &ctx, &cancel).await,
            "codex" => codex::run(&st, &ctx, &cancel).await,
            // "devin"/"devin-acp" and any unregistered instance route to the
            // devin ACP provider for now.
            _ => devin::run(&st, &ctx, &cancel).await,
        };
        if let Err(e) = result {
            tracing::warn!(thread = %ctx.thread_id, %e, "provider turn failed");
            let _ = fail_run(&st, &ctx, &e.to_string()).await;
        }
        let mut map = st.turns.lock().unwrap();
        if let Some(v) = map.get_mut(&ctx.thread_id) {
            v.retain(|t| !std::ptr::eq(t, &cancel));
            if v.is_empty() {
                map.remove(&ctx.thread_id);
            }
        }
    });
}

/// Emit an `error` turn item and mark the run failed.
pub async fn fail_run(state: &Arc<AppState>, ctx: &RunContext, message: &str) -> anyhow::Result<()> {
    let now = crate::domain::now_iso();
    let item = serde_json::json!({
        "id": crate::domain::new_id(),
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
            "message": message,
            "code": "provider_turn_failed",
            "retryable": false,
        },
    });
    let run = state
        .store
        .get_run(&ctx.thread_id, &ctx.run_id)
        .await
        .map(|mut r| {
            r["status"] = serde_json::json!("failed");
            r["completedAt"] = serde_json::json!(now);
            r
        })
        .unwrap_or_else(|| {
            serde_json::json!({
                "id": ctx.run_id,
                "threadId": ctx.thread_id,
                "status": "failed",
                "completedAt": now,
            })
        });
    let events = vec![
        crate::domain::thread_event(
            &ctx.thread_id, "turn-item.updated", item,
            Some(&ctx.run_id), Some(&ctx.root_node_id), Some("acpRegistry"),
            Some(&ctx.provider_instance_id), None,
        ),
        crate::domain::thread_event(
            &ctx.thread_id, "run.updated", run,
            Some(&ctx.run_id), Some(&ctx.root_node_id), Some("acpRegistry"),
            Some(&ctx.provider_instance_id), None,
        ),
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

/// Cancel all in-flight turns for a thread (`thread.stop`).
pub fn cancel_turns(state: &AppState, thread_id: &str) {
    let tokens: Vec<CancellationToken> = {
        let mut map = state.turns.lock().unwrap();
        map.remove(thread_id).unwrap_or_default()
    };
    for t in tokens {
        t.cancel();
    }
}
