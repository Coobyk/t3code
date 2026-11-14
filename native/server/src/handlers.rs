//! RPC method handlers: windowed subscription streams and one-shot requests.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{json, Map, Value};

use crate::domain;
use crate::provider;
use crate::store::CommittedEvent;
use crate::ws_rpc::{
    exit_failure, exit_success, send_json, OutTx, Window,
};
use crate::AppState;

pub fn is_windowed_tag(tag: &str) -> bool {
    matches!(
        tag,
        "orchestration.subscribeShell"
            | "orchestration.subscribeThread"
            | "orchestration.subscribeServerConfig"
    )
}

pub struct RpcResult(pub Result<Value, Value>);

impl RpcResult {
    pub fn ok(v: Value) -> Self {
        Self(Ok(v))
    }
    pub fn err(v: Value) -> Self {
        Self(Err(v))
    }
    pub fn to_exit(self, request_id: u64) -> Value {
        match self.0 {
            Ok(v) => exit_success(request_id, v),
            Err(e) => exit_failure(request_id, e),
        }
    }
}

pub async fn handle_request(state: Arc<AppState>, tag: &str, payload: Value) -> RpcResult {
    match tag {
        "orchestration.dispatchCommand" => dispatch_command(state, payload).await,
        "orchestration.getOlderSettledThreads" => {
            let before = payload
                .get("before")
                .and_then(|b| b.get("settledAt"))
                .and_then(Value::as_str)
                .unwrap_or("2999-01-01T00:00:00.000Z")
                .to_string();
            let limit = payload.get("limit").and_then(Value::as_i64).unwrap_or(50);
            let project_ids = payload.get("projectIds").and_then(Value::as_array).map(|a| {
                a.iter().filter_map(Value::as_str).map(str::to_owned).collect()
            });
            let threads = state.store.older_settled(&before, project_ids, limit).await;
            RpcResult::ok(json!({ "threads": threads }))
        }
        "orchestration.getThreadProjection" => {
            let thread_id = payload.get("threadId").and_then(Value::as_str).unwrap_or("");
            match state.store.thread_projection(thread_id).await {
                Some(p) => RpcResult::ok(domain::project_projection_for_wire(&p)),
                None => RpcResult::err(json!({
                    "_tag": "OrchestrationV2GetThreadProjectionError",
                    "threadId": thread_id,
                    "message": "Thread not found",
                })),
            }
        }
        "orchestration.getTurnItem" => RpcResult::err(json!({
            "_tag": "OrchestrationV2GetTurnItemError",
            "message": "not implemented",
        })),
        other => RpcResult::err(json!({
            "_tag": "RpcClientError",
            "message": format!("unknown rpc tag: {other}"),
        })),
    }
}

async fn dispatch_command(state: Arc<AppState>, payload: Value) -> RpcResult {
    let stop_thread = if payload.get("type").and_then(Value::as_str) == Some("thread.stop") {
        payload.get("threadId").and_then(Value::as_str).map(str::to_owned)
    } else {
        None
    };
    if let Some(tid) = &stop_thread {
        provider::cancel_turns(&state, tid);
    }
    let result = state.store.apply_command(payload).await;
    match result {
        Err(e) => RpcResult::err(json!({
            "_tag": "OrchestrationV2DispatchError",
            "message": e,
        })),
        Ok(applied) => {
            for ev in &applied.events {
                let _ = state.events_tx.send(ev.clone());
            }
            if let Some(ctx) = applied.run_ctx {
                provider::spawn_turn(state, ctx);
            }
            RpcResult::ok(json!({ "sequence": applied.sequence }))
        }
    }
}

// ---------------------------------------------------------------------------
// Subscription streams
// ---------------------------------------------------------------------------

pub async fn run_stream(
    state: Arc<AppState>,
    out: OutTx,
    request_id: u64,
    tag: &str,
    payload: Value,
    window: Arc<Window>,
) {
    match tag {
        "orchestration.subscribeShell" => {
            shell_stream(state, out, request_id, payload, window).await;
        }
        "orchestration.subscribeThread" => {
            thread_stream(state, out, request_id, payload, window).await;
        }
        "orchestration.subscribeServerConfig" => {
            // Minimal config payload; native client ignores this stream.
            let providers = json!({
                "kind": "config",
                "providers": [],
            });
            let _ = window.send_chunk(&out, request_id, vec![providers]).await;
        }
        _ => {}
    }
}

/// Resolve git identity roots for a project workspace: a directory containing
/// `.git` counts as one resolved root.
fn repo_roots_for(projects: &[Value]) -> Vec<String> {
    projects
        .iter()
        .filter_map(|p| {
            let root = p.get("workspaceRoot").and_then(Value::as_str)?;
            std::path::Path::new(root).join(".git").exists().then(|| root.to_string())
        })
        .collect()
}

async fn shell_stream(
    state: Arc<AppState>,
    out: OutTx,
    request_id: u64,
    payload: Value,
    window: Arc<Window>,
) {
    let omit_idle_subagents = payload.get("omitIdleSubagents").and_then(Value::as_bool) == Some(true);
    let omit_older_settled = payload.get("omitOlderSettled").and_then(Value::as_bool) == Some(true);
    let marker = payload.get("requestCompletionMarker").and_then(Value::as_bool) == Some(true);

    // Subscribe before snapshotting so live events after the snapshot sequence
    // cannot be missed.
    let mut rx = state.events_tx.subscribe();
    let snapshot = state
        .store
        .shell_snapshot_opts(omit_idle_subagents, omit_older_settled)
        .await;
    let snapshot_seq = snapshot.get("snapshotSequence").and_then(Value::as_i64).unwrap_or(0);

    // Item 1: authoritative snapshot.
    if !window
        .send_chunk(&out, request_id, vec![json!({ "kind": "snapshot", "snapshot": snapshot.clone() })])
        .await
    {
        return;
    }

    // Item 2 (optional): enrichment snapshot with resolved repository roots.
    let projects = snapshot.get("projects").and_then(Value::as_array).cloned().unwrap_or_default();
    let roots = repo_roots_for(&projects);
    if !roots.is_empty() {
        let filtered: Vec<Value> = projects
            .iter()
            .filter(|p| {
                p.get("workspaceRoot")
                    .and_then(Value::as_str)
                    .is_some_and(|r| roots.iter().any(|x| x == r))
            })
            .cloned()
            .collect();
        let mut enrich = snapshot.as_object().cloned().unwrap_or_default();
        enrich.insert("projects".into(), Value::Array(filtered));
        enrich.insert("threads".into(), json!([]));
        enrich.insert("archivedThreads".into(), json!([]));
        let item = json!({
            "kind": "snapshot",
            "snapshot": enrich,
            "resolvedRepositoryIdentityRoots": roots,
        });
        if !window.send_chunk(&out, request_id, vec![item]).await {
            return;
        }
    }

    if marker && !window.send_chunk(&out, request_id, vec![json!({ "kind": "synchronized" })]).await {
        return;
    }

    // Live tail.
    let mut last_sent: HashMap<String, (Value, Instant)> = HashMap::new();
    loop {
        let stored = match rx.recv().await {
            Ok(e) => e,
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
            Err(_) => break,
        };
        if window.is_cancelled() {
            return;
        }
        let items = shell_items_for_event(&state, &stored, snapshot_seq, &mut last_sent).await;
        for item in items {
            if !window.send_chunk(&out, request_id, vec![item]).await {
                return;
            }
        }
    }
}

const UNCHANGED_RESEND: Duration = Duration::from_millis(5_000);

async fn shell_items_for_event(
    state: &AppState,
    stored: &CommittedEvent,
    snapshot_seq: i64,
    last_sent: &mut HashMap<String, (Value, Instant)>,
) -> Vec<Value> {
    if stored.sequence <= snapshot_seq {
        return vec![];
    }
    if stored.aggregate_kind == "project" {
        let ty = stored.event.get("type").and_then(Value::as_str).unwrap_or("");
        if ty == "project.deleted" {
            return vec![json!({
                "kind": "project.removed",
                "sequence": stored.sequence,
                "projectId": stored.stream_id,
            })];
        }
        return match state.store.project_shell(&stored.stream_id).await {
            Some(p) => vec![json!({
                "kind": "project.updated",
                "sequence": stored.sequence,
                "project": p,
            })],
            None => vec![json!({
                "kind": "project.removed",
                "sequence": stored.sequence,
                "projectId": stored.stream_id,
            })],
        };
    }

    if stored.aggregate_kind != "thread" {
        return vec![];
    }
    let thread_id = &stored.stream_id;
    match state.store.thread_shell(thread_id).await {
        None => vec![json!({
            "kind": "thread.removed",
            "sequence": stored.sequence,
            "location": "active",
            "threadId": thread_id,
        })],
        Some(shell) => {
            // Archived threads leave the active list (shellStreamItemFromThreadShell).
            if !shell.get("archivedAt").unwrap_or(&Value::Null).is_null()
                || !shell.get("deletedAt").unwrap_or(&Value::Null).is_null()
            {
                last_sent.remove(thread_id);
                return vec![json!({
                    "kind": "thread.removed",
                    "sequence": stored.sequence,
                    "location": "active",
                    "threadId": shell.get("id").cloned().unwrap_or(json!(thread_id)),
                })];
            }
            // skipUnchangedThreadShells: resend unchanged shells only after 5s.
            let now = Instant::now();
            if let Some((prev, sent_at)) = last_sent.get(thread_id) {
                if now.duration_since(*sent_at) < UNCHANGED_RESEND {
                    let mut comparable = shell.clone();
                    if let (Some(a), Some(b)) = (
                        comparable.as_object_mut().and_then(|o| o.get_mut("updatedAt")),
                        prev.get("updatedAt"),
                    ) {
                        *a = b.clone();
                    }
                    if comparable == *prev {
                        return vec![];
                    }
                }
            }
            last_sent.insert(thread_id.clone(), (shell.clone(), now));
            vec![json!({
                "kind": "thread.updated",
                "sequence": stored.sequence,
                "location": "active",
                "thread": shell,
            })]
        }
    }
}

async fn thread_stream(
    state: Arc<AppState>,
    out: OutTx,
    request_id: u64,
    payload: Value,
    window: Arc<Window>,
) {
    let thread_id = payload.get("threadId").and_then(Value::as_str).unwrap_or("").to_string();
    let after = payload.get("afterSequence").and_then(Value::as_i64);
    let marker = payload.get("requestCompletionMarker").and_then(Value::as_bool) == Some(true);
    if thread_id.is_empty() {
        send_json(&out, &exit_failure(request_id, json!({
            "_tag": "OrchestrationV2GetThreadProjectionError",
            "message": "missing threadId",
        })));
        return;
    }

    let mut rx = state.events_tx.subscribe();

    if after.is_none() {
        // Snapshot path: emit the projection then tail live events.
        let snapshot_seq = state.store.latest_sequence().await;
        match state.store.thread_projection(&thread_id).await {
            Some(projection) => {
                let wire = domain::project_projection_for_wire(&projection);
                let item = json!({
                    "kind": "snapshot",
                    "snapshot": {
                        "snapshotSequence": snapshot_seq,
                        "projection": wire,
                    },
                });
                if !window.send_chunk(&out, request_id, vec![item]).await {
                    return;
                }
            }
            None => {
                send_json(&out, &exit_failure(request_id, json!({
                    "_tag": "OrchestrationV2GetThreadProjectionError",
                    "threadId": thread_id,
                    "message": "Failed to load orchestration V2 thread",
                })));
                return;
            }
        }
        if marker
            && !window
                .send_chunk(&out, request_id, vec![json!({ "kind": "synchronized" })])
                .await
        {
            return;
        }
        // Live tail for events after the snapshot sequence.
        loop {
            let stored = match rx.recv().await {
                Ok(e) => e,
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                Err(_) => break,
            };
            if window.is_cancelled() {
                return;
            }
            if stored.aggregate_kind != "thread"
                || stored.stream_id != thread_id
                || stored.sequence <= snapshot_seq
            {
                continue;
            }
            let item = json!({
                "kind": "event",
                "sequence": stored.sequence,
                "event": domain::project_domain_event_for_wire(&stored.event),
            });
            if !window.send_chunk(&out, request_id, vec![item]).await {
                return;
            }
        }
    } else {
        // Replay path: stored events after `afterSequence`, then live.
        let after_seq = after.unwrap_or(0);
        let stored = state.store.events_for_thread(&thread_id, after_seq).await;
        let mut high_water = after_seq;
        for ev in stored {
            high_water = high_water.max(ev.sequence);
            let item = json!({
                "kind": "event",
                "sequence": ev.sequence,
                "event": domain::project_domain_event_for_wire(&ev.event),
            });
            if !window.send_chunk(&out, request_id, vec![item]).await {
                return;
            }
        }
        if marker
            && !window
                .send_chunk(&out, request_id, vec![json!({ "kind": "synchronized" })])
                .await
        {
            return;
        }
        loop {
            let stored = match rx.recv().await {
                Ok(e) => e,
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                Err(_) => break,
            };
            if window.is_cancelled() {
                return;
            }
            if stored.aggregate_kind != "thread"
                || stored.stream_id != thread_id
                || stored.sequence <= high_water
            {
                continue;
            }
            high_water = stored.sequence;
            let item = json!({
                "kind": "event",
                "sequence": stored.sequence,
                "event": domain::project_domain_event_for_wire(&stored.event),
            });
            if !window.send_chunk(&out, request_id, vec![item]).await {
                return;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Seeding
// ---------------------------------------------------------------------------

pub async fn ensure_seed_project(state: &Arc<AppState>) -> anyhow::Result<()> {
    if !state.store.list_projects().await.is_empty() {
        return Ok(());
    }
    let root = std::env::var("C67_PROJECT_DIR")
        .or_else(|_| std::env::var("CODE67_PROJECT_DIR"))
        .unwrap_or_else(|_| {
            std::env::current_dir()
                .map(|p| p.to_string_lossy().to_string())
                .unwrap_or_else(|_| "/".into())
        });
    let model = std::env::var("C67_DEFAULT_MODEL").ok().map(|m| {
        json!({ "instanceId": "devReplay", "model": m })
    }).or_else(|| Some(json!({ "instanceId": "devReplay", "model": "dev-replay" })));
    state
        .store
        .seed_project("local", "Local workspace", &root, model)
        .await;
    tracing::info!(%root, "seeded local project");
    Ok(())
}

/// Small map helper for ordered JSON objects.
#[allow(dead_code)]
pub fn obj(fields: &[(&str, Value)]) -> Value {
    let mut m = Map::new();
    for (k, v) in fields {
        m.insert((*k).to_string(), v.clone());
    }
    Value::Object(m)
}
