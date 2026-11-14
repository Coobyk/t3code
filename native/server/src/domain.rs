//! Domain JSON builders matching `packages/contracts/src/orchestrationV2.ts`.
//! All wire objects are produced as `serde_json::Value` with the exact field
//! names the TS contracts define.

use serde_json::{json, Map, Value};
use time::format_description::FormatItem;
use time::OffsetDateTime;

const ISO_FORMAT: &[FormatItem<'static>] = time::macros::format_description!(
    "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3]Z"
);

pub fn now_iso() -> String {
    OffsetDateTime::now_utc().format(ISO_FORMAT).unwrap()
}

pub fn new_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

pub const PROJECTION_SCHEMA_VERSION: i64 = 2;

/// Wire limits from `apps/server/src/orchestration-v2/WireProjection.ts`.
const MAX_DETAIL_STRING_BYTES: usize = 32_768;
const MAX_DYNAMIC_VALUE_BYTES: usize = 16_384;
const MAX_WIRE_CHECKPOINT_FILES: usize = 200;
const TRUNCATION_MARKER: &str = "\n… output truncated for transport";

fn truncate_detail(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes && value.as_bytes().len() <= max_bytes {
        return value.to_string();
    }
    let budget = max_bytes - TRUNCATION_MARKER.len();
    let mut end = budget.min(value.len());
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}{}", &value[..end], TRUNCATION_MARKER)
}

fn tool_output_indicates_failure(output: &str) -> bool {
    // Mirrors @t3tools/shared/toolOutput heuristics loosely: obvious failure
    // markers in command output text.
    let lower = output.to_lowercase();
    lower.contains("exit code")
        || lower.contains("command failed")
        || lower.contains("enoent")
        || lower.contains("permission denied")
        || lower.contains("traceback (most recent call last)")
        || lower.contains("error:")
}

/// `projectTurnItemForWire` from WireProjection.ts.
pub fn project_turn_item_for_wire(item: &Value) -> Value {
    let mut obj = match item.as_object() {
        Some(o) => o.clone(),
        None => return item.clone(),
    };
    match obj.get("type").and_then(Value::as_str) {
        Some("checkpoint") => {
            cap_checkpoint_files(&mut obj);
            Value::Object(obj)
        }
        Some("handoff") => {
            obj.remove("summary");
            Value::Object(obj)
        }
        Some("command_execution") => {
            let output = obj.remove("output");
            let output_str = output.as_ref().and_then(Value::as_str);
            let failed = match obj.get("exitCode").and_then(Value::as_i64) {
                None => {
                    obj.get("outputIndicatesFailure").and_then(Value::as_bool) == Some(true)
                        || output_str
                            .map(|o| tool_output_indicates_failure(&o[..o.len().min(MAX_DETAIL_STRING_BYTES)]))
                            .unwrap_or(false)
                }
                Some(code) => code != 0,
            };
            if failed {
                obj.insert("outputIndicatesFailure".into(), json!(true));
            }
            if output_str.map(|o| !o.trim().is_empty()).unwrap_or(false) {
                obj.insert("outputOmitted".into(), json!(true));
            }
            Value::Object(obj)
        }
        Some("file_change") => {
            let diff = obj.remove("diffStr");
            obj.remove("oldStr");
            obj.remove("newStr");
            if obj.get("status").and_then(Value::as_str) == Some("failed") {
                if let Some(d) = diff.as_ref().and_then(Value::as_str) {
                    if !d.trim().is_empty() {
                        obj.insert("diffStr".into(), json!(truncate_detail(d, MAX_DETAIL_STRING_BYTES)));
                    }
                }
            }
            Value::Object(obj)
        }
        Some("subagent") => {
            if let Some(p) = obj.get("prompt").and_then(Value::as_str) {
                obj.insert("prompt".into(), json!(truncate_detail(p, MAX_DETAIL_STRING_BYTES)));
            }
            if let Some(p) = obj.get("progress").and_then(Value::as_str) {
                obj.insert("progress".into(), json!(truncate_detail(p, MAX_DETAIL_STRING_BYTES)));
            }
            if let Some(r) = obj.get("result").and_then(Value::as_str) {
                obj.insert("result".into(), json!(truncate_detail(r, MAX_DETAIL_STRING_BYTES)));
            }
            Value::Object(obj)
        }
        Some("dynamic_tool") => {
            let raw_output = obj.remove("output");
            let input = obj.get("input").cloned().unwrap_or(Value::Null);
            obj.insert("input".into(), summarize_dynamic_value(&input));
            if let Some(out) = raw_output.as_ref() {
                obj.insert("output".into(), summarize_dynamic_value(out));
            }
            if raw_output.as_ref().map(has_dynamic_value).unwrap_or(false) {
                obj.insert("outputOmitted".into(), json!(true));
            }
            Value::Object(obj)
        }
        _ => Value::Object(obj),
    }
}

fn has_dynamic_value(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::String(s) => !s.trim().is_empty(),
        Value::Array(a) => !a.is_empty(),
        Value::Object(o) => !o.is_empty(),
        _ => true,
    }
}

fn summarize_dynamic_value(value: &Value) -> Value {
    let serialized: String = match value {
        Value::String(s) if s.len() > MAX_DYNAMIC_VALUE_BYTES => s.clone(),
        other => {
            let json = serde_json::to_string(other).unwrap_or_else(|_| "null".into());
            if json.len() <= MAX_DYNAMIC_VALUE_BYTES {
                return other.clone();
            }
            match other {
                Value::String(s) => s.clone(),
                _ => json,
            }
        }
    };
    let mut first_line = String::new();
    let mut pending_space = false;
    for ch in serialized.trim_start().chars() {
        if ch == '\n' || first_line.len() > 160 {
            break;
        }
        if ch.is_whitespace() {
            pending_space = true;
            continue;
        }
        if pending_space {
            first_line.push(' ');
        }
        first_line.push(ch);
        pending_space = false;
    }
    let summary = if first_line.len() <= 160 {
        first_line
    } else {
        let mut s: String = first_line.chars().take(159).collect();
        while s.ends_with(char::is_whitespace) {
            s.pop();
        }
        s.push('…');
        s
    };
    json!({ "summary": summary, "truncated": true })
}

fn cap_checkpoint_files(obj: &mut Map<String, Value>) {
    let files_len = obj
        .get("files")
        .and_then(Value::as_array)
        .map(|a| a.len())
        .unwrap_or(0);
    if files_len <= MAX_WIRE_CHECKPOINT_FILES || obj.get("filesTotal").is_some() {
        return;
    }
    let files = obj.get("files").and_then(Value::as_array).cloned().unwrap_or_default();
    let mut additions = 0i64;
    let mut deletions = 0i64;
    for f in &files {
        additions += f.get("additions").and_then(Value::as_i64).unwrap_or(0);
        deletions += f.get("deletions").and_then(Value::as_i64).unwrap_or(0);
    }
    obj.insert("files".into(), Value::Array(files[..MAX_WIRE_CHECKPOINT_FILES].to_vec()));
    obj.insert(
        "filesTotal".into(),
        json!({ "count": files_len, "additions": additions, "deletions": deletions }),
    );
}

/// `projectDomainEventForWire` — wire projection for `turn-item.updated` /
/// `context-handoff.updated` / `checkpoint.captured` payloads.
pub fn project_domain_event_for_wire(event: &Value) -> Value {
    let mut ev = match event.as_object() {
        Some(o) => o.clone(),
        None => return event.clone(),
    };
    match ev.get("type").and_then(Value::as_str) {
        Some("turn-item.updated") => {
            if let Some(p) = ev.get("payload") {
                ev.insert("payload".into(), project_turn_item_for_wire(p));
            }
        }
        Some("context-handoff.updated") => {
            if let Some(p) = ev.get("payload").and_then(Value::as_object) {
                let mut p = p.clone();
                p.remove("history");
                p.remove("delivery");
                p.insert("summaryText".into(), json!(""));
                ev.insert("payload".into(), Value::Object(p));
            }
        }
        Some("checkpoint.captured") => {
            if let Some(p) = ev.get("payload").and_then(Value::as_object) {
                let mut p = p.clone();
                cap_checkpoint_files(&mut p);
                ev.insert("payload".into(), Value::Object(p));
            }
        }
        _ => {}
    }
    Value::Object(ev)
}

/// `projectThreadProjectionForWire`.
pub fn project_projection_for_wire(projection: &Value) -> Value {
    let mut p = match projection.as_object() {
        Some(o) => o.clone(),
        None => return projection.clone(),
    };
    if let Some(items) = p.get("checkpoints").and_then(Value::as_array) {
        let out: Vec<Value> = items
            .iter()
            .map(|c| {
                let mut o = c.as_object().cloned().unwrap_or_default();
                cap_checkpoint_files(&mut o);
                Value::Object(o)
            })
            .collect();
        p.insert("checkpoints".into(), Value::Array(out));
    }
    if let Some(items) = p.get("contextHandoffs").and_then(Value::as_array) {
        let out: Vec<Value> = items
            .iter()
            .map(|h| {
                let mut o = h.as_object().cloned().unwrap_or_default();
                o.remove("history");
                o.remove("delivery");
                o.insert("summaryText".into(), json!(""));
                Value::Object(o)
            })
            .collect();
        p.insert("contextHandoffs".into(), Value::Array(out));
    }
    if let Some(items) = p.get("turnItems").and_then(Value::as_array) {
        p.insert(
            "turnItems".into(),
            Value::Array(items.iter().map(project_turn_item_for_wire).collect()),
        );
    }
    if let Some(rows) = p.get("visibleTurnItems").and_then(Value::as_array) {
        let out: Vec<Value> = rows
            .iter()
            .map(|row| {
                let mut r = row.as_object().cloned().unwrap_or_default();
                if let Some(item) = r.get("item") {
                    r.insert("item".into(), project_turn_item_for_wire(item));
                }
                Value::Object(r)
            })
            .collect();
        p.insert("visibleTurnItems".into(), Value::Array(out));
    }
    Value::Object(p)
}

// ---------------------------------------------------------------------------
// Shell objects
// ---------------------------------------------------------------------------

/// `isListedThreadLineage`.
pub fn is_listed_lineage(lineage: &Value) -> bool {
    lineage.get("relationshipToParent").and_then(Value::as_str) != Some("subagent")
        && lineage.get("sideChat").and_then(Value::as_bool) != Some(true)
}

/// Project shell row → `OrchestrationV2ProjectShell`-shaped JSON.
/// Columns: project_id,title,workspace_root,scripts_json,created_at,updated_at,
/// deleted_at,default_model_selection_json,default_thread_env_mode,
/// favicon_path,auto_pull,project_icon_json
pub fn project_shell(
    project_id: &str,
    title: &str,
    workspace_root: &str,
    scripts_json: &str,
    created_at: &str,
    updated_at: &str,
    default_model_selection_json: Option<&str>,
    default_thread_env_mode: Option<&str>,
    project_icon_json: Option<&str>,
    repository_identity: Option<&Value>,
) -> Value {
    let scripts: Value =
        serde_json::from_str(scripts_json).unwrap_or(Value::Array(vec![]));
    let mut obj = Map::new();
    obj.insert("id".into(), json!(project_id));
    obj.insert("title".into(), json!(title));
    obj.insert("workspaceRoot".into(), json!(workspace_root));
    obj.insert(
        "defaultModelSelection".into(),
        default_model_selection_json
            .and_then(|s| serde_json::from_str(s).ok())
            .unwrap_or(Value::Null),
    );
    obj.insert(
        "defaultThreadEnvMode".into(),
        default_thread_env_mode.map(Value::from).unwrap_or(Value::Null),
    );
    obj.insert("scripts".into(), scripts);
    if let Some(icon) = project_icon_json {
        if let Ok(v) = serde_json::from_str::<Value>(icon) {
            obj.insert("icon".into(), v);
        }
    }
    obj.insert(
        "repositoryIdentity".into(),
        repository_identity.cloned().unwrap_or(Value::Null),
    );
    obj.insert("repository".into(), Value::Null);
    obj.insert("createdAt".into(), json!(created_at));
    obj.insert("updatedAt".into(), json!(updated_at));
    Value::Object(obj)
}

/// Build the `OrchestrationV2ThreadShell` JSON from the stored AppThread plus
/// derived aggregates (latest run, pending request, message summary, counts).
pub struct ThreadDerived {
    pub latest_run: Option<Value>,
    pub active_run: Option<Value>,
    pub pending_request: Option<Value>,
    pub latest_visible_message: Option<Value>,
    pub latest_user_message_at: Option<String>,
    pub item_count: i64,
    pub visible_item_count: i64,
    pub has_actionable_proposed_plan: bool,
    pub pending_background_tasks: Vec<Value>,
    pub provider_instance_history: Vec<Value>,
}

/// Build the shell object the shell stream emits. `app` is the stored
/// AppThread payload.
pub fn thread_shell(app: &Value, d: &ThreadDerived) -> Value {
    let mut obj = app.as_object().cloned().unwrap_or_default();

    // Drop AppThread-only fields the shell does not carry.
    for k in [
        "lastSnoozeWakeAt",
        "guestShare",
        "rollbackRequestId",
        "rollbackFailure",
    ] {
        obj.remove(k);
    }

    let latest_run = d.latest_run.as_ref();
    let status = latest_run
        .and_then(|r| r.get("status").and_then(Value::as_str))
        .unwrap_or("idle");
    let active_run = d.active_run.as_ref();

    obj.insert(
        "latestRunId".into(),
        latest_run
            .and_then(|r| r.get("id").cloned())
            .unwrap_or(Value::Null),
    );
    obj.insert(
        "latestRunRequestedAt".into(),
        latest_run
            .and_then(|r| r.get("requestedAt").cloned())
            .unwrap_or(Value::Null),
    );
    obj.insert(
        "latestRunStartedAt".into(),
        latest_run
            .and_then(|r| r.get("startedAt").cloned())
            .unwrap_or(Value::Null),
    );
    obj.insert(
        "latestRunCompletedAt".into(),
        latest_run
            .and_then(|r| r.get("completedAt").cloned())
            .unwrap_or(Value::Null),
    );
    obj.insert(
        "activeRunId".into(),
        active_run
            .and_then(|r| r.get("id").cloned())
            .unwrap_or(Value::Null),
    );
    obj.insert(
        "activityRunStartedAt".into(),
        active_run
            .and_then(|r| r.get("startedAt").cloned())
            .unwrap_or(Value::Null),
    );
    obj.insert(
        "activityRunStatus".into(),
        active_run
            .and_then(|r| r.get("status").cloned())
            .unwrap_or(Value::Null),
    );
    obj.insert("status".into(), json!(status));
    obj.insert(
        "pendingRuntimeRequest".into(),
        d.pending_request.clone().unwrap_or(Value::Null),
    );
    obj.insert(
        "latestVisibleMessage".into(),
        d.latest_visible_message.clone().unwrap_or(Value::Null),
    );
    obj.insert(
        "latestUserMessageAt".into(),
        d.latest_user_message_at
            .as_ref()
            .map(|s| json!(s))
            .unwrap_or(Value::Null),
    );
    obj.insert(
        "hasActionableProposedPlan".into(),
        json!(d.has_actionable_proposed_plan),
    );
    obj.insert(
        "pendingBackgroundTasks".into(),
        Value::Array(d.pending_background_tasks.clone()),
    );
    obj.insert(
        "providerInstanceHistory".into(),
        Value::Array(d.provider_instance_history.clone()),
    );
    obj.insert("itemCount".into(), json!(d.item_count));
    obj.insert("visibleItemCount".into(), json!(d.visible_item_count));
    Value::Object(obj)
}

/// `isOlderSettledThreadShell`.
pub fn is_older_settled_shell(shell: &Value, before_ms: i64) -> bool {
    is_listed_lineage(shell.get("lineage").unwrap_or(&Value::Null))
        && shell.get("settledOverride").and_then(Value::as_str) == Some("settled")
        && shell
            .get("settledAt")
            .and_then(Value::as_str)
            .and_then(iso_to_ms)
            .map(|ms| ms < before_ms)
            .unwrap_or(false)
}

pub fn iso_to_ms(iso: &str) -> Option<i64> {
    use time::format_description::well_known::Iso8601;
    OffsetDateTime::parse(iso, &Iso8601::DEFAULT)
        .ok()
        .map(|dt| dt.unix_timestamp() * 1000 + (dt.millisecond() as i64))
}

// ---------------------------------------------------------------------------
// Domain event envelope
// ---------------------------------------------------------------------------

/// Build a domain event object ({id, threadId, type, occurredAt, payload, ...}).
pub fn thread_event(
    thread_id: &str,
    ty: &str,
    payload: Value,
    run_id: Option<&str>,
    node_id: Option<&str>,
    driver: Option<&str>,
    provider_instance_id: Option<&str>,
    raw_event_id: Option<&str>,
) -> Value {
    let mut ev = Map::new();
    ev.insert("id".into(), json!(new_id()));
    ev.insert("threadId".into(), json!(thread_id));
    ev.insert("type".into(), json!(ty));
    ev.insert("occurredAt".into(), json!(now_iso()));
    ev.insert("payload".into(), payload);
    if let Some(v) = run_id {
        ev.insert("runId".into(), json!(v));
    }
    if let Some(v) = node_id {
        ev.insert("nodeId".into(), json!(v));
    }
    if let Some(v) = driver {
        ev.insert("driver".into(), json!(v));
    }
    if let Some(v) = provider_instance_id {
        ev.insert("providerInstanceId".into(), json!(v));
    }
    if let Some(v) = raw_event_id {
        ev.insert("rawEventId".into(), json!(v));
    }
    Value::Object(ev)
}

// ---------------------------------------------------------------------------
// AppThread construction from thread.create
// ---------------------------------------------------------------------------

pub fn app_thread_from_create(cmd: &Value, now: &str) -> Value {
    let thread_id = cmd.get("threadId").and_then(Value::as_str).unwrap_or("");
    let lineage = cmd.get("lineage").cloned().unwrap_or_else(|| {
        json!({
            "parentThreadId": null,
            "relationshipToParent": null,
            "rootThreadId": thread_id,
        })
    });
    json!({
        "createdBy": cmd.get("createdBy").cloned().unwrap_or(json!("user")),
        "creationSource": cmd.get("creationSource").cloned().unwrap_or(json!("web")),
        "id": thread_id,
        "projectId": cmd.get("projectId"),
        "title": cmd.get("title").cloned().unwrap_or(json!("New thread")),
        "providerInstanceId": cmd.get("providerInstanceId")
            .or_else(|| cmd.pointer("/modelSelection/instanceId"))
            .cloned()
            .unwrap_or(Value::Null),
        "modelSelection": cmd.get("modelSelection").cloned().unwrap_or(Value::Null),
        "runtimeMode": cmd.get("runtimeMode").cloned().unwrap_or(json!("local")),
        "interactionMode": cmd.get("interactionMode").cloned().unwrap_or(json!("default")),
        "branch": cmd.get("branch").cloned().unwrap_or(Value::Null),
        "worktreePath": cmd.get("worktreePath").cloned().unwrap_or(Value::Null),
        "activeProviderThreadId": null,
        "lineage": lineage,
        "forkedFrom": cmd.get("forkedFrom").cloned().unwrap_or(Value::Null),
        "createdAt": now,
        "updatedAt": now,
        "archivedAt": null,
        "settledOverride": null,
        "settledAt": null,
        "lastVisitedAt": null,
        "deletedAt": null,
    })
}
