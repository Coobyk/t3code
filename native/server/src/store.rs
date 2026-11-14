//! Single-threaded SQLite actor owning `statev2.sqlite` semantics.
//!
//! Writes go through `commit_events`, which appends rows to the unified
//! `orchestration_events` log (application_event_version = 2) and maintains the
//! `orchestration_v2_projection_*` tables in the same transaction.

use std::path::Path;
use std::sync::mpsc as std_mpsc;

use anyhow::Context;
use rusqlite::{params, Connection};
use serde_json::{json, Map, Value};
use tokio::sync::oneshot;

use crate::domain;

pub const WINDOW_SIZE: usize = 8;

#[derive(Debug, Clone)]
pub struct CommittedEvent {
    pub sequence: i64,
    pub aggregate_kind: String,
    pub stream_id: String,
    pub event: Value,
}

/// What `message.dispatch` produces for the provider turn runner.
#[derive(Debug, Clone)]
pub struct RunContext {
    pub thread_id: String,
    pub run_id: String,
    pub attempt_id: String,
    pub root_node_id: String,
    pub provider_thread_id: String,
    pub provider_turn_id: String,
    pub user_message_id: String,
    pub prompt: String,
    pub model_selection: Value,
    pub provider_instance_id: String,
    pub workspace_root: String,
    pub dispatch_queued: bool,
}

pub struct ApplyResult {
    pub sequence: i64,
    pub events: Vec<CommittedEvent>,
    pub run_ctx: Option<RunContext>,
}

type Reply<T> = oneshot::Sender<T>;

enum StoreCmd {
    ShellSnapshot(Reply<Value>),
    ShellSnapshotWithOptions {
        omit_idle_subagents: bool,
        omit_older_settled: bool,
        reply: Reply<Value>,
    },
    ThreadShell {
        thread_id: String,
        reply: Reply<Option<Value>>,
    },
    ProjectShell {
        project_id: String,
        reply: Reply<Option<Value>>,
    },
    ThreadProjection {
        thread_id: String,
        reply: Reply<Option<Value>>,
    },
    EventsForThread {
        thread_id: String,
        after: i64,
        reply: Reply<Vec<CommittedEvent>>,
    },
    OlderSettled {
        before: String,
        project_ids: Option<Vec<String>>,
        limit: i64,
        reply: Reply<Vec<Value>>,
    },
    ApplyCommand {
        command: Value,
        reply: Reply<Result<ApplyResult, String>>,
    },
    CommitEvents {
        thread_id: String,
        command_id: Option<String>,
        events: Vec<Value>,
        reply: Reply<Result<Vec<CommittedEvent>, String>>,
    },
    LatestSequence(Reply<i64>),
    ListProjects(Reply<Vec<Value>>),
    GetThread {
        thread_id: String,
        reply: Reply<Option<Value>>,
    },
    GetRun {
        thread_id: String,
        run_id: String,
        reply: Reply<Option<Value>>,
    },
    GetTurnItem {
        thread_id: String,
        item_id: String,
        reply: Reply<Option<Value>>,
    },
    SeedProject {
        id: String,
        title: String,
        workspace_root: String,
        default_model_selection: Option<Value>,
        reply: Reply<()>,
    },
}

#[derive(Clone)]
pub struct StoreHandle {
    tx: std_mpsc::Sender<StoreCmd>,
}

impl StoreHandle {
    pub fn spawn(db_path: &Path) -> anyhow::Result<Self> {
        let mut conn = Connection::open(db_path)
            .with_context(|| format!("open {}", db_path.display()))?;
        migrate(&mut conn)?;
        let (tx, rx) = std_mpsc::channel::<StoreCmd>();
        std::thread::Builder::new()
            .name("store".into())
            .spawn(move || actor_loop(conn, rx))
            .context("spawn store thread")?;
        Ok(Self { tx })
    }

    fn send(&self, cmd: StoreCmd) {
        let _ = self.tx.send(cmd);
    }

    pub async fn latest_sequence(&self) -> i64 {
        let (tx, rx) = oneshot::channel();
        self.send(StoreCmd::LatestSequence(tx));
        rx.await.unwrap_or(0)
    }

    pub async fn shell_snapshot(&self) -> Value {
        let (tx, rx) = oneshot::channel();
        self.send(StoreCmd::ShellSnapshot(tx));
        rx.await.unwrap_or_else(|_| json!({}))
    }

    pub async fn shell_snapshot_opts(
        &self,
        omit_idle_subagents: bool,
        omit_older_settled: bool,
    ) -> Value {
        let (tx, rx) = oneshot::channel();
        self.send(StoreCmd::ShellSnapshotWithOptions {
            omit_idle_subagents,
            omit_older_settled,
            reply: tx,
        });
        rx.await.unwrap_or_else(|_| json!({}))
    }

    pub async fn thread_shell(&self, thread_id: &str) -> Option<Value> {
        let (tx, rx) = oneshot::channel();
        self.send(StoreCmd::ThreadShell {
            thread_id: thread_id.into(),
            reply: tx,
        });
        rx.await.ok().flatten()
    }

    pub async fn project_shell(&self, project_id: &str) -> Option<Value> {
        let (tx, rx) = oneshot::channel();
        self.send(StoreCmd::ProjectShell {
            project_id: project_id.into(),
            reply: tx,
        });
        rx.await.ok().flatten()
    }

    pub async fn thread_projection(&self, thread_id: &str) -> Option<Value> {
        let (tx, rx) = oneshot::channel();
        self.send(StoreCmd::ThreadProjection {
            thread_id: thread_id.into(),
            reply: tx,
        });
        rx.await.ok().flatten()
    }

    pub async fn events_for_thread(&self, thread_id: &str, after: i64) -> Vec<CommittedEvent> {
        let (tx, rx) = oneshot::channel();
        self.send(StoreCmd::EventsForThread {
            thread_id: thread_id.into(),
            after,
            reply: tx,
        });
        rx.await.unwrap_or_default()
    }

    pub async fn older_settled(
        &self,
        before: &str,
        project_ids: Option<Vec<String>>,
        limit: i64,
    ) -> Vec<Value> {
        let (tx, rx) = oneshot::channel();
        self.send(StoreCmd::OlderSettled {
            before: before.into(),
            project_ids,
            limit,
            reply: tx,
        });
        rx.await.unwrap_or_default()
    }

    pub async fn apply_command(&self, command: Value) -> Result<ApplyResult, String> {
        let (tx, rx) = oneshot::channel();
        self.send(StoreCmd::ApplyCommand {
            command,
            reply: tx,
        });
        rx.await.unwrap_or_else(|_| Err("store actor died".into()))
    }

    pub async fn commit_events(
        &self,
        thread_id: &str,
        command_id: Option<&str>,
        events: Vec<Value>,
    ) -> Result<Vec<CommittedEvent>, String> {
        let (tx, rx) = oneshot::channel();
        self.send(StoreCmd::CommitEvents {
            thread_id: thread_id.into(),
            command_id: command_id.map(str::to_owned),
            events,
            reply: tx,
        });
        rx.await.unwrap_or_else(|_| Err("store actor died".into()))
    }

    pub async fn list_projects(&self) -> Vec<Value> {
        let (tx, rx) = oneshot::channel();
        self.send(StoreCmd::ListProjects(tx));
        rx.await.unwrap_or_default()
    }

    pub async fn get_thread(&self, thread_id: &str) -> Option<Value> {
        let (tx, rx) = oneshot::channel();
        self.send(StoreCmd::GetThread {
            thread_id: thread_id.into(),
            reply: tx,
        });
        rx.await.ok().flatten()
    }

    pub async fn get_run(&self, thread_id: &str, run_id: &str) -> Option<Value> {
        let (tx, rx) = oneshot::channel();
        self.send(StoreCmd::GetRun {
            thread_id: thread_id.into(),
            run_id: run_id.into(),
            reply: tx,
        });
        rx.await.ok().flatten()
    }

    pub async fn get_turn_item(&self, thread_id: &str, item_id: &str) -> Option<Value> {
        let (tx, rx) = oneshot::channel();
        self.send(StoreCmd::GetTurnItem {
            thread_id: thread_id.into(),
            item_id: item_id.into(),
            reply: tx,
        });
        rx.await.ok().flatten()
    }

    pub async fn seed_project(
        &self,
        id: &str,
        title: &str,
        workspace_root: &str,
        default_model_selection: Option<Value>,
    ) {
        let (tx, rx) = oneshot::channel();
        self.send(StoreCmd::SeedProject {
            id: id.into(),
            title: title.into(),
            workspace_root: workspace_root.into(),
            default_model_selection,
            reply: tx,
        });
        let _ = rx.await;
    }
}

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

fn migrate(conn: &mut Connection) -> anyhow::Result<()> {
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "foreign_keys", "ON")?;
    conn.execute_batch(SCHEMA_DDL)?;
    Ok(())
}

const SCHEMA_DDL: &str = r#"
CREATE TABLE IF NOT EXISTS projection_projects (
  project_id TEXT PRIMARY KEY,
  title TEXT NOT NULL,
  workspace_root TEXT NOT NULL,
  scripts_json TEXT NOT NULL DEFAULT '[]',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  deleted_at TEXT,
  default_model_selection_json TEXT,
  default_thread_env_mode TEXT,
  favicon_path TEXT,
  auto_pull INTEGER NOT NULL DEFAULT 0,
  project_icon_json TEXT
);

CREATE TABLE IF NOT EXISTS orchestration_events (
  sequence INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id TEXT NOT NULL UNIQUE,
  aggregate_kind TEXT NOT NULL,
  stream_id TEXT NOT NULL,
  stream_version INTEGER NOT NULL,
  event_type TEXT NOT NULL,
  occurred_at TEXT NOT NULL,
  command_id TEXT,
  causation_event_id TEXT,
  correlation_id TEXT,
  actor_kind TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  metadata_json TEXT NOT NULL,
  application_event_version INTEGER NOT NULL DEFAULT 1
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_orch_events_stream_version
  ON orchestration_events(aggregate_kind, stream_id, stream_version);
CREATE INDEX IF NOT EXISTS idx_orch_events_stream_sequence
  ON orchestration_events(aggregate_kind, stream_id, sequence);
CREATE INDEX IF NOT EXISTS idx_orchestration_events_application_sequence
  ON orchestration_events(application_event_version, sequence);
CREATE INDEX IF NOT EXISTS idx_orchestration_events_application_high_water
  ON orchestration_events(sequence)
  WHERE aggregate_kind = 'project'
     OR (application_event_version = 2 AND aggregate_kind = 'thread');
CREATE INDEX IF NOT EXISTS idx_orchestration_events_agent_stream_sequence
  ON orchestration_events(stream_id, sequence)
  WHERE application_event_version = 2 AND aggregate_kind = 'thread';

CREATE TABLE IF NOT EXISTS orchestration_v2_command_receipts (
  command_id TEXT PRIMARY KEY,
  thread_id TEXT NOT NULL,
  command_type TEXT NOT NULL,
  accepted_at TEXT NOT NULL,
  result_sequence INTEGER NOT NULL,
  status TEXT NOT NULL,
  error TEXT
);

CREATE TABLE IF NOT EXISTS orchestration_v2_projection_metadata (
  projection_name TEXT PRIMARY KEY,
  schema_version INTEGER NOT NULL,
  last_sequence INTEGER NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS orchestration_v2_projection_threads (
  thread_id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL,
  title TEXT NOT NULL,
  default_provider TEXT,
  runtime_mode TEXT,
  interaction_mode TEXT,
  active_provider_thread_id TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  archived_at TEXT,
  deleted_at TEXT,
  payload_json TEXT NOT NULL,
  provider_instance_id TEXT
);
CREATE INDEX IF NOT EXISTS orchestration_v2_projection_threads_project_updated_idx
  ON orchestration_v2_projection_threads(project_id, updated_at);

CREATE TABLE IF NOT EXISTS orchestration_v2_projection_runs (
  run_id TEXT PRIMARY KEY,
  thread_id TEXT NOT NULL,
  ordinal INTEGER NOT NULL,
  provider TEXT,
  provider_thread_id TEXT,
  status TEXT NOT NULL,
  requested_at TEXT NOT NULL,
  completed_at TEXT,
  payload_json TEXT NOT NULL,
  provider_instance_id TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS orchestration_v2_projection_runs_thread_ordinal_idx
  ON orchestration_v2_projection_runs(thread_id, ordinal);

CREATE TABLE IF NOT EXISTS orchestration_v2_projection_run_attempts (
  attempt_id TEXT PRIMARY KEY,
  thread_id TEXT NOT NULL,
  run_id TEXT NOT NULL,
  attempt_ordinal INTEGER NOT NULL,
  root_node_id TEXT NOT NULL,
  provider TEXT,
  provider_thread_id TEXT,
  provider_turn_id TEXT,
  status TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  provider_instance_id TEXT
);

CREATE TABLE IF NOT EXISTS orchestration_v2_projection_nodes (
  node_id TEXT PRIMARY KEY,
  thread_id TEXT NOT NULL,
  run_id TEXT,
  parent_node_id TEXT,
  root_node_id TEXT NOT NULL,
  kind TEXT NOT NULL,
  status TEXT NOT NULL,
  provider_thread_id TEXT,
  provider_turn_id TEXT,
  runtime_request_id TEXT,
  checkpoint_scope_id TEXT,
  started_at TEXT,
  completed_at TEXT,
  payload_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS orchestration_v2_projection_nodes_thread_run_idx
  ON orchestration_v2_projection_nodes(thread_id, run_id);

CREATE TABLE IF NOT EXISTS orchestration_v2_projection_provider_sessions (
  provider_session_id TEXT PRIMARY KEY,
  thread_id TEXT,
  provider TEXT,
  status TEXT,
  model TEXT,
  updated_at TEXT,
  payload_json TEXT NOT NULL,
  driver TEXT,
  provider_instance_id TEXT
);

CREATE TABLE IF NOT EXISTS orchestration_v2_projection_provider_session_bindings (
  provider_session_id TEXT NOT NULL,
  thread_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS orchestration_v2_projection_provider_threads (
  provider_thread_id TEXT PRIMARY KEY,
  thread_id TEXT,
  owner_node_id TEXT,
  provider TEXT,
  provider_session_id TEXT,
  status TEXT,
  first_run_ordinal INTEGER,
  last_run_ordinal INTEGER,
  updated_at TEXT,
  payload_json TEXT NOT NULL,
  driver TEXT,
  provider_instance_id TEXT
);
CREATE INDEX IF NOT EXISTS orchestration_v2_projection_provider_threads_thread_idx
  ON orchestration_v2_projection_provider_threads(thread_id);

CREATE TABLE IF NOT EXISTS orchestration_v2_projection_provider_turns (
  provider_turn_id TEXT PRIMARY KEY,
  thread_id TEXT,
  provider_thread_id TEXT,
  node_id TEXT,
  run_attempt_id TEXT,
  ordinal INTEGER,
  status TEXT,
  started_at TEXT,
  completed_at TEXT,
  payload_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS orchestration_v2_projection_provider_turns_thread_idx
  ON orchestration_v2_projection_provider_turns(thread_id);

CREATE TABLE IF NOT EXISTS orchestration_v2_projection_runtime_requests (
  runtime_request_id TEXT PRIMARY KEY,
  thread_id TEXT NOT NULL,
  node_id TEXT,
  provider_turn_id TEXT,
  kind TEXT,
  status TEXT,
  created_at TEXT,
  resolved_at TEXT,
  payload_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS orchestration_v2_projection_runtime_requests_thread_status_idx
  ON orchestration_v2_projection_runtime_requests(thread_id, status);

CREATE TABLE IF NOT EXISTS orchestration_v2_projection_messages (
  message_id TEXT PRIMARY KEY,
  thread_id TEXT NOT NULL,
  run_id TEXT,
  node_id TEXT,
  role TEXT NOT NULL,
  streaming INTEGER NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  payload_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS orchestration_v2_projection_messages_thread_created_idx
  ON orchestration_v2_projection_messages(thread_id, created_at, message_id);

CREATE TABLE IF NOT EXISTS orchestration_v2_projection_plans (
  plan_id TEXT PRIMARY KEY,
  thread_id TEXT NOT NULL,
  run_id TEXT,
  node_id TEXT,
  kind TEXT NOT NULL,
  status TEXT NOT NULL,
  payload_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS orchestration_v2_projection_turn_items (
  turn_item_id TEXT PRIMARY KEY,
  thread_id TEXT NOT NULL,
  run_id TEXT,
  node_id TEXT,
  provider_thread_id TEXT,
  provider_turn_id TEXT,
  parent_item_id TEXT,
  ordinal INTEGER NOT NULL,
  type TEXT NOT NULL,
  status TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  payload_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS orchestration_v2_projection_turn_items_thread_ordinal_idx
  ON orchestration_v2_projection_turn_items(thread_id, ordinal, turn_item_id);

CREATE TABLE IF NOT EXISTS orchestration_v2_turn_item_positions (
  thread_id TEXT NOT NULL,
  turn_item_id TEXT NOT NULL,
  ordinal INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS orchestration_v2_projection_subagents (
  subagent_id TEXT PRIMARY KEY,
  thread_id TEXT NOT NULL,
  run_id TEXT,
  parent_node_id TEXT,
  provider TEXT,
  provider_thread_id TEXT,
  child_thread_id TEXT,
  origin TEXT,
  status TEXT,
  started_at TEXT,
  completed_at TEXT,
  updated_at TEXT,
  payload_json TEXT NOT NULL,
  driver TEXT,
  provider_instance_id TEXT
);

CREATE TABLE IF NOT EXISTS orchestration_v2_projection_checkpoint_scopes (
  scope_id TEXT PRIMARY KEY,
  thread_id TEXT NOT NULL,
  run_id TEXT,
  node_id TEXT NOT NULL,
  parent_scope_id TEXT,
  provider_thread_id TEXT,
  kind TEXT NOT NULL,
  ordinal_within_parent INTEGER NOT NULL,
  advances_app_run_count INTEGER NOT NULL,
  created_at TEXT NOT NULL,
  payload_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS orchestration_v2_projection_checkpoints (
  checkpoint_id TEXT PRIMARY KEY,
  thread_id TEXT NOT NULL,
  scope_id TEXT NOT NULL,
  run_id TEXT,
  node_id TEXT NOT NULL,
  parent_checkpoint_id TEXT,
  ordinal_within_scope INTEGER NOT NULL,
  app_run_ordinal INTEGER,
  status TEXT NOT NULL,
  captured_at TEXT NOT NULL,
  payload_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS orchestration_v2_projection_context_handoffs (
  context_handoff_id TEXT PRIMARY KEY,
  thread_id TEXT NOT NULL,
  target_run_id TEXT NOT NULL,
  to_provider_thread_id TEXT NOT NULL,
  strategy TEXT NOT NULL,
  status TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  payload_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS orchestration_v2_projection_context_transfers (
  context_transfer_id TEXT PRIMARY KEY,
  source_thread_id TEXT NOT NULL,
  target_thread_id TEXT NOT NULL,
  target_run_id TEXT,
  type TEXT NOT NULL,
  status TEXT NOT NULL,
  source_provider TEXT,
  target_provider TEXT,
  updated_at TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  source_provider_instance_id TEXT,
  target_provider_instance_id TEXT
);
"#;

// ---------------------------------------------------------------------------
// Actor
// ---------------------------------------------------------------------------

fn actor_loop(conn: Connection, rx: std_mpsc::Receiver<StoreCmd>) {
    for cmd in rx {
        match cmd {
            StoreCmd::ShellSnapshot(reply) => {
                let _ = reply.send(shell_snapshot(&conn, false, false));
            }
            StoreCmd::ShellSnapshotWithOptions {
                omit_idle_subagents,
                omit_older_settled,
                reply,
            } => {
                let _ = reply.send(shell_snapshot(&conn, omit_idle_subagents, omit_older_settled));
            }
            StoreCmd::ThreadShell { thread_id, reply } => {
                let _ = reply.send(thread_shell_for(&conn, &thread_id));
            }
            StoreCmd::ProjectShell { project_id, reply } => {
                let _ = reply.send(project_shell_for(&conn, &project_id));
            }
            StoreCmd::ThreadProjection { thread_id, reply } => {
                let _ = reply.send(thread_projection(&conn, &thread_id));
            }
            StoreCmd::EventsForThread {
                thread_id,
                after,
                reply,
            } => {
                let _ = reply.send(events_for_thread(&conn, &thread_id, after));
            }
            StoreCmd::OlderSettled {
                before,
                project_ids,
                limit,
                reply,
            } => {
                let _ = reply.send(older_settled(&conn, &before, project_ids.as_deref(), limit));
            }
            StoreCmd::ApplyCommand { command, reply } => {
                let _ = reply.send(apply_command(&conn, &command));
            }
            StoreCmd::CommitEvents {
                thread_id,
                command_id,
                events,
                reply,
            } => {
                let _ = reply.send(commit_events(
                    &conn,
                    &thread_id,
                    command_id.as_deref(),
                    events,
                ));
            }
            StoreCmd::LatestSequence(reply) => {
                let _ = reply.send(latest_sequence(&conn));
            }
            StoreCmd::ListProjects(reply) => {
                let _ = reply.send(list_projects(&conn));
            }
            StoreCmd::GetThread { thread_id, reply } => {
                let _ = reply.send(get_thread_row(&conn, &thread_id));
            }
            StoreCmd::GetRun {
                thread_id: _,
                run_id,
                reply,
            } => {
                let v = conn
                    .query_row(
                        "SELECT payload_json FROM orchestration_v2_projection_runs WHERE run_id = ?1",
                        params![run_id],
                        |r| r.get::<_, String>(0),
                    )
                    .ok()
                    .and_then(|s| serde_json::from_str(&s).ok());
                let _ = reply.send(v);
            }
            StoreCmd::GetTurnItem {
                thread_id: _,
                item_id,
                reply,
            } => {
                let v = conn
                    .query_row(
                        "SELECT payload_json FROM orchestration_v2_projection_turn_items WHERE turn_item_id = ?1",
                        params![item_id],
                        |r| r.get::<_, String>(0),
                    )
                    .ok()
                    .and_then(|s| serde_json::from_str(&s).ok());
                let _ = reply.send(v);
            }
            StoreCmd::SeedProject {
                id,
                title,
                workspace_root,
                default_model_selection,
                reply,
            } => {
                seed_project(&conn, &id, &title, &workspace_root, default_model_selection);
                let _ = reply.send(());
            }
        }
    }
}

fn latest_sequence(conn: &Connection) -> i64 {
    conn.query_row(
        "SELECT COALESCE(MAX(sequence), 0) FROM orchestration_events \
         WHERE aggregate_kind = 'project' \
            OR (application_event_version = 2 AND aggregate_kind = 'thread')",
        [],
        |r| r.get(0),
    )
    .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Reads
// ---------------------------------------------------------------------------

fn project_shell_for(conn: &Connection, project_id: &str) -> Option<Value> {
    conn.query_row(
        "SELECT project_id, title, workspace_root, scripts_json, created_at, updated_at, \
                default_model_selection_json, default_thread_env_mode, project_icon_json \
         FROM projection_projects WHERE project_id = ?1 AND deleted_at IS NULL",
        params![project_id],
        |r| {
            let s = |i| r.get::<_, Option<String>>(i).unwrap_or_default().unwrap_or_default();
            Ok(domain::project_shell(
                &s(0),
                &s(1),
                &s(2),
                &s(3),
                &s(4),
                &s(5),
                r.get::<_, Option<String>>(6)?.as_deref(),
                r.get::<_, Option<String>>(7)?.as_deref(),
                r.get::<_, Option<String>>(8)?.as_deref(),
                None,
            ))
        },
    )
    .ok()
}

fn list_projects(conn: &Connection) -> Vec<Value> {
    let mut stmt = match conn.prepare(
        "SELECT project_id, title, workspace_root, scripts_json, created_at, updated_at, \
                default_model_selection_json, default_thread_env_mode, project_icon_json \
         FROM projection_projects WHERE deleted_at IS NULL ORDER BY created_at ASC, project_id ASC",
    ) {
        Ok(s) => s,
        Err(_) => return vec![],
    };
    let rows = stmt
        .query_map([], |r| {
            let s = |i| r.get::<_, Option<String>>(i).unwrap_or_default().unwrap_or_default();
            Ok(domain::project_shell(
                &s(0),
                &s(1),
                &s(2),
                &s(3),
                &s(4),
                &s(5),
                r.get::<_, Option<String>>(6)?.as_deref(),
                r.get::<_, Option<String>>(7)?.as_deref(),
                r.get::<_, Option<String>>(8)?.as_deref(),
                None,
            ))
        })
        .map(|m| m.filter_map(Result::ok).collect())
        .unwrap_or_default();
    rows
}

fn get_thread_row(conn: &Connection, thread_id: &str) -> Option<Value> {
    conn.query_row(
        "SELECT payload_json FROM orchestration_v2_projection_threads \
         WHERE thread_id = ?1 AND deleted_at IS NULL",
        params![thread_id],
        |r| r.get::<_, String>(0),
    )
    .ok()
    .and_then(|s| serde_json::from_str(&s).ok())
}

fn column_json(conn: &Connection, sql: &str, params: &[&dyn rusqlite::ToSql]) -> Vec<Value> {
    let mut stmt = match conn.prepare(sql) {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(%e, %sql, "column_json prepare failed");
            return vec![];
        }
    };
    stmt.query_map(params, |r| r.get::<_, String>(0))
        .map(|rows| {
            rows.filter_map(|r| r.ok())
                .filter_map(|s| serde_json::from_str(&s).ok())
                .collect()
        })
        .unwrap_or_default()
}

/// Derive the ThreadShell for one stored AppThread.
fn thread_shell_for(conn: &Connection, thread_id: &str) -> Option<Value> {
    let app = get_thread_row(conn, thread_id)?;
    Some(thread_shell_from_app(conn, &app))
}

const ACTIVE_RUN_STATUSES: &str = "'preparing','starting','running'";
const ACTIVITY_RUN_STATUSES: &str = "'preparing','starting','running','waiting'";

fn thread_shell_from_app(conn: &Connection, app: &Value) -> Value {
    let thread_id = app.get("id").and_then(Value::as_str).unwrap_or_default();

    // Presented run: newest non-queueHeld run (ignoring silent compaction runs).
    let presented = conn
        .query_row(
            "SELECT payload_json FROM orchestration_v2_projection_runs \
             WHERE thread_id = ?1 \
               AND NOT (status = 'queued' AND json_extract(payload_json, '$.queueHeld') IS 1) \
             ORDER BY ordinal DESC, run_id DESC LIMIT 1",
            params![thread_id],
            |r| r.get::<_, String>(0),
        )
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok());

    let active = conn
        .query_row(
            &format!(
                "SELECT payload_json FROM orchestration_v2_projection_runs \
                 WHERE thread_id = ?1 AND status IN ({ACTIVE_RUN_STATUSES}) \
                 ORDER BY ordinal DESC, run_id DESC LIMIT 1"
            ),
            params![thread_id],
            |r| r.get::<_, String>(0),
        )
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok());

    let activity = conn
        .query_row(
            &format!(
                "SELECT payload_json FROM orchestration_v2_projection_runs \
                 WHERE thread_id = ?1 AND status IN ({ACTIVITY_RUN_STATUSES}) \
                 ORDER BY ordinal DESC, run_id DESC LIMIT 1"
            ),
            params![thread_id],
            |r| r.get::<_, String>(0),
        )
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok());

    let pending_request: Option<Value> = conn
        .query_row(
            "SELECT payload_json FROM orchestration_v2_projection_runtime_requests \
             WHERE thread_id = ?1 AND status = 'pending' \
             ORDER BY (kind = 'user_input') ASC, created_at DESC, runtime_request_id DESC LIMIT 1",
            params![thread_id],
            |r| r.get::<_, String>(0),
        )
        .ok()
        .and_then(|s| serde_json::from_str::<Value>(&s).ok())
        .map(|req| {
            json!({
                "id": req.get("id"),
                "kind": req.get("kind"),
                "createdAt": req.get("createdAt"),
            })
        });

    let latest_user_message_at: Option<String> = conn
        .query_row(
            "SELECT updated_at FROM orchestration_v2_projection_messages \
             WHERE thread_id = ?1 AND role = 'user' \
             ORDER BY updated_at DESC, message_id DESC LIMIT 1",
            params![thread_id],
            |r| r.get(0),
        )
        .ok();

    let has_actionable_proposed_plan = conn
        .query_row(
            "SELECT 1 FROM orchestration_v2_projection_plans \
             WHERE thread_id = ?1 AND kind = 'proposed_plan' AND status = 'active' LIMIT 1",
            params![thread_id],
            |_| Ok(()),
        )
        .is_ok();

    // Visibility: hide items of rolled_back runs, queued user messages of
    // cancelled runs, and superseded interrupt results.
    let runs: Vec<Value> = column_json(
        conn,
        "SELECT payload_json FROM orchestration_v2_projection_runs WHERE thread_id = ?1",
        &[&thread_id],
    );
    let attempts: Vec<Value> = column_json(
        conn,
        "SELECT payload_json FROM orchestration_v2_projection_run_attempts WHERE thread_id = ?1",
        &[&thread_id],
    );
    let items: Vec<Value> = column_json(
        conn,
        "SELECT payload_json FROM orchestration_v2_projection_turn_items \
         WHERE thread_id = ?1 ORDER BY ordinal ASC, turn_item_id ASC",
        &[&thread_id],
    );
    let visible = visible_turn_items(&runs, &attempts, &items);

    let stop_requested = presented.as_ref().is_some_and(|run: &Value| {
        let rid = run.get("id").and_then(Value::as_str).unwrap_or("");
        items.iter().any(|i| {
            i.get("runId").and_then(Value::as_str) == Some(rid)
                && i.get("type").and_then(Value::as_str) == Some("run_interrupt_request")
        })
    });

    let provider_instance_history: Vec<Value> = {
        let pts = column_json(
            conn,
            "SELECT payload_json FROM orchestration_v2_projection_provider_threads \
             WHERE thread_id = ?1 ORDER BY json_extract(payload_json, '$.createdAt') ASC, provider_thread_id ASC",
            &[&thread_id],
        );
        let mut history: Vec<Value> = vec![];
        for pt in pts {
            let matches_thread = pt.get("appThreadId").and_then(Value::as_str) == Some(thread_id)
                && pt.get("ownerNodeId").is_none_or(Value::is_null);
            if matches_thread {
                if let Some(inst) = pt.get("providerInstanceId") {
                    if !inst.is_null() && !history.contains(inst) {
                        history.push(inst.clone());
                    }
                }
            }
        }
        history
    };

    let mut d = domain::ThreadDerived {
        latest_run: presented.clone(),
        active_run: activity.clone().or(active),
        pending_request,
        latest_visible_message: None,
        latest_user_message_at,
        item_count: visible.len() as i64,
        visible_item_count: visible.len() as i64,
        has_actionable_proposed_plan,
        pending_background_tasks: vec![],
        provider_instance_history,
    };
    let mut shell = domain::thread_shell(app, &d);
    if stop_requested {
        shell["latestRunStopRequested"] = json!(true);
    } else {
        shell["latestRunStopRequested"] = json!(false);
    }
    d.latest_run = None;
    shell
}

/// `createOrchestrationV2TurnItemVisibility` port: returns visible items as
/// `{position, visibility:"local", sourceThreadId, sourceItemId, item}` rows.
fn visible_turn_items(runs: &[Value], attempts: &[Value], items: &[Value]) -> Vec<Value> {
    use std::collections::{HashMap, HashSet};
    let statuses: HashMap<&str, &str> = runs
        .iter()
        .filter_map(|r| {
            let id = r.get("id")?.as_str()?;
            let s = r.get("status")?.as_str()?;
            (s == "rolled_back" || s == "cancelled").then_some((id, s))
        })
        .collect();
    let mut superseded_roots: HashMap<&str, HashSet<&str>> = HashMap::new();
    for a in attempts {
        if a.get("status").and_then(Value::as_str) != Some("superseded") {
            continue;
        }
        let run_id = a.get("runId").and_then(Value::as_str).unwrap_or("");
        let root = a.get("rootNodeId").and_then(Value::as_str).unwrap_or("");
        superseded_roots.entry(run_id).or_default().insert(root);
    }
    let interrupt_runs: HashSet<&str> = if superseded_roots.is_empty() {
        HashSet::new()
    } else {
        items
            .iter()
            .filter(|i| i.get("type").and_then(Value::as_str) == Some("run_interrupt_request"))
            .filter_map(|i| i.get("runId").and_then(Value::as_str))
            .collect()
    };
    let visible: Vec<Value> = items
        .iter()
        .filter(|item| {
            let run_id = item.get("runId").and_then(Value::as_str);
            let ty = item.get("type").and_then(Value::as_str).unwrap_or("");
            let status = run_id.and_then(|id| statuses.get(id).copied());
            if status == Some("rolled_back") {
                return false;
            }
            if status == Some("cancelled")
                && ty == "user_message"
                && item.get("inputIntent").and_then(Value::as_str) == Some("queued_turn")
            {
                return false;
            }
            if ty == "run_interrupt_result" {
                let node_id = item.get("nodeId").and_then(Value::as_str);
                if let (Some(rid), Some(nid)) = (run_id, node_id) {
                    if superseded_roots
                        .get(rid)
                        .is_some_and(|roots| roots.contains(nid))
                        && !interrupt_runs.contains(rid)
                    {
                        return false;
                    }
                }
            }
            true
        })
        .enumerate()
        .map(|(position, item)| {
            json!({
                "position": position,
                "visibility": "local",
                "sourceThreadId": item.get("threadId"),
                "sourceItemId": item.get("id"),
                "item": item,
            })
        })
        .collect();
    visible
}

fn shell_snapshot(conn: &Connection, omit_idle_subagents: bool, omit_older_settled: bool) -> Value {
    let snapshot_sequence = latest_sequence(conn);
    let projects = list_projects(conn);

    let app_rows: Vec<Value> = column_json(
        conn,
        "SELECT payload_json FROM orchestration_v2_projection_threads \
         WHERE deleted_at IS NULL ORDER BY updated_at ASC, thread_id ASC",
        &[],
    );

    let mut threads = Vec::new();
    let mut archived = Vec::new();
    for app in &app_rows {
        let shell = thread_shell_from_app(conn, app);
        let listed = domain::is_listed_lineage(app.get("lineage").unwrap_or(&Value::Null));
        let is_archived = !app.get("archivedAt").unwrap_or(&Value::Null).is_null();
        if is_archived {
            archived.push(shell);
        } else if listed || (omit_idle_subagents && is_live_subagent_shell(&shell)) || !listed && !omit_idle_subagents {
            threads.push(shell);
        }
    }

    // Per-project counts of older settled threads when omitting them.
    let mut older_settled = Vec::new();
    if omit_older_settled {
        let cutoff_ms = chrono_now_ms();
        for shell in &threads {
            if domain::is_older_settled_shell(shell, cutoff_ms) {
                // still emitted below; nothing extra needed
            }
        }
        // Count all older-settled threads per project (sent or not).
        let mut counts: Map<String, Value> = Map::new();
        for shell in threads.iter().chain(archived.iter()) {
            if domain::is_older_settled_shell(shell, cutoff_ms) {
                if let Some(pid) = shell.get("projectId").and_then(Value::as_str) {
                    let e = counts.entry(pid.to_string()).or_insert(json!(0));
                    *e = json!(e.as_i64().unwrap_or(0) + 1);
                }
            }
        }
        for (pid, count) in counts {
            older_settled.push(json!({ "projectId": pid, "count": count }));
        }
    }

    let mut snap = Map::new();
    snap.insert("schemaVersion".into(), json!(domain::PROJECTION_SCHEMA_VERSION));
    snap.insert("snapshotSequence".into(), json!(snapshot_sequence));
    snap.insert("projects".into(), Value::Array(projects));
    if omit_older_settled {
        // Exclude older settled threads from the active list.
        let cutoff_ms = chrono_now_ms();
        threads.retain(|s| !domain::is_older_settled_shell(s, cutoff_ms));
        snap.insert("olderSettled".into(), Value::Array(older_settled));
    }
    snap.insert("threads".into(), Value::Array(threads));
    snap.insert("archivedThreads".into(), Value::Array(archived));
    Value::Object(snap)
}

fn is_live_subagent_shell(shell: &Value) -> bool {
    let live = |s: Option<&str>| {
        matches!(s, Some("queued" | "preparing" | "starting" | "running" | "waiting"))
    };
    !shell.get("activityRunStatus").is_none_or(Value::is_null)
        || live(shell.get("status").and_then(Value::as_str))
        || !shell.get("pendingRuntimeRequest").is_none_or(Value::is_null)
        || shell
            .get("pendingBackgroundTasks")
            .and_then(Value::as_array)
            .is_some_and(|a| !a.is_empty())
}

fn chrono_now_ms() -> i64 {
    time::OffsetDateTime::now_utc().unix_timestamp() * 1000
}

fn older_settled(
    conn: &Connection,
    before: &str,
    project_ids: Option<&[String]>,
    limit: i64,
) -> Vec<Value> {
    let before_ms = domain::iso_to_ms(before).unwrap_or(i64::MAX);
    let mut sql = String::from(
        "SELECT payload_json FROM orchestration_v2_projection_threads \
         WHERE deleted_at IS NULL \
           AND json_extract(payload_json, '$.settledAt') < ?1 \
           AND json_extract(payload_json, '$.settledOverride') = 'settled' \
           AND json_extract(payload_json, '$.lineage.relationshipToParent') IS NOT 'subagent' \
           AND json_extract(payload_json, '$.lineage.sideChat') IS NOT 1",
    );
    let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = vec![Box::new(before.to_string())];
    if let Some(ids) = project_ids {
        if !ids.is_empty() {
            let list = ids
                .iter()
                .map(|id| format!("'{}'", id.replace('\'', "''")))
                .collect::<Vec<_>>()
                .join(",");
            sql.push_str(&format!(" AND project_id IN ({list})"));
        }
    }
    sql.push_str(" ORDER BY json_extract(payload_json, '$.settledAt') DESC, thread_id DESC LIMIT ?2");
    params_vec.push(Box::new(limit));

    let refs: Vec<&dyn rusqlite::ToSql> = params_vec.iter().map(|b| b.as_ref()).collect();
    column_json(conn, &sql, &refs)
        .iter()
        .filter(|app| {
            app.get("settledAt")
                .and_then(Value::as_str)
                .and_then(domain::iso_to_ms)
                .is_some_and(|ms| ms < before_ms)
        })
        .map(|app| thread_shell_from_app(conn, app))
        .collect()
}

fn events_for_thread(conn: &Connection, thread_id: &str, after: i64) -> Vec<CommittedEvent> {
    let mut stmt = match conn.prepare(
        "SELECT sequence, event_id, stream_id, event_type, occurred_at, payload_json, metadata_json \
         FROM orchestration_events \
         WHERE sequence > ?1 AND aggregate_kind = 'thread' AND stream_id = ?2 \
           AND application_event_version = 2 \
         ORDER BY sequence ASC",
    ) {
        Ok(s) => s,
        Err(_) => return vec![],
    };
    stmt.query_map(params![after, thread_id], |r| {
        Ok((
            r.get::<_, i64>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, String>(2)?,
            r.get::<_, String>(3)?,
            r.get::<_, String>(4)?,
            r.get::<_, String>(5)?,
            r.get::<_, String>(6)?,
        ))
    })
    .map(|rows| {
        rows.filter_map(|r| r.ok())
            .map(|(sequence, event_id, stream_id, ty, occurred_at, payload, metadata)| {
                CommittedEvent {
                    sequence,
                    aggregate_kind: "thread".into(),
                    stream_id,
                    event: rebuild_event(&event_id, thread_id, &ty, &occurred_at, &payload, &metadata),
                }
            })
            .collect()
    })
    .unwrap_or_default()
}

/// Rebuild the wire domain event from a stored row (rowToV2StoredEvent).
fn rebuild_event(
    event_id: &str,
    thread_id: &str,
    ty: &str,
    occurred_at: &str,
    payload_json: &str,
    metadata_json: &str,
) -> Value {
    let payload: Value = serde_json::from_str(payload_json).unwrap_or(Value::Null);
    let metadata: Value = serde_json::from_str(metadata_json).unwrap_or(json!({}));
    let mut ev = Map::new();
    ev.insert("id".into(), json!(event_id));
    ev.insert("threadId".into(), json!(thread_id));
    ev.insert("type".into(), json!(ty));
    ev.insert("occurredAt".into(), json!(occurred_at));
    ev.insert("payload".into(), payload);
    for k in ["runId", "nodeId", "driver", "providerInstanceId", "rawEventId"] {
        if let Some(v) = metadata.get(k) {
            ev.insert(k.into(), v.clone());
        }
    }
    Value::Object(ev)
}

fn thread_projection(conn: &Connection, thread_id: &str) -> Option<Value> {
    let thread = get_thread_row(conn, thread_id)?;
    let col = |sql: &str| column_json(conn, sql, &[&thread_id]);
    let runs = col(
        "SELECT payload_json FROM orchestration_v2_projection_runs \
         WHERE thread_id = ?1 ORDER BY ordinal ASC, run_id ASC",
    );
    let attempts = col(
        "SELECT payload_json FROM orchestration_v2_projection_run_attempts \
         WHERE thread_id = ?1 ORDER BY attempt_ordinal ASC, attempt_id ASC",
    );
    let nodes = col(
        "SELECT payload_json FROM orchestration_v2_projection_nodes WHERE thread_id = ?1",
    );
    let subagents = col(
        "SELECT payload_json FROM orchestration_v2_projection_subagents WHERE thread_id = ?1",
    );
    let provider_sessions = col(
        "SELECT payload_json FROM orchestration_v2_projection_provider_sessions WHERE thread_id = ?1",
    );
    let provider_threads = col(
        "SELECT payload_json FROM orchestration_v2_projection_provider_threads WHERE thread_id = ?1",
    );
    let provider_turns = col(
        "SELECT payload_json FROM orchestration_v2_projection_provider_turns WHERE thread_id = ?1",
    );
    let runtime_requests = col(
        "SELECT payload_json FROM orchestration_v2_projection_runtime_requests WHERE thread_id = ?1",
    );
    let messages = col(
        "SELECT payload_json FROM orchestration_v2_projection_messages \
         WHERE thread_id = ?1 ORDER BY created_at ASC, message_id ASC",
    );
    let plans = col(
        "SELECT payload_json FROM orchestration_v2_projection_plans WHERE thread_id = ?1",
    );
    let turn_items = col(
        "SELECT payload_json FROM orchestration_v2_projection_turn_items \
         WHERE thread_id = ?1 ORDER BY ordinal ASC, turn_item_id ASC",
    );
    let checkpoint_scopes = col(
        "SELECT payload_json FROM orchestration_v2_projection_checkpoint_scopes WHERE thread_id = ?1",
    );
    let checkpoints = col(
        "SELECT payload_json FROM orchestration_v2_projection_checkpoints WHERE thread_id = ?1",
    );
    let handoffs = col(
        "SELECT payload_json FROM orchestration_v2_projection_context_handoffs WHERE thread_id = ?1",
    );
    let transfers = col(
        "SELECT payload_json FROM orchestration_v2_projection_context_transfers \
         WHERE source_thread_id = ?1 OR target_thread_id = ?1",
    );
    let visible = visible_turn_items(&runs, &attempts, &turn_items);
    let updated_at = thread
        .get("updatedAt")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    Some(json!({
        "thread": thread,
        "runs": runs,
        "attempts": attempts,
        "nodes": nodes,
        "subagents": subagents,
        "providerSessions": provider_sessions,
        "providerThreads": provider_threads,
        "providerTurns": provider_turns,
        "runtimeRequests": runtime_requests,
        "messages": messages,
        "plans": plans,
        "turnItems": turn_items,
        "checkpointScopes": checkpoint_scopes,
        "checkpoints": checkpoints,
        "contextHandoffs": handoffs,
        "contextTransfers": transfers,
        "visibleTurnItems": visible,
        "updatedAt": updated_at,
    }))
}

// ---------------------------------------------------------------------------
// Event commits
// ---------------------------------------------------------------------------

fn metadata_for_event(event: &Value) -> Value {
    let mut m = Map::new();
    for k in ["runId", "nodeId", "driver", "providerInstanceId", "rawEventId"] {
        if let Some(v) = event.get(k) {
            if !v.is_null() {
                m.insert(k.into(), v.clone());
            }
        }
    }
    Value::Object(m)
}

/// Insert one row into `orchestration_events`, return the committed view.
fn append_event(
    conn: &Connection,
    aggregate_kind: &str,
    stream_id: &str,
    event: &Value,
    command_id: Option<&str>,
) -> Result<CommittedEvent, String> {
    let ty = event.get("type").and_then(Value::as_str).unwrap_or("unknown");
    let occurred_at = event
        .get("occurredAt")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .unwrap_or_else(domain::now_iso);
    let payload_json = serde_json::to_string(event.get("payload").unwrap_or(&Value::Null))
        .map_err(|e| e.to_string())?;
    let metadata_json = serde_json::to_string(&metadata_for_event(event)).map_err(|e| e.to_string())?;
    let actor = if event.get("rawEventId").is_some() {
        "provider"
    } else {
        "server"
    };
    let event_id = event
        .get("id")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .unwrap_or_else(domain::new_id);

    let sequence: i64 = conn
        .query_row(
            "INSERT INTO orchestration_events (
               event_id, aggregate_kind, stream_id, stream_version, event_type,
               occurred_at, command_id, causation_event_id, correlation_id,
               actor_kind, payload_json, metadata_json, application_event_version
             ) VALUES (
               ?1, ?2, ?3,
               COALESCE((SELECT MAX(stream_version) + 1 FROM orchestration_events
                         WHERE aggregate_kind = ?2 AND stream_id = ?3), 0),
               ?4, ?5, ?6, NULL, ?6, ?7, ?8, ?9, 2
             ) RETURNING sequence",
            params![
                event_id,
                aggregate_kind,
                stream_id,
                ty,
                occurred_at,
                command_id,
                actor,
                payload_json,
                metadata_json
            ],
            |r| r.get(0),
        )
        .map_err(|e| format!("append event {ty}: {e}"))?;

    Ok(CommittedEvent {
        sequence,
        aggregate_kind: aggregate_kind.into(),
        stream_id: stream_id.into(),
        event: rebuild_event(&event_id, stream_id, ty, &occurred_at, &payload_json, &metadata_json),
    })
}

fn commit_events(
    conn: &Connection,
    thread_id: &str,
    command_id: Option<&str>,
    events: Vec<Value>,
) -> Result<Vec<CommittedEvent>, String> {
    let tx = conn.unchecked_transaction().map_err(|e| e.to_string())?;
    let mut committed = Vec::with_capacity(events.len());
    for event in &events {
        let stored = append_event(&tx, "thread", thread_id, event, command_id)?;
        apply_projection(&tx, thread_id, event)?;
        committed.push(stored);
    }
    tx.commit().map_err(|e| e.to_string())?;
    Ok(committed)
}

/// Mirror the projection side effects of each domain event type.
fn apply_projection(conn: &Connection, thread_id: &str, event: &Value) -> Result<(), String> {
    let ty = event.get("type").and_then(Value::as_str).unwrap_or("");
    let p = event.get("payload").cloned().unwrap_or(Value::Null);
    let pj = serde_json::to_string(&p).map_err(|e| e.to_string())?;
    let s = |k: &str| -> Option<String> {
        p.get(k).and_then(Value::as_str).map(str::to_owned)
    };
    let run = |sql: &str, params: &[&dyn rusqlite::ToSql]| -> Result<(), String> {
        conn.execute(sql, params).map(|_| ()).map_err(|e| e.to_string())
    };

    match ty {
        "thread.created"
        | "thread.archived"
        | "thread.unarchived"
        | "thread.deleted"
        | "thread.settled"
        | "thread.unsettled"
        | "thread.snoozed"
        | "thread.unsnoozed"
        | "thread.pinned"
        | "thread.auto-settle-set"
        | "thread.unpinned"
        | "thread.pin-reordered"
        | "thread.active-reordered"
        | "thread.tags-updated"
        | "thread.visited"
        | "thread.marked-unread"
        | "thread.metadata-updated"
        | "thread.pull-request-synced"
        | "thread.runtime-mode-updated"
        | "thread.interaction-mode-updated"
        | "thread.model-selection-updated"
        | "thread.provider-switched"
        | "thread.guest-share-updated" => {
            run(
                "INSERT INTO orchestration_v2_projection_threads (
                   thread_id, project_id, title, default_provider, runtime_mode,
                   interaction_mode, active_provider_thread_id, created_at, updated_at,
                   archived_at, deleted_at, payload_json, provider_instance_id
                 ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)
                 ON CONFLICT(thread_id) DO UPDATE SET
                   project_id=excluded.project_id, title=excluded.title,
                   default_provider=excluded.default_provider,
                   runtime_mode=excluded.runtime_mode,
                   interaction_mode=excluded.interaction_mode,
                   active_provider_thread_id=excluded.active_provider_thread_id,
                   updated_at=excluded.updated_at, archived_at=excluded.archived_at,
                   deleted_at=excluded.deleted_at, payload_json=excluded.payload_json,
                   provider_instance_id=excluded.provider_instance_id",
                &[
                    &s("id"),
                    &s("projectId"),
                    &s("title"),
                    &p.get("modelSelection").and_then(|m| m.get("instanceId")).and_then(Value::as_str).map(str::to_owned),
                    &s("runtimeMode"),
                    &s("interactionMode"),
                    &s("activeProviderThreadId"),
                    &s("createdAt"),
                    &s("updatedAt"),
                    &s("archivedAt"),
                    &s("deletedAt"),
                    &pj,
                    &s("providerInstanceId"),
                ],
            )
        }
        "run.created" | "run.updated" => run(
            "INSERT INTO orchestration_v2_projection_runs (
               run_id, thread_id, ordinal, provider, provider_thread_id, status,
               requested_at, completed_at, payload_json, provider_instance_id
             ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)
             ON CONFLICT(run_id) DO UPDATE SET
               status=excluded.status, completed_at=excluded.completed_at,
               provider_thread_id=excluded.provider_thread_id,
               payload_json=excluded.payload_json,
               provider_instance_id=excluded.provider_instance_id",
            &[
                &s("id"),
                &s("threadId"),
                &p.get("ordinal").and_then(Value::as_i64),
                &s("provider"),
                &s("providerThreadId"),
                &s("status"),
                &s("requestedAt"),
                &s("completedAt"),
                &pj,
                &s("providerInstanceId"),
            ],
        ),
        "run-attempt.created" | "run-attempt.updated" => run(
            "INSERT INTO orchestration_v2_projection_run_attempts (
               attempt_id, thread_id, run_id, attempt_ordinal, root_node_id,
               provider, provider_thread_id, provider_turn_id, status,
               payload_json, provider_instance_id
             ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)
             ON CONFLICT(attempt_id) DO UPDATE SET
               status=excluded.status, provider_turn_id=excluded.provider_turn_id,
               payload_json=excluded.payload_json",
            &[
                &s("id"),
                &thread_id,
                &s("runId"),
                &p.get("attemptOrdinal").and_then(Value::as_i64),
                &s("rootNodeId"),
                &s("provider"),
                &s("providerThreadId"),
                &s("providerTurnId"),
                &s("status"),
                &pj,
                &s("providerInstanceId"),
            ],
        ),
        "node.updated" => run(
            "INSERT INTO orchestration_v2_projection_nodes (
               node_id, thread_id, run_id, parent_node_id, root_node_id, kind,
               status, provider_thread_id, provider_turn_id, runtime_request_id,
               checkpoint_scope_id, started_at, completed_at, payload_json
             ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14)
             ON CONFLICT(node_id) DO UPDATE SET
               status=excluded.status, run_id=excluded.run_id,
               provider_turn_id=excluded.provider_turn_id,
               runtime_request_id=excluded.runtime_request_id,
               checkpoint_scope_id=excluded.checkpoint_scope_id,
               started_at=excluded.started_at, completed_at=excluded.completed_at,
               payload_json=excluded.payload_json",
            &[
                &s("id"),
                &thread_id,
                &s("runId"),
                &s("parentNodeId"),
                &s("rootNodeId"),
                &s("kind"),
                &s("status"),
                &s("providerThreadId"),
                &s("providerTurnId"),
                &s("runtimeRequestId"),
                &s("checkpointScopeId"),
                &s("startedAt"),
                &s("completedAt"),
                &pj,
            ],
        ),
        "turn-item.updated" => {
            // boundFileChangeForStorage caps stored file_change bodies.
            let mut stored = p.clone();
            if stored.get("type").and_then(Value::as_str) == Some("file_change") {
                for k in ["diffStr", "oldStr", "newStr"] {
                    if let Some(v) = stored.get(k).and_then(Value::as_str) {
                        if v.len() > 32_768 {
                            let mut end = 32_768.min(v.len());
                            while !v.is_char_boundary(end) {
                                end -= 1;
                            }
                            stored[k] = json!(format!("{}{}", &v[..end], "\n… output truncated for transport"));
                        }
                    }
                }
            }
            let pj = serde_json::to_string(&stored).map_err(|e| e.to_string())?;
            let s = |k: &str| stored.get(k).and_then(Value::as_str).map(str::to_owned);
            run(
                "INSERT INTO orchestration_v2_projection_turn_items (
                   turn_item_id, thread_id, run_id, node_id, provider_thread_id,
                   provider_turn_id, parent_item_id, ordinal, type, status,
                   updated_at, payload_json
                 ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)
                 ON CONFLICT(turn_item_id) DO UPDATE SET
                   run_id=excluded.run_id, node_id=excluded.node_id,
                   provider_turn_id=excluded.provider_turn_id,
                   ordinal=excluded.ordinal, status=excluded.status,
                   updated_at=excluded.updated_at, payload_json=excluded.payload_json",
                &[
                    &s("id"),
                    &thread_id,
                    &s("runId"),
                    &s("nodeId"),
                    &s("providerThreadId"),
                    &s("providerTurnId"),
                    &s("parentItemId"),
                    &stored.get("ordinal").and_then(Value::as_i64),
                    &s("type"),
                    &s("status"),
                    &s("updatedAt"),
                    &pj,
                ],
            )?;
            run(
                "INSERT INTO orchestration_v2_turn_item_positions (thread_id, turn_item_id, ordinal) \
                 VALUES (?1,?2,?3) \
                 ON CONFLICT DO NOTHING",
                &[
                    &thread_id,
                    &stored.get("id").and_then(Value::as_str).map(str::to_owned),
                    &stored.get("ordinal").and_then(Value::as_i64),
                ],
            )
        }
        "message.updated" => run(
            "INSERT INTO orchestration_v2_projection_messages (
               message_id, thread_id, run_id, node_id, role, streaming,
               created_at, updated_at, payload_json
             ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)
             ON CONFLICT(message_id) DO UPDATE SET
               run_id=excluded.run_id, node_id=excluded.node_id,
               role=excluded.role, streaming=excluded.streaming,
               updated_at=excluded.updated_at, payload_json=excluded.payload_json",
            &[
                &s("id"),
                &thread_id,
                &s("runId"),
                &s("nodeId"),
                &s("role"),
                &p.get("streaming").and_then(Value::as_bool).map(|b| b as i64),
                &s("createdAt"),
                &s("updatedAt"),
                &pj,
            ],
        ),
        "plan.updated" => run(
            "INSERT INTO orchestration_v2_projection_plans (
               plan_id, thread_id, run_id, node_id, kind, status, payload_json
             ) VALUES (?1,?2,?3,?4,?5,?6,?7)
             ON CONFLICT(plan_id) DO UPDATE SET
               status=excluded.status, payload_json=excluded.payload_json",
            &[&s("id"), &thread_id, &s("runId"), &s("nodeId"), &s("kind"), &s("status"), &pj],
        ),
        "provider-session.attached" | "provider-session.updated" => {
            let thread_id_val = s("threadId").or_else(|| Some(thread_id.to_string()));
            run(
                "INSERT INTO orchestration_v2_projection_provider_sessions (
                   provider_session_id, thread_id, provider, status, model,
                   updated_at, payload_json, driver, provider_instance_id
                 ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)
                 ON CONFLICT(provider_session_id) DO UPDATE SET
                   status=excluded.status, updated_at=excluded.updated_at,
                   payload_json=excluded.payload_json",
                &[
                    &s("id"),
                    &thread_id_val,
                    &s("provider"),
                    &s("status"),
                    &s("model"),
                    &s("updatedAt"),
                    &pj,
                    &s("driver"),
                    &s("providerInstanceId"),
                ],
            )
        }
        "provider-thread.updated" => run(
            "INSERT INTO orchestration_v2_projection_provider_threads (
               provider_thread_id, thread_id, owner_node_id, provider,
               provider_session_id, status, first_run_ordinal, last_run_ordinal,
               updated_at, payload_json, driver, provider_instance_id
             ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)
             ON CONFLICT(provider_thread_id) DO UPDATE SET
               status=excluded.status, last_run_ordinal=excluded.last_run_ordinal,
               updated_at=excluded.updated_at, payload_json=excluded.payload_json",
            &[
                &s("id"),
                &s("appThreadId").or_else(|| Some(thread_id.to_string())),
                &s("ownerNodeId"),
                &s("provider"),
                &s("providerSessionId"),
                &s("status"),
                &p.get("firstRunOrdinal").and_then(Value::as_i64),
                &p.get("lastRunOrdinal").and_then(Value::as_i64),
                &s("updatedAt"),
                &pj,
                &s("driver"),
                &s("providerInstanceId"),
            ],
        ),
        "provider-turn.updated" => run(
            "INSERT INTO orchestration_v2_projection_provider_turns (
               provider_turn_id, thread_id, provider_thread_id, node_id,
               run_attempt_id, ordinal, status, started_at, completed_at, payload_json
             ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)
             ON CONFLICT(provider_turn_id) DO UPDATE SET
               status=excluded.status, completed_at=excluded.completed_at,
               payload_json=excluded.payload_json",
            &[
                &s("id"),
                &thread_id,
                &s("providerThreadId"),
                &s("nodeId"),
                &s("runAttemptId"),
                &p.get("ordinal").and_then(Value::as_i64),
                &s("status"),
                &s("startedAt"),
                &s("completedAt"),
                &pj,
            ],
        ),
        "runtime-request.updated" => run(
            "INSERT INTO orchestration_v2_projection_runtime_requests (
               runtime_request_id, thread_id, node_id, provider_turn_id, kind,
               status, created_at, resolved_at, payload_json
             ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)
             ON CONFLICT(runtime_request_id) DO UPDATE SET
               status=excluded.status, resolved_at=excluded.resolved_at,
               payload_json=excluded.payload_json",
            &[
                &s("id"),
                &thread_id,
                &s("nodeId"),
                &s("providerTurnId"),
                &s("kind"),
                &s("status"),
                &s("createdAt"),
                &s("resolvedAt"),
                &pj,
            ],
        ),
        "subagent.updated" => run(
            "INSERT INTO orchestration_v2_projection_subagents (
               subagent_id, thread_id, run_id, parent_node_id, provider,
               provider_thread_id, child_thread_id, origin, status, started_at,
               completed_at, updated_at, payload_json, driver, provider_instance_id
             ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15)
             ON CONFLICT(subagent_id) DO UPDATE SET
               status=excluded.status, completed_at=excluded.completed_at,
               updated_at=excluded.updated_at, payload_json=excluded.payload_json",
            &[
                &s("id"),
                &thread_id,
                &s("runId"),
                &s("parentNodeId"),
                &s("provider"),
                &s("providerThreadId"),
                &s("childThreadId"),
                &s("origin"),
                &s("status"),
                &s("startedAt"),
                &s("completedAt"),
                &s("updatedAt"),
                &pj,
                &s("driver"),
                &s("providerInstanceId"),
            ],
        ),
        "checkpoint-scope.created" => run(
            "INSERT INTO orchestration_v2_projection_checkpoint_scopes (
               scope_id, thread_id, run_id, node_id, parent_scope_id,
               provider_thread_id, kind, ordinal_within_parent,
               advances_app_run_count, created_at, payload_json
             ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)
             ON CONFLICT(scope_id) DO UPDATE SET payload_json=excluded.payload_json",
            &[
                &s("id"),
                &thread_id,
                &s("runId"),
                &s("nodeId"),
                &s("parentScopeId"),
                &s("providerThreadId"),
                &s("kind"),
                &p.get("ordinalWithinParent").and_then(Value::as_i64),
                &p.get("advancesAppRunCount").and_then(Value::as_bool).map(|b| b as i64),
                &s("createdAt"),
                &pj,
            ],
        ),
        "checkpoint.captured" => run(
            "INSERT INTO orchestration_v2_projection_checkpoints (
               checkpoint_id, thread_id, scope_id, run_id, node_id,
               parent_checkpoint_id, ordinal_within_scope, app_run_ordinal,
               status, captured_at, payload_json
             ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)
             ON CONFLICT(checkpoint_id) DO UPDATE SET
               status=excluded.status, payload_json=excluded.payload_json",
            &[
                &s("id"),
                &thread_id,
                &s("scopeId"),
                &s("runId"),
                &s("nodeId"),
                &s("parentCheckpointId"),
                &p.get("ordinalWithinScope").and_then(Value::as_i64),
                &p.get("appRunOrdinal").and_then(Value::as_i64),
                &s("status"),
                &s("capturedAt"),
                &pj,
            ],
        ),
        "context-handoff.updated" => run(
            "INSERT INTO orchestration_v2_projection_context_handoffs (
               context_handoff_id, thread_id, target_run_id, to_provider_thread_id,
               strategy, status, updated_at, payload_json
             ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8)
             ON CONFLICT(context_handoff_id) DO UPDATE SET
               status=excluded.status, updated_at=excluded.updated_at,
               payload_json=excluded.payload_json",
            &[
                &s("id"),
                &thread_id,
                &s("targetRunId"),
                &s("toProviderThreadId"),
                &s("strategy"),
                &s("status"),
                &s("updatedAt"),
                &pj,
            ],
        ),
        "context-transfer.created" | "context-transfer.updated" => run(
            "INSERT INTO orchestration_v2_projection_context_transfers (
               context_transfer_id, source_thread_id, target_thread_id,
               target_run_id, type, status, source_provider, target_provider,
               updated_at, payload_json, source_provider_instance_id,
               target_provider_instance_id
             ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)
             ON CONFLICT(context_transfer_id) DO UPDATE SET
               status=excluded.status, updated_at=excluded.updated_at,
               payload_json=excluded.payload_json",
            &[
                &s("id"),
                &s("sourceThreadId"),
                &s("targetThreadId"),
                &s("targetRunId"),
                &s("type"),
                &s("status"),
                &s("sourceProvider"),
                &s("targetProvider"),
                &s("updatedAt"),
                &pj,
                &s("sourceProviderInstanceId"),
                &s("targetProviderInstanceId"),
            ],
        ),
        _ => Ok(()),
    }
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

fn apply_command(conn: &Connection, command: &Value) -> Result<ApplyResult, String> {
    let ty = command.get("type").and_then(Value::as_str).unwrap_or("");
    let command_id = command
        .get("commandId")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();

    // Idempotency: a command id already accepted returns its stored sequence.
    if !command_id.is_empty() {
        if let Ok(seq) = conn.query_row(
            "SELECT result_sequence FROM orchestration_v2_command_receipts \
             WHERE command_id = ?1 AND status = 'accepted'",
            params![command_id],
            |r| r.get::<_, i64>(0),
        ) {
            return Ok(ApplyResult {
                sequence: seq,
                events: vec![],
                run_ctx: None,
            });
        }
    }

    match ty {
        "thread.create" => apply_thread_create(conn, command, &command_id),
        "message.dispatch" => apply_message_dispatch(conn, command, &command_id),
        "thread.stop" => apply_thread_stop(conn, command, &command_id),
        other => Err(format!("unsupported command type: {other}")),
    }
}

fn record_receipt(
    conn: &Connection,
    command_id: &str,
    thread_id: &str,
    ty: &str,
    sequence: i64,
) {
    if command_id.is_empty() {
        return;
    }
    let _ = conn.execute(
        "INSERT OR IGNORE INTO orchestration_v2_command_receipts (
           command_id, thread_id, command_type, accepted_at, result_sequence, status, error
         ) VALUES (?1,?2,?3,?4,?5,'accepted',NULL)",
        params![command_id, thread_id, ty, domain::now_iso(), sequence],
    );
}

fn apply_thread_create(
    conn: &Connection,
    command: &Value,
    command_id: &str,
) -> Result<ApplyResult, String> {
    let thread_id = command
        .get("threadId")
        .and_then(Value::as_str)
        .ok_or("thread.create missing threadId")?
        .to_string();
    let now = domain::now_iso();
    let app = domain::app_thread_from_create(command, &now);
    let event = domain::thread_event(
        &thread_id,
        "thread.created",
        app,
        None,
        None,
        None,
        command.get("providerInstanceId").and_then(Value::as_str),
        None,
    );
    let tx = conn.unchecked_transaction().map_err(|e| e.to_string())?;
    let stored = append_event(&tx, "thread", &thread_id, &event, Some(command_id))?;
    apply_projection(&tx, &thread_id, &event)?;
    let seq = stored.sequence;
    tx.commit().map_err(|e| e.to_string())?;
    record_receipt(conn, command_id, &thread_id, "thread.create", seq);
    Ok(ApplyResult {
        sequence: seq,
        events: vec![stored],
        run_ctx: None,
    })
}

fn apply_message_dispatch(
    conn: &Connection,
    command: &Value,
    command_id: &str,
) -> Result<ApplyResult, String> {
    let thread_id = command
        .get("threadId")
        .and_then(Value::as_str)
        .ok_or("message.dispatch missing threadId")?
        .to_string();
    let app = get_thread_row(conn, &thread_id).ok_or("thread not found")?;
    let project_id = app.get("projectId").and_then(Value::as_str).unwrap_or("");
    let workspace_root = conn
        .query_row(
            "SELECT workspace_root FROM projection_projects WHERE project_id = ?1",
            params![project_id],
            |r| r.get::<_, String>(0),
        )
        .unwrap_or_else(|_| String::new());

    let now = domain::now_iso();
    let message_id = command
        .get("messageId")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .unwrap_or_else(domain::new_id);
    let text = command.get("text").and_then(Value::as_str).unwrap_or("").to_string();
    let dispatch_queued = command.get("dispatchMode").and_then(Value::as_str) == Some("queue");
    let model_selection = command
        .get("modelSelection")
        .cloned()
        .or_else(|| app.get("modelSelection").cloned())
        .unwrap_or(Value::Null);
    let provider_instance_id = model_selection
        .get("instanceId")
        .and_then(Value::as_str)
        .or_else(|| app.get("providerInstanceId").and_then(Value::as_str))
        .unwrap_or("")
        .to_string();
    let driver = provider_driver(&provider_instance_id);

    let run_ordinal: i64 = conn
        .query_row(
            "SELECT COALESCE(MAX(ordinal), 0) + 1 FROM orchestration_v2_projection_runs WHERE thread_id = ?1",
            params![thread_id],
            |r| r.get(0),
        )
        .unwrap_or(1);

    let run_id = domain::new_id();
    let attempt_id = domain::new_id();
    let root_node_id = domain::new_id();
    let provider_thread_id = app
        .get("activeProviderThreadId")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .unwrap_or_else(domain::new_id);
    let provider_turn_id = domain::new_id();
    let run_status = if dispatch_queued { "queued" } else { "running" };

    let mut message = json!({
        "createdBy": command.get("createdBy").cloned().unwrap_or(json!("user")),
        "creationSource": command.get("creationSource").cloned().unwrap_or(json!("web")),
        "id": message_id,
        "threadId": thread_id,
        "runId": run_id,
        "nodeId": root_node_id,
        "role": "user",
        "text": text,
        "attachments": command.get("attachments").cloned().unwrap_or(json!([])),
        "streaming": false,
        "createdAt": now,
        "updatedAt": now,
    });
    if let Some(ctx) = command.get("context") {
        message.as_object_mut().unwrap().insert("context".into(), ctx.clone());
    }

    let user_item_id = domain::new_id();
    let user_item = json!({
        "id": user_item_id,
        "threadId": thread_id,
        "runId": run_id,
        "nodeId": root_node_id,
        "providerThreadId": provider_thread_id,
        "providerTurnId": provider_turn_id,
        "nativeItemRef": null,
        "parentItemId": null,
        "ordinal": next_ordinal(conn, &thread_id),
        "status": "completed",
        "title": null,
        "startedAt": now,
        "completedAt": now,
        "updatedAt": now,
        "createdBy": command.get("createdBy").cloned().unwrap_or(json!("user")),
        "creationSource": command.get("creationSource").cloned().unwrap_or(json!("web")),
        "type": "user_message",
        "messageId": message_id,
        "inputIntent": if dispatch_queued { "queued_turn" } else { "turn_start" },
        "text": text,
        "attachments": command.get("attachments").cloned().unwrap_or(json!([])),
    });

    let mut run = json!({
        "id": run_id,
        "threadId": thread_id,
        "ordinal": run_ordinal,
        "providerInstanceId": provider_instance_id,
        "modelSelection": model_selection,
        "providerThreadId": provider_thread_id,
        "userMessageId": message_id,
        "rootNodeId": root_node_id,
        "activeAttemptId": attempt_id,
        "status": run_status,
        "requestedAt": now,
        "startedAt": if dispatch_queued { Value::Null } else { json!(now) },
        "completedAt": null,
        "checkpointId": null,
        "contextHandoffId": null,
    });
    if command.get("freshContext") == Some(&json!(true)) {
        run.as_object_mut().unwrap().insert("freshContext".into(), json!(true));
    }

    let attempt = json!({
        "id": attempt_id,
        "runId": run_id,
        "attemptOrdinal": 1,
        "rootNodeId": root_node_id,
        "providerInstanceId": provider_instance_id,
        "providerThreadId": provider_thread_id,
        "providerTurnId": provider_turn_id,
        "reason": "initial",
        "status": run_status,
        "startedAt": if dispatch_queued { Value::Null } else { json!(now) },
        "completedAt": null,
    });

    let root_node = json!({
        "id": root_node_id,
        "threadId": thread_id,
        "runId": run_id,
        "parentNodeId": null,
        "rootNodeId": root_node_id,
        "kind": "root_turn",
        "status": run_status,
        "countsForRun": true,
        "providerThreadId": provider_thread_id,
        "providerTurnId": provider_turn_id,
        "nativeItemRef": null,
        "runtimeRequestId": null,
        "checkpointScopeId": null,
        "startedAt": if dispatch_queued { Value::Null } else { json!(now) },
        "completedAt": null,
    });

    // Reuse an existing provider thread row when the app thread already has one.
    let existing_pt: Option<Value> = conn
        .query_row(
            "SELECT payload_json FROM orchestration_v2_projection_provider_threads \
             WHERE provider_thread_id = ?1",
            params![provider_thread_id],
            |r| r.get::<_, String>(0),
        )
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok());
    let provider_thread = match existing_pt {
        Some(mut pt) => {
            let o = pt.as_object_mut().unwrap();
            o.insert("status".into(), json!("active"));
            o.insert("lastRunOrdinal".into(), json!(run_ordinal));
            o.insert("updatedAt".into(), json!(now));
            pt
        }
        None => json!({
            "id": provider_thread_id,
            "driver": driver,
            "providerInstanceId": provider_instance_id,
            "providerSessionId": null,
            "appThreadId": thread_id,
            "ownerNodeId": null,
            "nativeThreadRef": null,
            "nativeConversationHeadRef": null,
            "status": "active",
            "firstRunOrdinal": run_ordinal,
            "lastRunOrdinal": run_ordinal,
            "handoffIds": [],
            "pendingBackgroundTasks": [],
            "contextUsage": null,
            "nativeMetadata": null,
            "goal": null,
            "createdAt": now,
            "updatedAt": now,
        }),
    };

    let provider_turn = json!({
        "id": provider_turn_id,
        "providerThreadId": provider_thread_id,
        "nodeId": root_node_id,
        "runAttemptId": attempt_id,
        "nativeTurnRef": null,
        "ordinal": run_ordinal,
        "status": run_status,
        "startedAt": if dispatch_queued { Value::Null } else { json!(now) },
        "completedAt": null,
    });

    // Update the stored AppThread's activeProviderThreadId.
    let mut app_updated = app.clone();
    app_updated["activeProviderThreadId"] = json!(provider_thread_id);
    app_updated["updatedAt"] = json!(now);
    let app_event = domain::thread_event(
        &thread_id,
        "thread.metadata-updated",
        app_updated,
        None,
        None,
        None,
        None,
        None,
    );

    let events: Vec<Value> = vec![
        domain::thread_event(&thread_id, "message.updated", message, Some(&run_id), Some(&root_node_id), None, None, None),
        domain::thread_event(&thread_id, "turn-item.updated", user_item, Some(&run_id), Some(&root_node_id), Some(driver), Some(&provider_instance_id), None),
        domain::thread_event(&thread_id, "run.created", run, Some(&run_id), None, Some(driver), Some(&provider_instance_id), None),
        domain::thread_event(&thread_id, "run-attempt.created", attempt, Some(&run_id), Some(&root_node_id), Some(driver), Some(&provider_instance_id), None),
        domain::thread_event(&thread_id, "node.updated", root_node, Some(&run_id), Some(&root_node_id), Some(driver), Some(&provider_instance_id), None),
        domain::thread_event(&thread_id, "provider-thread.updated", provider_thread, Some(&run_id), None, Some(driver), Some(&provider_instance_id), None),
        domain::thread_event(&thread_id, "provider-turn.updated", provider_turn, Some(&run_id), Some(&root_node_id), Some(driver), Some(&provider_instance_id), None),
        app_event,
    ];

    let committed = commit_events(conn, &thread_id, Some(command_id), events)?;
    let seq = committed.last().map(|e| e.sequence).unwrap_or(0);
    record_receipt(conn, command_id, &thread_id, "message.dispatch", seq);

    Ok(ApplyResult {
        sequence: seq,
        events: committed,
        run_ctx: Some(RunContext {
            thread_id,
            run_id,
            attempt_id,
            root_node_id,
            provider_thread_id,
            provider_turn_id,
            user_message_id: message_id,
            prompt: text,
            model_selection,
            provider_instance_id,
            workspace_root,
            dispatch_queued,
        }),
    })
}

fn next_ordinal(conn: &Connection, thread_id: &str) -> i64 {
    conn.query_row(
        "SELECT COALESCE(MAX(ordinal), -1) + 1 FROM orchestration_v2_projection_turn_items WHERE thread_id = ?1",
        params![thread_id],
        |r| r.get(0),
    )
    .unwrap_or(0)
}

fn provider_driver(instance_id: &str) -> &'static str {
    match instance_id {
        "codex" => "codex",
        "claudeAgent" | "claude" => "claudeCode",
        "cursor" | "cursorAgent" => "cursor",
        "opencode" => "opencode",
        "devReplay" | "replay" => "acpRegistry",
        _ => "acpRegistry",
    }
}

fn apply_thread_stop(
    conn: &Connection,
    command: &Value,
    command_id: &str,
) -> Result<ApplyResult, String> {
    let thread_id = command
        .get("threadId")
        .and_then(Value::as_str)
        .ok_or("thread.stop missing threadId")?
        .to_string();
    let now = domain::now_iso();

    // Latest live run (queued included).
    let run_row: Option<Value> = conn
        .query_row(
            "SELECT payload_json FROM orchestration_v2_projection_runs \
             WHERE thread_id = ?1 AND status IN ('queued','preparing','starting','running','waiting') \
             ORDER BY ordinal DESC, run_id DESC LIMIT 1",
            params![thread_id],
            |r| r.get::<_, String>(0),
        )
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok());

    let Some(mut run) = run_row else {
        return Ok(ApplyResult {
            sequence: latest_sequence(conn),
            events: vec![],
            run_ctx: None,
        });
    };
    let run_id = run.get("id").and_then(Value::as_str).unwrap_or("").to_string();
    let root_node_id = run
        .get("rootNodeId")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let provider_instance_id = run
        .get("providerInstanceId")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let driver = provider_driver(&provider_instance_id);

    run["status"] = json!("interrupted");
    run["completedAt"] = json!(now);

    let mut events = vec![domain::thread_event(
        &thread_id,
        "run.updated",
        run,
        Some(&run_id),
        Some(&root_node_id),
        Some(driver),
        Some(&provider_instance_id),
        None,
    )];

    // Interrupt the active attempt and the root node.
    if let Some(attempt_s) = conn
        .query_row(
            "SELECT payload_json FROM orchestration_v2_projection_run_attempts \
             WHERE run_id = ?1 ORDER BY attempt_ordinal DESC LIMIT 1",
            params![run_id],
            |r| r.get::<_, String>(0),
        )
        .ok()
    {
        if let Ok(mut attempt) = serde_json::from_str::<Value>(&attempt_s) {
            attempt["status"] = json!("interrupted");
            attempt["completedAt"] = json!(now);
            events.push(domain::thread_event(
                &thread_id,
                "run-attempt.updated",
                attempt,
                Some(&run_id),
                Some(&root_node_id),
                Some(driver),
                Some(&provider_instance_id),
                None,
            ));
        }
    }

    // Non-terminal nodes → interrupted.
    let nodes = column_json(
        conn,
        "SELECT payload_json FROM orchestration_v2_projection_nodes \
         WHERE thread_id = ?1 AND status IN ('idle','pending','running','waiting')",
        &[&thread_id],
    );
    for mut node in nodes {
        node["status"] = json!("interrupted");
        node["completedAt"] = json!(now);
        events.push(domain::thread_event(
            &thread_id,
            "node.updated",
            node,
            Some(&run_id),
            Some(&root_node_id),
            Some(driver),
            Some(&provider_instance_id),
            None,
        ));
    }

    // Open provider turn → interrupted.
    if let Some(turn_s) = conn
        .query_row(
            "SELECT payload_json FROM orchestration_v2_projection_provider_turns \
             WHERE thread_id = ?1 AND status IN ('pending','running') \
             ORDER BY ordinal DESC LIMIT 1",
            params![thread_id],
            |r| r.get::<_, String>(0),
        )
        .ok()
    {
        if let Ok(mut turn) = serde_json::from_str::<Value>(&turn_s) {
            turn["status"] = json!("interrupted");
            turn["completedAt"] = json!(now);
            events.push(domain::thread_event(
                &thread_id,
                "provider-turn.updated",
                turn,
                Some(&run_id),
                Some(&root_node_id),
                Some(driver),
                Some(&provider_instance_id),
                None,
            ));
        }
    }

    // In-flight items → interrupted; streaming messages → closed.
    let items = column_json(
        conn,
        "SELECT payload_json FROM orchestration_v2_projection_turn_items \
         WHERE thread_id = ?1 AND run_id = ?2 AND status IN ('pending','running','waiting','idle')",
        &[&thread_id, &run_id],
    );
    for mut item in items {
        item["status"] = json!("interrupted");
        item["completedAt"] = json!(now);
        item["updatedAt"] = json!(now);
        events.push(domain::thread_event(
            &thread_id,
            "turn-item.updated",
            item,
            Some(&run_id),
            Some(&root_node_id),
            Some(driver),
            Some(&provider_instance_id),
            None,
        ));
    }
    let messages = column_json(
        conn,
        "SELECT payload_json FROM orchestration_v2_projection_messages \
         WHERE thread_id = ?1 AND run_id = ?2 AND streaming = 1",
        &[&thread_id, &run_id],
    );
    for mut msg in messages {
        msg["streaming"] = json!(false);
        msg["updatedAt"] = json!(now);
        events.push(domain::thread_event(
            &thread_id,
            "message.updated",
            msg,
            Some(&run_id),
            Some(&root_node_id),
            Some(driver),
            Some(&provider_instance_id),
            None,
        ));
    }

    let committed = commit_events(conn, &thread_id, Some(command_id), events)?;
    let seq = committed.last().map(|e| e.sequence).unwrap_or(0);
    record_receipt(conn, command_id, &thread_id, "thread.stop", seq);
    Ok(ApplyResult {
        sequence: seq,
        events: committed,
        run_ctx: None,
    })
}

fn seed_project(
    conn: &Connection,
    id: &str,
    title: &str,
    workspace_root: &str,
    default_model_selection: Option<Value>,
) {
    let now = domain::now_iso();
    let _ = conn.execute(
        "INSERT OR IGNORE INTO projection_projects (
           project_id, title, workspace_root, scripts_json, created_at, updated_at,
           deleted_at, default_model_selection_json, default_thread_env_mode,
           favicon_path, auto_pull, project_icon_json
         ) VALUES (?1,?2,?3,'[]',?4,?4,NULL,?5,NULL,NULL,0,NULL)",
        params![
            id,
            title,
            workspace_root,
            now,
            default_model_selection.map(|v| v.to_string()),
        ],
    );
}
