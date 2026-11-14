//! Minimal client for a 67 server: HTTP websocket-ticket mint + Effect-RPC
//! over WebSocket (`packages/contracts/src/rpc.ts`, JSON serialization).
//!
//! Wire format (`.repos/effect/.../rpc/RpcMessage.ts`):
//!   client -> {"_tag":"Request","id":N,"tag":"method","payload":{...},"headers":[]}
//!   server -> {"_tag":"Chunk","requestId":N,"values":[...]}
//!             {"_tag":"Exit","requestId":N,"exit":{"_tag":"Success","value":...}}
//!             {"_tag":"Ping"}  (answer with {"_tag":"Pong"})

use std::io::ErrorKind;
use std::time::Duration;

use serde_json::{Value, json};
use smol::channel::Sender;

#[derive(Debug, Clone)]
pub struct ServerEndpoint {
    /// e.g. `http://127.0.0.1:13773`
    pub http_url: String,
    /// Bearer credential (`CODE67_DEV_AUTH_TOKEN` in dev).
    pub token: String,
}

impl ServerEndpoint {
    pub fn from_env() -> Option<Self> {
        let http_url = std::env::var("C67_SERVER_URL").ok()?;
        let token = std::env::var("C67_TOKEN").ok()?;
        Some(Self { http_url, token })
    }

    fn ws_url(&self) -> String {
        let mut url = self.http_url.trim_end_matches('/').to_string();
        if let Some(rest) = url.strip_prefix("http://") {
            url = format!("ws://{rest}");
        } else if let Some(rest) = url.strip_prefix("https://") {
            url = format!("wss://{rest}");
        }
        url
    }
}

#[derive(Debug, Clone)]
pub struct ShellProject {
    pub id: String,
    pub title: String,
    pub workspace_root: String,
}

#[derive(Debug, Clone)]
pub struct ShellThread {
    pub id: String,
    pub project_id: String,
    pub title: String,
    pub status: String,
    pub archived: bool,
    pub deleted: bool,
}

#[derive(Debug, Clone)]
pub struct ShellSnapshot {
    pub projects: Vec<ShellProject>,
    pub threads: Vec<ShellThread>,
}

/// A render-ready view of one `OrchestrationV2TurnItem`.
#[derive(Debug, Clone)]
pub struct TurnItemView {
    pub id: String,
    pub kind: String,
    pub title: Option<String>,
    pub body: Option<String>,
    pub streaming: bool,
    pub status: String,
}

#[derive(Debug)]
pub enum ShellEvent {
    Connected,
    Snapshot(ShellSnapshot),
    ProjectUpserted(ShellProject),
    ProjectRemoved { project_id: String },
    ThreadUpserted { thread: ShellThread },
    ThreadRemoved { thread_id: String },
    ThreadSnapshot { items: Vec<TurnItemView> },
    ThreadItemUpdated { item: TurnItemView },
    OlderSettledThreads { threads: Vec<ShellThread> },
    /// Enrichment refresh: `resolvedRepositoryIdentityRoots` is present, so the
    /// snapshot carries project updates only — threads must be preserved.
    ProjectsRefreshed { projects: Vec<ShellProject> },
    Error(String),
    Disconnected(String),
}

#[derive(Debug)]
pub enum ClientCommand {
    SubscribeShell,
    GetOlderSettled,
    SubscribeThread { thread_id: String },
    /// A raw `OrchestrationV2Command` JSON payload.
    Dispatch { command: Value },
    #[allow(dead_code)]
    Interrupt { request_id: u64 },
}

fn parse_project(v: &Value) -> ShellProject {
    ShellProject {
        id: v["id"].as_str().unwrap_or_default().to_string(),
        title: v["title"].as_str().unwrap_or("Untitled project").to_string(),
        workspace_root: v["workspaceRoot"].as_str().unwrap_or_default().to_string(),
    }
}

fn parse_thread(v: &Value) -> ShellThread {
    ShellThread {
        id: v["id"].as_str().unwrap_or_default().to_string(),
        project_id: v["projectId"].as_str().unwrap_or_default().to_string(),
        title: v["title"].as_str().unwrap_or("Untitled").to_string(),
        status: v["status"].as_str().unwrap_or("idle").to_string(),
        archived: !v["archivedAt"].is_null(),
        deleted: !v["deletedAt"].is_null(),
    }
}

fn turn_item_body(item: &Value) -> Option<String> {
    let kind = item["type"].as_str()?;
    let text = match kind {
        "user_message" | "assistant_message" | "reasoning" | "system_notice" | "error" => {
            item["text"].as_str().map(|s| s.to_string())
        }
        "proposed_plan" => item["markdown"].as_str().map(|s| s.to_string()),
        "command_execution" => Some(
            item["input"]
                .as_str()
                .map(|i| format!("$ {i}"))
                .unwrap_or_else(|| "command".to_string()),
        ),
        "file_change" => item["fileName"]
            .as_str()
            .map(|f| format!("edited {f}")),
        "todo_list" => item["steps"].as_array().map(|s| format!("{} steps", s.len())),
        _ => None,
    };
    text.filter(|t| !t.is_empty())
}

fn parse_turn_item(item: &Value) -> Option<TurnItemView> {
    Some(TurnItemView {
        id: item["id"].as_str()?.to_string(),
        kind: item["type"].as_str()?.to_string(),
        title: item["title"].as_str().map(|s| s.to_string()),
        body: turn_item_body(item),
        streaming: item["streaming"].as_bool().unwrap_or(false),
        status: item["status"].as_str().unwrap_or("done").to_string(),
    })
}

/// One item of the `orchestration.subscribeThread` stream.
fn parse_thread_stream_item(v: &Value) -> Option<ShellEvent> {
    match v["kind"].as_str()? {
        "snapshot" => {
            let items = v["projection"]["visibleTurnItems"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|entry| parse_turn_item(&entry["item"]))
                        .collect()
                })
                .unwrap_or_default();
            Some(ShellEvent::ThreadSnapshot { items })
        }
        "event" => {
            let event = &v["event"];
            if event["type"].as_str() == Some("turn-item.updated") {
                parse_turn_item(&event["payload"])
                    .map(|item| ShellEvent::ThreadItemUpdated { item })
            } else {
                None
            }
        }
        _ => None,
    }
}

fn parse_stream_item(v: &Value) -> Option<ShellEvent> {
    match v["kind"].as_str()? {
        "snapshot" => {
            let s = &v["snapshot"];
            if v["resolvedRepositoryIdentityRoots"].is_array() {
                return Some(ShellEvent::ProjectsRefreshed {
                    projects: s["projects"]
                        .as_array()
                        .map(|a| a.iter().map(parse_project).collect())
                        .unwrap_or_default(),
                });
            }
            Some(ShellEvent::Snapshot(ShellSnapshot {
                projects: s["projects"]
                    .as_array()
                    .map(|a| a.iter().map(parse_project).collect())
                    .unwrap_or_default(),
                threads: s["threads"]
                    .as_array()
                    .map(|a| a.iter().map(parse_thread).collect())
                    .unwrap_or_default(),
            }))
        }
        "project.updated" => Some(ShellEvent::ProjectUpserted(parse_project(&v["project"]))),
        "project.removed" => Some(ShellEvent::ProjectRemoved {
            project_id: v["projectId"].as_str().unwrap_or_default().to_string(),
        }),
        "thread.updated" => Some(ShellEvent::ThreadUpserted {
            thread: parse_thread(&v["thread"]),
        }),
        "thread.removed" => Some(ShellEvent::ThreadRemoved {
            thread_id: v["threadId"].as_str().unwrap_or_default().to_string(),
        }),
        "synchronized" | "snapshot-required" => None,
        _ => None,
    }
}

fn mint_ticket(endpoint: &ServerEndpoint) -> Result<String, String> {
    let url = format!(
        "{}/api/auth/websocket-ticket/rpc",
        endpoint.http_url.trim_end_matches('/')
    );
    let mut response = ureq::post(&url)
        .header("authorization", format!("Bearer {}", endpoint.token))
        .header("content-type", "application/json")
        .send_json(json!({}))
        .map_err(|e| format!("ticket request failed: {e}"))?;
    let body: Value = response
        .body_mut()
        .read_json()
        .map_err(|e| format!("ticket response decode failed: {e}"))?;
    body["ticket"]
        .as_str()
        .map(|s| s.to_string())
        .ok_or_else(|| format!("no ticket in response: {body}"))
}

/// Connects, subscribes to the orchestration shell stream, and forwards parsed
/// events on `events`. Runs on a dedicated thread; `commands` carries outbound
/// RPC requests. Returns when the socket closes.
pub fn run(
    endpoint: ServerEndpoint,
    events: Sender<ShellEvent>,
    commands: std::sync::mpsc::Receiver<ClientCommand>,
) {
    if let Err(e) = run_inner(endpoint, events.clone(), commands) {
        let _ = events.send_blocking(ShellEvent::Disconnected(e));
    }
}

fn run_inner(
    endpoint: ServerEndpoint,
    events: Sender<ShellEvent>,
    commands: std::sync::mpsc::Receiver<ClientCommand>,
) -> Result<(), String> {
    let ticket = mint_ticket(&endpoint)?;
    let ws_url = format!("{}/ws?wsTicket={ticket}&frameParts=1&orchestrationProtocol=2", endpoint.ws_url());

    let (mut socket, _response) = tungstenite::connect(&ws_url)
        .map_err(|e| format!("websocket connect failed: {e}"))?;
    if let tungstenite::stream::MaybeTlsStream::Plain(tcp) = socket.get_mut() {
        tcp.set_read_timeout(Some(Duration::from_millis(50)))
            .map_err(|e| format!("socket config failed: {e}"))?;
    }

    let _ = events.send_blocking(ShellEvent::Connected);

    let mut next_id: u64 = 1;
    let mut subscriptions: std::collections::HashMap<u64, SubKind> =
        std::collections::HashMap::new();
    let mut pending_commands: std::collections::VecDeque<ClientCommand> =
        [ClientCommand::SubscribeShell, ClientCommand::GetOlderSettled].into();

    loop {
        if let Some(command) = pending_commands.pop_front() {
            send_command(&mut socket, &mut subscriptions, next_id, &command)
                .map_err(|e| format!("send failed: {e}"))?;
            next_id += 1;
        }
        while let Ok(command) = commands.try_recv() {
            send_command(&mut socket, &mut subscriptions, next_id, &command)
                .map_err(|e| format!("send failed: {e}"))?;
            next_id += 1;
        }

        match socket.read() {
            Ok(tungstenite::Message::Text(text)) => {
                handle_frame(&text, &mut socket, &events, &mut subscriptions)?;
            }
            Ok(tungstenite::Message::Ping(payload)) => {
                let _ = socket.send(tungstenite::Message::Pong(payload));
            }
            Ok(tungstenite::Message::Close(_)) => {
                return Err("socket closed by server".into());
            }
            Ok(_) => {}
            Err(tungstenite::Error::Io(e))
                if e.kind() == ErrorKind::WouldBlock || e.kind() == ErrorKind::TimedOut => {}
            Err(tungstenite::Error::Io(e)) => {
                return Err(format!("socket io error: {e}"));
            }
            Err(e) => return Err(format!("socket error: {e}")),
        }
    }
}

#[derive(Debug)]
enum SubKind {
    Shell,
    Thread,
    OlderSettled,
}

fn send_command(
    socket: &mut tungstenite::WebSocket<tungstenite::stream::MaybeTlsStream<std::net::TcpStream>>,
    subscriptions: &mut std::collections::HashMap<u64, SubKind>,
    id: u64,
    command: &ClientCommand,
) -> Result<(), tungstenite::Error> {
    match command {
        ClientCommand::SubscribeShell => {
            subscriptions.insert(id, SubKind::Shell);
        }
        ClientCommand::GetOlderSettled => {
            subscriptions.insert(id, SubKind::OlderSettled);
        }
        ClientCommand::SubscribeThread { .. } => {
            // One live thread stream at a time: interrupt the previous one.
            let stale: Vec<u64> = subscriptions
                .iter()
                .filter(|(_, kind)| matches!(kind, SubKind::Thread))
                .map(|(rid, _)| *rid)
                .collect();
            for rid in stale {
                subscriptions.remove(&rid);
                let _ = socket.send(tungstenite::Message::Text(
                    json!({"_tag": "Interrupt", "requestId": rid, "interruptors": []})
                        .to_string()
                        .into(),
                ));
            }
            subscriptions.insert(id, SubKind::Thread);
        }
        _ => {}
    }
    let frame = match command {
        ClientCommand::SubscribeShell => json!({
            "_tag": "Request",
            "id": id,
            "tag": "orchestration.subscribeShell",
            "payload": { "requestCompletionMarker": true },
            "headers": [],
        }),
        ClientCommand::GetOlderSettled => json!({
            "_tag": "Request",
            "id": id,
            "tag": "orchestration.getOlderSettledThreads",
            "payload": {
                "before": { "settledAt": "2999-01-01T00:00:00.000Z" },
                "limit": 50,
            },
            "headers": [],
        }),
        ClientCommand::SubscribeThread { thread_id } => json!({
            "_tag": "Request",
            "id": id,
            "tag": "orchestration.subscribeThread",
            "payload": { "threadId": thread_id, "requestCompletionMarker": true },
            "headers": [],
        }),
        ClientCommand::Dispatch { command } => json!({
            "_tag": "Request",
            "id": id,
            "tag": "orchestration.dispatchCommand",
            "payload": command,
            "headers": [],
        }),
        ClientCommand::Interrupt { request_id } => json!({
            "_tag": "Interrupt",
            "requestId": request_id,
        }),
    };
    socket.send(tungstenite::Message::Text(frame.to_string().into()))
}

fn handle_frame(
    text: &tungstenite::Utf8Bytes,
    socket: &mut tungstenite::WebSocket<tungstenite::stream::MaybeTlsStream<std::net::TcpStream>>,
    events: &Sender<ShellEvent>,
    subscriptions: &mut std::collections::HashMap<u64, SubKind>,
) -> Result<(), String> {
    let frames: Vec<Value> = serde_json::from_str::<Value>(text)
        .map(|v| if v.is_array() { vec![v] } else { vec![v] })
        .and_then(|v| {
            // Frames may arrive as a single object or an array of them.
            if let Value::Array(items) = &v[0] {
                Ok(items.clone())
            } else {
                Ok(v)
            }
        })
        .map_err(|e| format!("frame decode failed: {e}"))?;

    for frame in frames {
        match frame["_tag"].as_str() {
            Some("Chunk") => {
                let request_id = frame["requestId"].as_u64().unwrap_or(0);
                // Stream chunks are windowed (8 per request): ack each one so the
                // server keeps writing.
                let _ = socket.send(tungstenite::Message::Text(
                    json!({"_tag": "Ack", "requestId": request_id})
                        .to_string()
                        .into(),
                ));
                let is_thread = matches!(subscriptions.get(&request_id), Some(SubKind::Thread));
                if std::env::var("C67_DEBUG").is_ok() {
                    eprintln!("[chunk] req={request_id} is_thread={is_thread} kinds={:?}",
                        frame["values"].as_array().map(|a| a.iter().map(|v| v["kind"].as_str().unwrap_or("?").to_string()).collect::<Vec<_>>()));
                }
                if let Some(values) = frame["values"].as_array() {
                    for value in values {
                        let parsed = if is_thread {
                            if std::env::var("C67_DEBUG").is_ok()
                                && value["kind"].as_str() == Some("event")
                            {
                                eprintln!("[ev] type={:?} parsed={}",
                                    value["event"]["type"].as_str(),
                                    parse_thread_stream_item(value).is_some());
                            }
                            parse_thread_stream_item(value)
                        } else {
                            parse_stream_item(value)
                        };
                        if let Some(event) = parsed {
                            let _ = events.send_blocking(event);
                        }
                    }
                }
            }
            Some("Exit") => {
                let request_id = frame["requestId"].as_u64();
                let kind = request_id.and_then(|rid| subscriptions.remove(&rid));
                if frame["exit"]["_tag"].as_str() == Some("Success")
                    && matches!(kind, Some(SubKind::OlderSettled))
                {
                    let threads = frame["exit"]["value"]["threads"]
                        .as_array()
                        .map(|a| a.iter().map(parse_thread).collect())
                        .unwrap_or_default();
                    let _ = events.send_blocking(ShellEvent::OlderSettledThreads { threads });
                }
                if frame["exit"]["_tag"].as_str() != Some("Success") {
                    let _ = events.send_blocking(ShellEvent::Error(format!(
                        "request failed: {}",
                        frame["exit"]
                    )));
                }
            }
            Some("Ping") => {
                let _ = socket.send(tungstenite::Message::Text(
                    json!({"_tag": "Pong"}).to_string().into(),
                ));
            }
            Some("Defect") => {
                let _ = events.send_blocking(ShellEvent::Error(format!(
                    "server defect: {}",
                    frame["defect"]
                )));
            }
            _ => {}
        }
    }
    Ok(())
}
