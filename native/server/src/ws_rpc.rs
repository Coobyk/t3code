//! Effect-RPC-compatible WebSocket framing: `_tag` envelopes, count-windowed
//! `Chunk` streams gated by client `Ack` frames, and `\u{1e}`-prefixed frame
//! splitting above 48 KiB.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::response::IntoResponse;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::{mpsc, Notify};

use crate::handlers;
use crate::store::WINDOW_SIZE;
use crate::AppState;

const FRAME_PART_BYTES: usize = 48 * 1024;

#[derive(Deserialize)]
pub struct WsParams {
    #[serde(rename = "wsTicket")]
    ws_ticket: Option<String>,
    #[serde(rename = "orchestrationProtocol")]
    orchestration_protocol: Option<String>,
    #[serde(rename = "frameParts")]
    _frame_parts: Option<String>,
}

pub async fn ws_upgrade(
    State(state): State<Arc<AppState>>,
    Query(params): Query<WsParams>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    let ticket_ok = params
        .ws_ticket
        .as_deref()
        .is_some_and(|t| state.tickets.take(t));
    if !ticket_ok {
        return axum::http::Response::builder()
            .status(axum::http::StatusCode::UNAUTHORIZED)
            .body(axum::body::Body::empty())
            .unwrap()
            .into_response();
    }
    if params.orchestration_protocol.as_deref() != Some("2") {
        return axum::http::Response::builder()
            .status(axum::http::StatusCode::BAD_REQUEST)
            .body(axum::body::Body::empty())
            .unwrap()
            .into_response();
    }
    ws.on_upgrade(move |socket| connection(state, socket))
        .into_response()
}

pub type OutTx = mpsc::UnboundedSender<String>;

/// Split a serialized JSON frame the way the TS server does: every part except
/// the last carries a leading `\u{1e}` record separator.
pub fn split_frame(s: &str) -> Vec<String> {
    if s.len() <= FRAME_PART_BYTES {
        return vec![s.to_string()];
    }
    let mut parts = Vec::new();
    let mut start = 0usize;
    while start < s.len() {
        let mut end = (start + FRAME_PART_BYTES).min(s.len());
        while !s.is_char_boundary(end) {
            end -= 1;
        }
        let is_last = end == s.len();
        let mut part = String::with_capacity(end - start + 1);
        if !is_last {
            part.push('\u{1e}');
        }
        part.push_str(&s[start..end]);
        parts.push(part);
        start = end;
    }
    parts
}

pub fn send_json(out: &OutTx, value: &Value) {
    let text = serde_json::to_string(value).unwrap_or_else(|_| "null".into());
    for part in split_frame(&text) {
        let _ = out.send(part);
    }
}

/// Per-request windowed-stream credit tracker.
pub struct Window {
    in_flight: Mutex<usize>,
    notify: Notify,
    cancelled: AtomicBool,
}

impl Window {
    fn new() -> Self {
        Self {
            in_flight: Mutex::new(0),
            notify: Notify::new(),
            cancelled: AtomicBool::new(false),
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    /// Wait until there is credit for another Chunk. False when cancelled.
    pub async fn acquire(&self) -> bool {
        loop {
            if self.is_cancelled() {
                return false;
            }
            if *self.in_flight.lock().unwrap() < WINDOW_SIZE {
                return true;
            }
            self.notify.notified().await;
        }
    }

    /// Send one Chunk worth of values for `request_id` under the window.
    pub async fn send_chunk(&self, out: &OutTx, request_id: u64, values: Vec<Value>) -> bool {
        if !self.acquire().await {
            return false;
        }
        *self.in_flight.lock().unwrap() += 1;
        send_json(
            out,
            &json!({ "_tag": "Chunk", "requestId": request_id, "values": values }),
        );
        true
    }

    fn ack(&self) {
        let mut n = self.in_flight.lock().unwrap();
        if *n > 0 {
            *n -= 1;
        }
        drop(n);
        self.notify.notify_waiters();
    }
}

pub fn exit_success(request_id: u64, value: Value) -> Value {
    json!({
        "_tag": "Exit",
        "requestId": request_id,
        "exit": { "_tag": "Success", "value": value },
    })
}

pub fn exit_interrupt(request_id: u64) -> Value {
    json!({
        "_tag": "Exit",
        "requestId": request_id,
        "exit": { "_tag": "Failure", "cause": { "_tag": "Interrupt" } },
    })
}

pub fn exit_failure(request_id: u64, error: Value) -> Value {
    json!({
        "_tag": "Exit",
        "requestId": request_id,
        "exit": { "_tag": "Failure", "cause": { "_tag": "Fail", "error": error } },
    })
}

async fn connection(state: Arc<AppState>, socket: WebSocket) {
    let (mut sink, mut stream) = socket.split();
    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<String>();
    let out: OutTx = out_tx;

    let writer = tokio::spawn(async move {
        while let Some(text) = out_rx.recv().await {
            if sink.send(Message::Text(text.into())).await.is_err() {
                break;
            }
        }
    });

    let windows: Arc<Mutex<HashMap<u64, Arc<Window>>>> = Arc::new(Mutex::new(HashMap::new()));
    let mut next_live_poll = tokio::time::interval(std::time::Duration::from_millis(250));
    next_live_poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            msg = stream.next() => {
                let Some(Ok(msg)) = msg else { break };
                match msg {
                    Message::Text(text) => {
                        handle_text(&state, &out, &windows, text.as_str());
                    }
                    Message::Close(_) => break,
                    _ => {}
                }
            }
            _ = next_live_poll.tick() => {
                // Idle loop tick; windows and producers are event-driven, but a
                // periodic sweep keeps `Ack` accounting responsive after races.
            }
        }
    }

    for (_, w) in windows.lock().unwrap().drain() {
        w.cancel();
    }
    drop(out);
    let _ = writer.await;
}

fn handle_text(
    state: &Arc<AppState>,
    out: &OutTx,
    windows: &Arc<Mutex<HashMap<u64, Arc<Window>>>>,
    text: &str,
) {
    let parsed: Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(%e, "bad client frame");
            return;
        }
    };
    let frames: Vec<Value> = match &parsed {
        Value::Array(items) => items.clone(),
        _ => vec![parsed],
    };
    for frame in frames {
        match frame.get("_tag").and_then(Value::as_str) {
            Some("Request") => {
                let id = frame.get("id").and_then(Value::as_u64).unwrap_or(0);
                let tag = frame
                    .get("tag")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();
                let payload = frame.get("payload").cloned().unwrap_or(json!({}));
                if handlers::is_windowed_tag(&tag) {
                    let window = Arc::new(Window::new());
                    windows.lock().unwrap().insert(id, window.clone());
                    let st = state.clone();
                    let out2 = out.clone();
                    let wins = windows.clone();
                    tokio::spawn(async move {
                        handlers::run_stream(st, out2.clone(), id, &tag, payload, window.clone())
                            .await;
                        wins.lock().unwrap().remove(&id);
                    });
                } else {
                    let st = state.clone();
                    let out2 = out.clone();
                    tokio::spawn(async move {
                        let result = handlers::handle_request(st, &tag, payload).await;
                        send_json(&out2, &result.to_exit(id));
                    });
                }
            }
            Some("Ack") => {
                let id = frame.get("requestId").and_then(Value::as_u64).unwrap_or(0);
                if let Some(w) = windows.lock().unwrap().get(&id) {
                    w.ack();
                }
            }
            Some("Interrupt") => {
                let id = frame.get("requestId").and_then(Value::as_u64).unwrap_or(0);
                let w = windows.lock().unwrap().remove(&id);
                if let Some(w) = w {
                    w.cancel();
                    send_json(out, &exit_interrupt(id));
                }
            }
            Some("Ping") => {
                send_json(out, &json!({ "_tag": "Pong" }));
            }
            Some("Eof") | Some("Pong") => {}
            other => {
                if let Some(tag) = other {
                    tracing::debug!(tag, "unhandled client frame tag");
                }
            }
        }
    }
}
