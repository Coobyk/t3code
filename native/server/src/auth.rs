use std::collections::HashMap;
use std::sync::Mutex as StdMutex;
use std::time::{Duration, Instant};

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use serde_json::{json, Value};

use crate::AppState;

const TICKET_TTL: Duration = Duration::from_secs(120);

pub struct TicketStore {
    /// ticket -> issued-at. Single-use; consumed on the WS handshake.
    tickets: StdMutex<HashMap<String, Instant>>,
}

impl TicketStore {
    pub fn new() -> Self {
        Self {
            tickets: StdMutex::new(HashMap::new()),
        }
    }

    fn mint(&self) -> String {
        let ticket = uuid::Uuid::new_v4().to_string();
        self.tickets.lock().unwrap().insert(ticket.clone(), Instant::now());
        ticket
    }

    /// Consume a ticket. Valid only once and only within the TTL.
    pub fn take(&self, ticket: &str) -> bool {
        let mut map = self.tickets.lock().unwrap();
        map.retain(|_, at| at.elapsed() < TICKET_TTL);
        map.remove(ticket).is_some()
    }
}

/// `POST /api/auth/websocket-ticket/rpc`
/// Headers: `Authorization: Bearer <token>` → `{ "ticket": "<opaque>" }`.
pub async fn issue_ticket(
    State(state): State<std::sync::Arc<AppState>>,
    headers: HeaderMap,
    _body: Option<Json<Value>>,
) -> Result<Json<Value>, StatusCode> {
    let bearer = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    let authed = match (&state.auth_token, bearer) {
        (Some(expected), Some(got)) => got == expected.as_str(),
        (None, Some(_)) => true,
        _ => false,
    };
    if !authed {
        return Err(StatusCode::UNAUTHORIZED);
    }
    Ok(Json(json!({ "ticket": state.tickets.mint() })))
}
