mod auth;
mod domain;
mod handlers;
mod provider;
mod store;
mod ws_rpc;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Mutex as StdMutex;

use axum::routing::{get, post};
use axum::Router;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::store::{CommittedEvent, StoreHandle};

/// Provider turns in flight, keyed by thread id. `thread.stop` cancels them.
pub type TurnRegistry = StdMutex<HashMap<String, Vec<CancellationToken>>>;

pub struct AppState {
    /// Dev auth token; when `None`, any bearer token is accepted.
    pub auth_token: Option<String>,
    pub tickets: auth::TicketStore,
    pub store: StoreHandle,
    /// Broadcast of committed domain events (sequences assigned at commit).
    pub events_tx: broadcast::Sender<CommittedEvent>,
    pub turns: TurnRegistry,
    /// Directory holding `statev2.sqlite` and provider state.
    pub home_dir: PathBuf,
    /// Directory containing dev replay fixtures (`apps/server/scripts/dev-replay`).
    pub replay_dir: PathBuf,
}

fn env_first(names: &[&str]) -> Option<String> {
    names.iter().find_map(|n| std::env::var(n).ok().filter(|v| !v.is_empty()))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "c67_server=info".into()),
        )
        .init();

    let port: u16 = env_first(&["C67_PORT", "CODE67_PORT", "T3CODE_PORT"])
        .and_then(|v| v.parse().ok())
        .unwrap_or(13773);

    let auth_token = env_first(&[
        "C67_DEV_AUTH_TOKEN",
        "CODE67_DEV_AUTH_TOKEN",
        "T3CODE_DEV_AUTH_TOKEN",
        "C67_AUTH_TOKEN",
    ]);

    let home_dir = env_first(&["C67_HOME", "CODE67_HOME", "T3CODE_HOME"])
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(".t3"));
    let data_dir = home_dir.join("userdata");
    std::fs::create_dir_all(&data_dir)?;
    let db_path = data_dir.join("statev2.sqlite");

    // Seed from a fixture DB if requested and the DB is missing.
    if !db_path.exists() {
        if let Some(seed) = env_first(&["C67_SEED_DB"]) {
            let seed_path = PathBuf::from(&seed);
            if seed_path.exists() {
                tracing::info!(from = %seed, "seeding statev2.sqlite from fixture");
                std::fs::copy(&seed_path, &db_path)?;
            }
        }
    }

    let replay_dir = env_first(&["C67_REPLAY_DIR"])
        .map(PathBuf::from)
        .or_else(|| {
            // Find apps/server/scripts/dev-replay from cwd or an ancestor.
            let cwd = std::env::current_dir().ok()?;
            let mut dir = cwd.as_path();
            loop {
                let candidate = dir.join("apps/server/scripts/dev-replay");
                if candidate.join("showcase_transcript.ndjson").exists() {
                    return Some(candidate);
                }
                dir = dir.parent()?;
            }
        })
        .unwrap_or_else(|| PathBuf::from("apps/server/scripts/dev-replay"));

    let store = StoreHandle::spawn(&db_path)?;
    let (events_tx, _rx) = broadcast::channel(2048);

    let state = std::sync::Arc::new(AppState {
        auth_token,
        tickets: auth::TicketStore::new(),
        store,
        events_tx,
        turns: StdMutex::new(HashMap::new()),
        home_dir: home_dir.clone(),
        replay_dir,
    });

    // Guarantee at least one project so clients have somewhere to create threads.
    handlers::ensure_seed_project(&state).await?;

    let app = Router::new()
        .route("/api/health", get(|| async { "ok" }))
        .route("/api/auth/websocket-ticket/rpc", post(auth::issue_ticket))
        .route("/ws", get(ws_rpc::ws_upgrade))
        .with_state(state);

    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    tracing::info!(%addr, "c67-server listening");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
