use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use axum::extract::State as AxumState;
use axum::routing::get;
use axum::{Json, Router};
#[cfg(feature = "events")]
use runtime_rs::LifecycleBus;
use runtime_rs::{Provider, Registry, Result, SharedState};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
struct State(Arc<Inner>);

struct Inner {
    shutdown: CancellationToken,
    registry: Registry<State>,
    #[cfg(feature = "events")]
    events: LifecycleBus
}

impl State {
    fn new() -> Self {
        Self(Arc::new(Inner {
            shutdown: CancellationToken::new(),
            registry: Registry::default(),
            #[cfg(feature = "events")]
            events: LifecycleBus::new()
        }))
    }
}

impl SharedState for State {
    fn shutdown_token(&self) -> CancellationToken {
        self.0.shutdown.clone()
    }

    fn registry_ref(&self) -> &Registry<Self> {
        &self.0.registry
    }

    #[cfg(feature = "events")]
    fn events(&self) -> &LifecycleBus {
        &self.0.events
    }
}

struct DbService;

impl DbService {
    fn health(&self) -> &'static str {
        "db:ok; service resolved from runtime-rs Registry"
    }
}

#[async_trait]
impl Provider<State> for DbService {
    fn name(&self) -> &'static str {
        "db"
    }
}

async fn health(AxumState(state): AxumState<State>) -> Json<&'static str> {
    let db = state.registry_ref().resolve::<DbService>().expect("DbService registered");
    Json(db.health())
}

#[tokio::main]
async fn main() -> Result<()> {
    let state = State::new();
    state.registry_ref().insert(Arc::new(DbService));
    state.registry_ref().boot_all(&state).await?;
    state.registry_ref().validate_all(&state)?;

    let app = Router::new().route("/health", get(health)).with_state(state.clone());

    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    println!("Axum example listening on http://127.0.0.1:3000/health");

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            tokio::signal::ctrl_c().await.ok();
            state.initiate_shutdown();
        })
        .await?;

    Ok(())
}
