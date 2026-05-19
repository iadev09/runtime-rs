use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
#[cfg(feature = "events")]
use runtime_rs::LifecycleBus;
use runtime_rs::{
    Error, Provider, Registry, ReloadState, Reloadable, Result, Runnable, Runtime, SharedState
};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
struct State(Arc<Inner>);

struct Inner {
    shutdown: CancellationToken,
    registry: Registry<State>,
    demo_interval_ms: AtomicU64,
    #[cfg(feature = "events")]
    events: LifecycleBus
}

impl Default for State {
    fn default() -> Self {
        Self(Arc::new(Inner {
            shutdown: CancellationToken::new(),
            registry: Registry::default(),
            demo_interval_ms: AtomicU64::new(1_000),
            #[cfg(feature = "events")]
            events: LifecycleBus::new()
        }))
    }
}

impl State {
    fn new() -> Self {
        Self::default()
    }

    fn on_shutdown(&self) -> impl Future<Output = ()> + '_ {
        self.0.shutdown.cancelled()
    }

    fn demo_interval(&self) -> Duration {
        Duration::from_millis(self.0.demo_interval_ms.load(Ordering::Relaxed))
    }

    fn speed_up_demo(&self) {
        self.0.demo_interval_ms.store(300, Ordering::Relaxed);
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

#[async_trait]
impl ReloadState for State {
    async fn reload(&self) -> Result<()> {
        println!("State reloaded");
        Ok(())
    }
}

struct IdleService;

struct DbProvider;

struct ControlProvider;

#[async_trait]
impl Provider<State> for IdleService {
    fn name(&self) -> &'static str {
        "idle"
    }

    async fn boot(
        &self,
        _state: &State
    ) -> Result<()> {
        println!("Idle Service booted");
        Ok(())
    }

    fn validate(
        &self,
        _state: &State
    ) -> Result<()> {
        println!("Idle Service validated");
        Ok(())
    }

    fn as_reloadable(&self) -> Option<&dyn Reloadable<State>> {
        Some(self)
    }

    fn as_runnable(&self) -> Option<&dyn Runnable<State>> {
        Some(self)
    }
}

impl Runnable<State> for IdleService {
    fn run(
        &self,
        state: State
    ) -> runtime_rs::registry::TaskFuture {
        Box::pin(start_service(state))
    }
}

#[async_trait]
impl Reloadable<State> for IdleService {
    async fn reload(
        &self,
        state: &State
    ) -> Result<()> {
        state.speed_up_demo();
        println!("Idle Service reloaded; interval is now 300ms");
        Ok(())
    }
}

#[async_trait]
impl Provider<State> for DbProvider {
    fn name(&self) -> &'static str {
        "db"
    }

    async fn boot(
        &self,
        _state: &State
    ) -> Result<()> {
        println!("DB provider booted");
        Ok(())
    }

    fn validate(
        &self,
        _state: &State
    ) -> Result<()> {
        println!("DB provider validated");
        Ok(())
    }

    fn as_runnable(&self) -> Option<&dyn Runnable<State>> {
        Some(self)
    }
}

impl Runnable<State> for DbProvider {
    fn run(
        &self,
        state: State
    ) -> runtime_rs::registry::TaskFuture {
        Box::pin(run_db_probe(state))
    }
}

#[async_trait]
impl Provider<State> for ControlProvider {
    fn name(&self) -> &'static str {
        "control"
    }

    async fn boot(
        &self,
        _state: &State
    ) -> Result<()> {
        println!("Control provider booted; it will send reload and then shut down");
        Ok(())
    }

    fn validate(
        &self,
        _state: &State
    ) -> Result<()> {
        println!("Control provider validated");
        Ok(())
    }

    fn as_runnable(&self) -> Option<&dyn Runnable<State>> {
        Some(self)
    }
}

impl Runnable<State> for ControlProvider {
    fn run(
        &self,
        state: State
    ) -> runtime_rs::registry::TaskFuture {
        Box::pin(run_control_provider(state))
    }
}

async fn run_db_probe(state: State) -> Result<()> {
    tokio::select! {
        _ = state.on_shutdown() => Ok(()),
        _ = tokio::time::sleep(Duration::from_millis(500)) => {
            println!("DB provider reports a recoverable error");
            Err(Error::recoverable("temporary database probe failed"))
        }
    }
}

async fn run_control_provider(state: State) -> Result<()> {
    tokio::time::sleep(Duration::from_secs(3)).await;
    println!("Control provider sending reload signal (simulates SIGHUP)");
    state.registry_ref().reload_all(&state).await?;

    tokio::time::sleep(Duration::from_secs(2)).await;
    println!("Control provider initiating shutdown");
    state.initiate_shutdown();

    Ok(())
}

async fn start_service(state: State) -> Result<()> {
    println!("Idle Service starting");

    loop {
        tokio::select! {
            _ = state.on_shutdown() => {
                println!("🔸 Idle Service got shutdown signal");
                return Ok(());
            }
            _ = tokio::time::sleep(state.demo_interval()) => {
                println!("Idle Service is working");
            }
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let state = State::new();
    state.registry_ref().insert(Arc::new(IdleService));
    state.registry_ref().insert(Arc::new(DbProvider));
    state.registry_ref().insert(Arc::new(ControlProvider));
    state.registry_ref().boot_all(&state).await?;
    state.registry_ref().validate_all(&state)?;

    let provider = state.registry_ref().resolve::<IdleService>().expect("IdleProvider registered");
    println!("Resolved provider: {}", provider.name());

    let mut runtime = Runtime::<State>::default();
    runtime.spawn_all(state.registry_ref(), state.clone());

    runtime.wait_until_shutdown(&state).await?;
    println!("Waiting for all services to finish gracefully..");
    runtime.drain().await?;
    println!("All services finished gracefully");
    Ok(())
}
