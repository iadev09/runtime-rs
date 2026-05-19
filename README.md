# runtime-rs

Tokio-native lifecycle and service composition for Rust applications.

`runtime-rs` gives your app one place to register services, boot them,
reload them, run their background tasks, resolve them by Rust type, and shut
them down gracefully:

- register typed services once
- boot / validate / reload / shutdown them in deterministic order
- resolve them later by concrete Rust type
- remove the need for ad-hoc runtime injection
- spawn long-running runnable providers
- drain accepted work through graceful shutdown gates
- publish typed in-process lifecycle events with the `events` feature

The core idea:

```text
Registry<State>
  ├─ DbService      -> Provider + Reloadable
  ├─ CacheService   -> Provider + Runnable
  └─ YourService    -> Provider

Runtime<State>
  └─ spawns every provider that exposes Runnable
```

## Features

Default features are:

```toml
default = ["registry"]
full = ["registry", "support", "events"]
```

- `registry` adds `registry_ref()` to `SharedState` for states that carry
  `Registry<Self>`. The `Registry` type itself is always available.
- `events` enables `LifecycleBus`, adds `events()` to `SharedState`, and
  enables the `dashmap` dependency.
- `support` enables `Gate`, `Permit`, `GuardGroup`, and `Guard`.

## Your State

The crate does not own your app state. Your state only needs to implement
`SharedState`. If you want `registry.reload_all(&state).await`, implement
`ReloadState` too.

```rust
use async_trait::async_trait;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use runtime_rs::LifecycleBus;
use runtime_rs::{Registry, ReloadState, Result, SharedState};

#[derive(Clone)]
pub struct AppState(Arc<Inner>);

struct Inner {
    shutdown: CancellationToken,
    registry: Registry<AppState>,
    events: LifecycleBus,
}

impl Default for AppState {
    fn default() -> Self {
        Self(Arc::new(Inner {
            shutdown: CancellationToken::new(),
            registry: Registry::default(),
            events: LifecycleBus::new(),
        }))
    }
}

impl AppState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn on_shutdown(&self) -> impl std::future::Future<Output = ()> + '_ {
        self.0.shutdown.cancelled()
    }
}

impl SharedState for AppState {
    fn shutdown_token(&self) -> CancellationToken {
        self.0.shutdown.clone()
    }

    fn registry_ref(&self) -> &Registry<Self> {
        &self.0.registry
    }

    fn events(&self) -> &LifecycleBus {
        &self.0.events
    }
}

#[async_trait]
impl ReloadState for AppState {
    async fn reload(&self) -> Result<()> {
        Ok(())
    }
}
```

## The Registry

`Registry<S>` is the main piece. It stores `Arc<T>` values by `TypeId`, while
also treating them as lifecycle providers.

That means one object can be all of these at once:

- a concrete service you can resolve later: `registry.resolve::<DbService>()`
- a lifecycle participant: `boot`, `validate`, `shutdown`
- a hot-reload participant: `Reloadable`
- a long-running background task: `Runnable`

```rust
use std::sync::Arc;

let state = AppState::new();
state
    .registry_ref()
    .insert(Arc::new(DbService::new()))
    .insert(Arc::new(CacheService::new()));

state.registry_ref().boot_all(&state).await?;
state.registry_ref().validate_all(&state)?;

let db = state.registry_ref().resolve::<DbService>().expect("DbService registered");
```

Register once. Boot, reload, run, and resolve by type.

## Providers

A provider is any service that wants to join the application lifecycle.

```rust
use async_trait::async_trait;
use runtime_rs::{Provider, Result};

pub struct DbService;

#[async_trait]
impl Provider<AppState> for DbService {
    fn name(&self) -> &'static str {
        "db"
    }

    async fn boot(
        &self,
        state: &AppState
    ) -> Result<()> {
        // Open pools, read config, build snapshots, publish ready state.
        Ok(())
    }

    fn validate(
        &self,
        state: &AppState
    ) -> Result<()> {
        // Cheap preflight checks before boot.
        Ok(())
    }

    async fn shutdown(
        &self,
        state: &AppState
    ) -> Result<()> {
        // Best-effort cleanup after shutdown begins.
        Ok(())
    }
}
```

Lifecycle order is deterministic. Providers can override `boot_priority()` and
`run_priority()` when ordering matters.

## Runnable Providers

Long-running loops live in `Runnable::run()`, not in `boot()`.

```rust
use runtime_rs::{Provider, Runnable};

impl Runnable<AppState> for CacheService {
    fn run(
        &self,
        state: AppState
    ) -> runtime_rs::registry::TaskFuture {
        Box::pin(async move {
            state.on_shutdown().await;
            Ok(())
        })
    }
}

impl Provider<AppState> for CacheService {
    fn as_runnable(&self) -> Option<&dyn Runnable<AppState>> {
        Some(self)
    }
}
```

Then the runtime starts every runnable provider:

```rust
use runtime_rs::{Runtime, SharedState};

let state = AppState::new();
let mut runtime = Runtime::<AppState>::default();

runtime.spawn_all(state.registry_ref(), state.clone());
state.initiate_shutdown();
runtime.wait_until_shutdown(&state).await?;
runtime.drain().await?;
# Ok::<(), runtime_rs::Error>(())
```

## Reloadable Providers

Reload is a capability, not a second registry.

```rust
use async_trait::async_trait;
use runtime_rs::{Provider, Reloadable, Result};

#[async_trait]
impl Reloadable<AppState> for DbService {
    async fn reload(
        &self,
        state: &AppState
    ) -> Result<()> {
        // Rebuild runtime snapshot and atomically publish it.
        Ok(())
    }
}

impl Provider<AppState> for DbService {
    fn as_reloadable(&self) -> Option<&dyn Reloadable<AppState>> {
        Some(self)
    }
}
```

Use `registry.reload_one("db", &state).await` for targeted reloads or
`registry.reload_all(&state).await` for full reloads.

## Gates

The `support` feature enables optional runtime support tools. They are not
required by `Registry` or `Runtime`, but they are useful when implementing
servers, connection loops, and request handlers.

`Gate` is a graceful shutdown admission/drain tool. It is inspired by axum's
server handle pattern, but lifted out of HTTP.

It can:

- reject new work after graceful shutdown starts
- track accepted in-flight work with `Permit`
- wait until all permits drop
- force shutdown after a grace period
- optionally apply simple max-in-flight admission control

```rust
use std::time::Duration;
use runtime_rs::Gate;

let gate = Gate::new(Some(1024), Duration::from_millis(100));
let permit = gate.enter().await?;

gate.graceful_shutdown(Some(Duration::from_secs(30)));
gate.wait_all_done().await;
# Ok::<(), runtime_rs::gate::Error>(())
```

`GuardGroup` is a smaller RAII in-flight counter for places where you only
need “count active work and wait until zero” without admission control.

## Lifecycle Events

The `events` feature enables `LifecycleBus`, a typed process-local event bus.
Use it when services need a loose in-process signal without depending on each
other directly.

```rust
use runtime_rs::LifecycleBus;

#[derive(Clone, Debug)]
struct ConfigReloaded;

let bus = LifecycleBus::new();
let mut rx = bus.subscribe::<ConfigReloaded>();
bus.emit(ConfigReloaded);
```

## Examples

The library crate keeps small single-file examples:

```text
examples/minimal.rs
examples/axum.rs
```

Run them with:

```sh
cargo run --example minimal --features registry
cargo run --example axum --features registry
```

`axum` is a dev-dependency only. It is there to show integration style; it is
not part of `runtime-rs` for library users.

## Dependencies

The dependency set is intentionally small and canonical for Tokio-based
services:

```toml
async-trait = "0.1"
dashmap = "6"
tokio = { version = "1", features = ["macros", "rt", "sync", "time"] }
tokio-util = "0.7"
tracing = "0.1"
```

`dashmap` is only required by the default `events` feature.
