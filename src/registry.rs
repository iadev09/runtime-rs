use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use tracing::{Instrument, error, info, warn};

// =====================================================================
// Error model
// =====================================================================
//
// The error type is deliberately scoped to the `registry` module because
// it is a *registry* concern (lifecycle stages: boot / validate / reload
// / run). Other helper primitives in this crate (`gate`, `guard`) have their
// own semantics and intentionally do not share this type.
//
// Consumers return any `std::error::Error + Send + Sync + 'static`
// from their `Provider` / `Reloadable` / `Runnable` methods — the blanket
// `From<E>` impl wraps it into `Error::Other`. The registry then re-wraps
// `Error::Other` into the appropriate lifecycle variant (`Boot`, `Reload`,
// `Run`, `Validate`) at the call site, so downstream logs and matches see
// where the failure happened. Providers that want to emit a typed variant
// themselves can construct it directly — the registry will leave it
// untouched.

pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub enum Error {
    Boot {
        name: &'static str,
        source: BoxError
    },
    Validate {
        name: &'static str,
        source: BoxError
    },
    Reload {
        name: &'static str,
        source: BoxError
    },
    /// Fatal runnable failure (default). The runtime tears the worker
    /// down so the supervisor can respawn cleanly.
    Run {
        name: &'static str,
        source: BoxError
    },
    /// Recoverable runnable failure. The runtime logs and keeps the
    /// worker serving — used for best-effort tasks (e.g. notify
    /// listeners, optional integrations) where a transient or
    /// configuration-driven failure shouldn't kill traffic.
    Recoverable {
        name: &'static str,
        source: BoxError
    },
    Other(BoxError)
}

impl std::fmt::Display for Error {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>
    ) -> std::fmt::Result {
        match self {
            Error::Boot { name, source } => {
                write!(f, "provider '{name}' failed during boot: {source}")
            }
            Error::Validate { name, source } => {
                write!(f, "provider '{name}' failed during validate: {source}")
            }
            Error::Reload { name, source } => {
                write!(f, "reload of '{name}' failed: {source}")
            }
            Error::Run { name, source } => {
                write!(f, "runnable '{name}' failed: {source}")
            }
            Error::Recoverable { name, source } => {
                write!(f, "runnable '{name}' failed (recoverable): {source}")
            }
            Error::Other(e) => std::fmt::Display::fmt(e, f)
        }
    }
}

// NOTE: `Error` intentionally does NOT implement `std::error::Error`.
// The blanket `From<E: Error>` below requires that `Error` itself not
// satisfy that bound (otherwise it would conflict with the core
// `From<T> for T` blanket). Consumers that need to chain `source()` can
// match on the variant and walk `BoxError` directly.

impl<E> From<E> for Error
where
    E: std::error::Error + Send + Sync + 'static
{
    fn from(e: E) -> Self {
        Error::Other(Box::new(e))
    }
}

/// Construct `Error::Other` from an arbitrary message string.
impl Error {
    pub fn msg(s: impl Into<String>) -> Self {
        #[derive(Debug)]
        struct MsgErr(String);
        impl std::fmt::Display for MsgErr {
            fn fmt(
                &self,
                f: &mut std::fmt::Formatter<'_>
            ) -> std::fmt::Result {
                std::fmt::Display::fmt(&self.0, f)
            }
        }
        impl std::error::Error for MsgErr {}
        Error::Other(Box::new(MsgErr(s.into())))
    }

    /// If the error is `Other`, re-wrap it as `Boot { name, source }`;
    /// otherwise leave it untouched. Used by `Registry::boot_all` to
    /// attach lifecycle context to anonymous user errors.
    fn into_boot(
        self,
        name: &'static str
    ) -> Self {
        match self {
            Error::Other(source) => Error::Boot { name, source },
            other => other
        }
    }
    fn into_validate(
        self,
        name: &'static str
    ) -> Self {
        match self {
            Error::Other(source) => Error::Validate { name, source },
            other => other
        }
    }
    /// Used by `reload_one` (targeted, fail-fast). `reload_all` is broadcast
    /// and intentionally fail-soft — a single provider's failure should not
    /// cancel the rest, so that path just logs a warning.
    fn into_reload(
        self,
        name: &'static str
    ) -> Self {
        match self {
            Error::Other(source) => Error::Reload { name, source },
            other => other
        }
    }
    fn into_run(
        self,
        name: &'static str
    ) -> Self {
        match self {
            Error::Other(source) => Error::Run { name, source },
            // Runnables that opt into recoverable failure construct
            // `Recoverable` with an empty `name`; `run_all` fills in the
            // provider name here so log lines stay attributed.
            Error::Recoverable { name: "", source } => Error::Recoverable { name, source },
            other => other
        }
    }

    /// Build a recoverable runnable error from an arbitrary message.
    /// The runtime logs this and lets the worker keep serving instead of
    /// tearing it down. The provider `name` is filled in by `run_all`'s
    /// wrapper, so callers only supply the message.
    pub fn recoverable(s: impl Into<String>) -> Self {
        #[derive(Debug)]
        struct MsgErr(String);
        impl std::fmt::Display for MsgErr {
            fn fmt(
                &self,
                f: &mut std::fmt::Formatter<'_>
            ) -> std::fmt::Result {
                std::fmt::Display::fmt(&self.0, f)
            }
        }
        impl std::error::Error for MsgErr {}
        Error::Recoverable { name: "", source: Box::new(MsgErr(s.into())) }
    }
}

// =====================================================================
// Priority helpers
// =====================================================================

/// Shared lifecycle priority definitions for providers/reloadables.
///
/// Lower values run earlier.
pub mod priority {
    pub const FIRST: u8 = 0;
    pub const EARLY: u8 = 50;
    pub const NORMAL: u8 = 100;
    pub const LATE: u8 = 150;
    pub const LAST: u8 = u8::MAX;
}

#[async_trait]
pub trait ReloadState: Send + Sync + Sized + 'static {
    async fn reload(&self) -> Result<()>;
}

/// Anything that can hot-reload itself when config changes.
///
/// `reload()` is the same shape as `Provider::boot()` — re-read the
/// on-disk config (use `tokio::fs`, never `std::fs` in this async path)
/// and rebuild the runtime snapshot, publishing it through an
/// `ArcSwap` so in-flight requests/connections see the swap atomically.
/// Reload must NOT change which providers are registered; it only
/// refreshes state of an already-registered provider.
#[async_trait]
pub trait Reloadable<S>: Send + Sync + 'static {
    /// Optional reload priority.
    ///
    /// Lower values run earlier. `None` means `priority::NORMAL`.
    fn priority(&self) -> Option<u8> {
        None
    }

    /// Perform a synchronous reload using the current shared state.
    /// Implementations may spawn async work internally if needed.
    async fn reload(
        &self,
        state: &S
    ) -> Result<()>;
}

/// Boxed future type returned by long-running providers (server loops/background workers).
pub type TaskFuture = Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>>;

/// Capability trait for providers that produce a long-running runtime task.
///
/// `run()` is the ONLY place in the lifecycle for long-running work
/// (accept loops, listeners, periodic tickers). It must NOT appear in
/// `register()` or `Provider::boot()`.
///
/// Config-driven gating: if the provider is disabled at runtime (e.g.
/// an `enabled: false` config flag, or a single-instance service whose
/// pinned `worker_id` doesn't match this worker), the future returned
/// here MUST short-circuit and return `Ok(())` immediately instead of
/// starting the long task. The provider stays registered for downstream
/// capability lookups; it just doesn't run on this process.
pub trait Runnable<S>: Send + Sync + 'static {
    /// Build the task future to be spawned by the bootstrap/supervisor layer.
    ///
    /// NOTICE (convention):
    /// If this future returns `Err`, implementation should log contextual
    /// failure details itself (provider/task specific metadata).
    ///
    /// Reason:
    /// - Runtime layer handles lifecycle/control-flow only.
    /// - Runtime cannot reliably attach provider-specific business context.
    /// - Non-critical runnable errors are not centrally logged to avoid
    ///   duplicate/no-context error lines.
    fn run(
        &self,
        state: S
    ) -> TaskFuture;
}

/// Any service that can be registered in the DI registry.
///
/// # Lifecycle convention
///
/// Each provider lives in four explicit phases. Mixing work across
/// phase boundaries is the most common bug source — keep them strict.
///
/// 1. **`register()` (free fn, outside the trait)** — synchronous, no
///    async, called once during bootstrap. Constructs the provider in
///    a placeholder/empty state and inserts it into the registry.
///
///    Allowed:
///    * Read state-level inputs (`state.run_mode()`, `state.config_dir()`)
///      to choose what to register.
///    * Read on-disk config synchronously *only* if the answer decides
///      whether to register the provider at all (e.g. feature toggles,
///      worker pinning). Use `std::fs` here — register is sync.
///
///    Forbidden:
///    * Resolving other providers from the registry (they may not exist
///      yet; ordering is settled by `boot_priority`, not by register order).
///    * Async I/O.
///    * Spawning tasks.
///    * Building the operational snapshot (that's `boot()`).
///
/// 2. **`boot()`** — async, called after every `register()` ran, in
///    `boot_priority` order. This is where the provider becomes usable.
///
///    Allowed / expected:
///    * Resolve dependencies from the registry — by now every other
///      `register()` has run.
///    * Async I/O — `tokio::fs` for config, network calls, etc. Never
///      `std::fs` (it blocks the runtime).
///    * Build the runtime snapshot and publish it via `ArcSwap` /
///      `ArcSwapOption` so concurrent readers see atomic swaps.
///    * Honor disabled-state from config: leave the snapshot empty and
///      return `Ok(())` rather than failing.
///
///    Forbidden:
///    * Spawning long-running tasks. Boot must return when state is
///      ready; the long task lives in `Runnable::run()`.
///
/// 3. **`Runnable::run()`** — see that trait. The only place for
///    long-lived loops; honors disabled-state by returning `Ok(())`
///    immediately.
///
/// 4. **`shutdown()`** — async best-effort cleanup after the shutdown
///    signal has fired and before the runtime aborts any remaining
///    runnable tasks. Use this for process-owned resources that must
///    not leak into the next graceful boot. Default no-op.
///
/// Reload (`Reloadable::reload()`) follows the same shape as `boot()`.
#[async_trait]
pub trait Provider<S>: Any + Send + Sync + 'static {
    /// Human-readable label for logs/diagnostics.
    fn name(&self) -> &'static str {
        "provider"
    }

    /// Optional boot priority. Lower values run earlier. `None` means
    /// `priority::NORMAL`. Use `priority::FIRST` for providers others
    /// depend on (e.g. `HttpService` publishing the parsed `http.yaml`),
    /// `priority::AFTER` / `priority::LATE` for consumers.
    fn boot_priority(&self) -> Option<u8> {
        None
    }

    /// Optional runtime task start priority. Lower values run earlier.
    /// `None` means `priority::NORMAL`.
    fn run_priority(&self) -> Option<u8> {
        None
    }

    /// Bootstrap-time async initialization. See the trait-level lifecycle
    /// convention for what belongs here vs in `register()` / `run()`.
    /// Default no-op so providers that only need `register()` insertion
    /// don't have to implement this.
    async fn boot(
        &self,
        _state: &S
    ) -> Result<()> {
        Ok(())
    }

    /// Graceful-shutdown cleanup hook.
    ///
    /// This is not a replacement for `Drop`: it is the lifecycle point
    /// for externally named resources whose stale presence can break the
    /// next boot, such as shm segments or lock files. Implementations
    /// should be idempotent because shutdown paths may be re-entered.
    async fn shutdown(
        &self,
        _state: &S
    ) -> Result<()> {
        Ok(())
    }

    /// Synchronous preflight validation.
    ///
    /// Call this before spawning runnable providers so bad config, missing
    /// files, or conflicting settings fail fast before long-running work
    /// starts. Applications may choose whether validation happens before or
    /// after `boot_all`, depending on whether a provider needs boot-time state
    /// to validate itself.
    fn validate(
        &self,
        _state: &S
    ) -> Result<()> {
        Ok(())
    }

    /// Downcast hook for typed resolve APIs.
    fn as_any(&self) -> &dyn Any
    where
        Self: Sized
    {
        self
    }

    /// Optional capability hook.
    fn as_reloadable(&self) -> Option<&dyn Reloadable<S>> {
        None
    }

    /// Optional capability hook.
    fn as_runnable(&self) -> Option<&dyn Runnable<S>> {
        None
    }
}

/// Type-erased provider registry used for service discovery and DI-style lookup.
/// Registration happens during bootstrap and runtime access is read-only via typed resolves.
/// We keep the underlying maps behind `RwLock<HashMap<..>>` so registration stays simple while
/// lookup only holds a short-lived read lock long enough to clone the stored `Arc`.
pub struct Registry<S> {
    providers: RwLock<HashMap<TypeId, Arc<dyn Provider<S>>>>,
    by_type: RwLock<HashMap<TypeId, Arc<dyn Any + Send + Sync>>>
}

impl<S: 'static> Registry<S> {
    /// Create the service with an empty registry. You can register later.
    pub fn new() -> Self {
        Self { providers: RwLock::new(HashMap::new()), by_type: RwLock::new(HashMap::new()) }
    }

    /// Register a provider into the registry.
    ///
    /// This accepts `Arc<T>` where `T: Provider`. The service is stored as a
    /// type-erased `Arc<dyn Provider>` but continues to point to the same underlying
    /// allocation (no new allocation is created).
    ///
    /// If another service with the same concrete type is already registered,
    /// the new registration is skipped and a warning is logged.
    ///
    /// Returns `&Self` to allow fluent chaining:
    ///
    /// ```ignore
    /// registry
    ///     .insert(dns.clone())
    ///     .insert(ipc.clone());
    /// ```
    pub fn insert<C>(
        &self,
        item: Arc<C>
    ) -> &Self
    where
        C: Provider<S> + 'static
    {
        let type_id = TypeId::of::<C>();
        let any: Arc<dyn Any + Send + Sync> = item.clone();
        let mut by_type = self.by_type.write().expect("registry by_type lock poisoned");
        if by_type.contains_key(&type_id) {
            warn!(
                "⚠️ duplicate provider type '{}' — skipping registration",
                std::any::type_name::<C>()
            );
            return self;
        }
        by_type.insert(type_id, any);
        drop(by_type);

        let it: Arc<dyn Provider<S>> = item;
        self.providers.write().expect("registry providers lock poisoned").insert(type_id, it);
        self
    }

    /// Execute a closure with a concrete typed reference `&T` if the service is registered.
    pub fn with_typed<T, R>(
        &self,
        f: impl FnOnce(&T) -> R
    ) -> Option<R>
    where
        T: Provider<S> + 'static
    {
        let typed = self.resolve::<T>()?;
        Some(f(typed.as_ref()))
    }

    /// Resolve a concrete service as an owned `Arc<T>` handle.
    ///
    /// This is the DI-style, high-level API: it returns a typed `Arc<T>` that
    /// points to the same underlying allocation as the internally registered
    /// provider (no new `Arc` allocation). The returned `Arc` is obtained by
    /// downcasting from a type-indexed map (`TypeId`).
    ///
    /// Returns `None` if the type is not registered.
    pub fn resolve<T>(&self) -> Option<Arc<T>>
    where
        T: Provider<S> + 'static
    {
        let any = self
            .by_type
            .read()
            .expect("registry by_type lock poisoned")
            .get(&TypeId::of::<T>())?
            .clone();
        Arc::downcast::<T>(any).ok()
    }

    /// Return a snapshot of registered providers.
    #[allow(unused)]
    pub fn providers(&self) -> Vec<Arc<dyn Provider<S>>> {
        self.providers.read().expect("registry providers lock poisoned").values().cloned().collect()
    }

    /// Return the list of provider display names (for diagnostics only).
    #[allow(unused)]
    pub fn list_names(&self) -> Vec<&'static str> {
        self.providers().iter().map(|c| c.name()).collect()
    }

    /// Spawn all runnable providers into the given JoinSet.
    ///
    /// Returns the number of tasks spawned.
    pub fn run_all(
        &self,
        state: S,
        join_set: &mut tokio::task::JoinSet<Result<()>>
    ) -> usize
    where
        S: Clone + Send + 'static
    {
        let mut spawned = 0usize;
        let mut providers = self.providers();
        providers.sort_by_key(|provider| {
            (provider.run_priority().unwrap_or(priority::NORMAL), provider.name())
        });

        for provider in providers {
            let Some(runnable) = provider.as_runnable() else {
                continue;
            };

            let name = provider.name();
            let fut = runnable
                .run(state.clone())
                .instrument(tracing::debug_span!("provider", provider = %name));
            join_set.spawn(async move { fut.await.map_err(|e| e.into_run(name)) });
            spawned += 1;
        }

        spawned
    }

    /// Run `validate` hook for all registered providers.
    pub fn validate_all(
        &self,
        state: &S
    ) -> Result<()> {
        for provider in self.providers() {
            let name = provider.name();
            provider.validate(state).map_err(|e| e.into_validate(name))?;
        }
        Ok(())
    }

    pub async fn boot_all(
        &self,
        state: &S
    ) -> Result<()> {
        let mut providers = self.providers();
        providers.sort_by_key(|provider| {
            (provider.boot_priority().unwrap_or(priority::NORMAL), provider.name())
        });

        for provider in providers {
            let name = provider.name();
            // debug!("🚀 booting provider '{}'", name);
            if let Err(e) = provider.boot(state).await {
                error!("❌ boot provider '{}' failed: {}", name, e);
                return Err(e.into_boot(name));
            }
            // debug!("✅ provider '{}' booted", name);
        }
        Ok(())
    }

    pub async fn shutdown_all(
        &self,
        state: &S
    ) -> Result<()> {
        let mut providers = self.providers();
        providers.sort_by_key(|provider| {
            (provider.boot_priority().unwrap_or(priority::NORMAL), provider.name())
        });
        providers.reverse();

        for provider in providers {
            let name = provider.name();
            if let Err(e) = provider.shutdown(state).await {
                warn!("shutdown of provider '{}' failed: {}", name, e);
            }
        }
        Ok(())
    }

    pub async fn reload_one(
        &self,
        name: &str,
        state: &S
    ) -> Result<()> {
        let Some(provider) = self.providers().into_iter().find(|provider| provider.name() == name)
        else {
            return Err(Error::msg(format!(
                "reload_by_name: no provider registered with name '{}'",
                name
            )));
        };

        let Some(reloadable) = provider.as_reloadable() else {
            return Err(Error::msg(format!(
                "reload_by_name: provider '{}' is not reloadable",
                name
            )));
        };

        info!("♻️  reloading service '{}'", name);

        match reloadable.reload(state).await {
            Ok(()) => {
                info!("♻️  {} reloaded", name);
                Ok(())
            }
            Err(e) => {
                warn!("❌ reload of {} failed: {e}", name);
                // Resolve the static name from the provider before consuming it.
                let static_name = provider.name();
                Err(e.into_reload(static_name))
            }
        }
    }
}

impl<S> Registry<S>
where
    S: ReloadState + 'static
{
    pub async fn reload_all(
        &self,
        state: &S
    ) -> Result<()> {
        state.reload().await?;

        info!("✅ state reloaded");

        let mut list: Vec<(u8, &'static str, Arc<dyn Provider<S>>)> = self
            .providers()
            .into_iter()
            .filter_map(|provider| {
                let reloadable = provider.as_reloadable()?;
                Some((reloadable.priority().unwrap_or(priority::NORMAL), provider.name(), provider))
            })
            .collect();

        // deterministic order: priority first, name second.
        list.sort_by_key(|(priority, name, _)| (*priority, *name));

        for (_, name, provider) in list {
            if let Some(reloadable) = provider.as_reloadable() {
                if let Err(e) = reloadable.reload(state).await {
                    warn!("❌ reload of {} failed: {e}", name);
                } else {
                    info!("♻️  {} reloaded", name);
                }
            }
        }

        Ok(())
    }
}

impl<S: 'static> Default for Registry<S> {
    fn default() -> Self {
        Self::new()
    }
}
