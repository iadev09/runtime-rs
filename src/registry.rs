use std::any::{Any, TypeId};
use std::collections::{HashMap, HashSet};
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
        source: BoxError,
    },
    Validate {
        name: &'static str,
        source: BoxError,
    },
    Reload {
        name: &'static str,
        source: BoxError,
    },
    /// Fatal runnable failure (default). The runtime tears the worker
    /// down so the supervisor can respawn cleanly.
    Run {
        name: &'static str,
        source: BoxError,
    },
    /// Recoverable runnable failure. The runtime logs and keeps the
    /// worker serving — used for best-effort tasks (e.g. notify
    /// listeners, optional integrations) where a transient or
    /// configuration-driven failure shouldn't kill traffic.
    Recoverable {
        name: &'static str,
        source: BoxError,
    },
    Other(BoxError),
}

impl std::fmt::Display for Error {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
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
            Error::Other(e) => std::fmt::Display::fmt(e, f),
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
    E: std::error::Error + Send + Sync + 'static,
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
                f: &mut std::fmt::Formatter<'_>,
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
        name: &'static str,
    ) -> Self {
        match self {
            Error::Other(source) => Error::Boot { name, source },
            other => other,
        }
    }
    fn into_validate(
        self,
        name: &'static str,
    ) -> Self {
        match self {
            Error::Other(source) => Error::Validate { name, source },
            other => other,
        }
    }
    /// Used by `reload_one` (targeted, fail-fast). `reload_all` is broadcast
    /// and intentionally fail-soft — a single provider's failure should not
    /// cancel the rest, so that path just logs a warning.
    fn into_reload(
        self,
        name: &'static str,
    ) -> Self {
        match self {
            Error::Other(source) => Error::Reload { name, source },
            other => other,
        }
    }
    fn into_run(
        self,
        name: &'static str,
    ) -> Self {
        match self {
            Error::Other(source) => Error::Run { name, source },
            // Runnables that opt into recoverable failure construct
            // `Recoverable` with an empty `name`; `run_all` fills in the
            // provider name here so log lines stay attributed.
            Error::Recoverable { name: "", source } => Error::Recoverable { name, source },
            other => other,
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
                f: &mut std::fmt::Formatter<'_>,
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
/// Lower values run earlier among providers that are otherwise ready.
/// Prefer `ProviderOrder` for real dependencies; priorities are only
/// coarse tie-breakers for legacy/simple cases.
pub mod priority {
    /// Reserved floor for workspace-internal root providers.
    ///
    /// Ordinary providers should use `EARLY`, `NORMAL`, `LATE`, or explicit
    /// `ProviderOrder` edges instead of depending on this extreme value.
    #[doc(hidden)]
    pub const FIRST: u8 = 0;
    pub const EARLY: u8 = 50;
    pub const NORMAL: u8 = 100;
    pub const LATE: u8 = 150;
    /// Reserved ceiling for final workspace-internal lifecycle providers.
    ///
    /// Other providers should use `LATE` plus explicit `ProviderOrder` edges
    /// when they need to be late.
    #[doc(hidden)]
    pub const LAST: u8 = u8::MAX;
}

/// Type-based lifecycle ordering hints.
///
/// Numeric priorities still provide a coarse tie-breaker. `ProviderOrder`
/// adds explicit relationships between provider concrete types, so code can
/// say "run me before `T`" without relying on magic numbers or provider names.
#[derive(Clone, Debug, Default)]
pub struct ProviderOrder {
    before: Vec<TypeId>,
    after: Vec<TypeId>,
}

impl ProviderOrder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn before<T: 'static>(mut self) -> Self {
        self.before.push(TypeId::of::<T>());
        self
    }

    pub fn after<T: 'static>(mut self) -> Self {
        self.after.push(TypeId::of::<T>());
        self
    }

    pub fn before_types(&self) -> &[TypeId] {
        &self.before
    }

    pub fn after_types(&self) -> &[TypeId] {
        &self.after
    }
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
    /// Lower values run earlier among otherwise-ready providers. `None`
    /// means `priority::NORMAL`. Prefer `Provider::order()` for real
    /// dependency relationships.
    fn priority(&self) -> Option<u8> {
        None
    }

    /// Perform a synchronous reload using the current shared state.
    /// Implementations may spawn async work internally if needed.
    async fn reload(
        &self,
        state: &S,
    ) -> Result<()>;
}

/// Capability trait for providers that produce a long-running runtime task.
///
/// `run()` is the ONLY place in the lifecycle for long-running work
/// (accept loops, listeners, periodic tickers). It must NOT appear in
/// `register()` or `Provider::boot()`.
///
/// Config-driven gating: if the provider is disabled at runtime (e.g.
/// an `enabled: false` config flag, or a single-instance service whose
/// pinned `worker_id` doesn't match this worker), this method MUST
/// short-circuit and return `Ok(())` immediately instead of starting the
/// long task. The provider stays registered for downstream capability
/// lookups; it just doesn't run on this process.
#[async_trait]
pub trait Runnable<S>: Send + Sync + 'static {
    /// Run the long-lived provider task spawned by the bootstrap/supervisor layer.
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
    async fn run(
        self: Arc<Self>,
        state: S,
    ) -> Result<()>;
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
///      yet; ordering is settled by `Provider::order()` and coarse
///      priority, not by register order).
///    * Async I/O.
///    * Spawning tasks.
///    * Building the operational snapshot (that's `boot()`).
///
/// 2. **`boot()`** — async, called after every `register()` ran, in
///    lifecycle order. This is where the provider becomes usable.
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

    /// Optional boot priority. Lower values run earlier among otherwise-ready
    /// providers. `None` means `priority::NORMAL`. Prefer `Provider::order()`
    /// for dependency relationships; priority is only a coarse tie-breaker.
    fn boot_priority(&self) -> Option<u8> {
        None
    }

    /// Optional runtime task start priority. Lower values run earlier.
    /// `None` means `priority::NORMAL`.
    fn run_priority(&self) -> Option<u8> {
        None
    }

    /// Optional type-based boot/reload ordering hints.
    ///
    /// The registry builds one ordered lifecycle plan and uses it for boot,
    /// validate, shutdown, and reload. Reload skips providers that are not
    /// `Reloadable`, but dependency relationships remain the same: reload is
    /// a boot emulation on a live process.
    fn order(&self) -> ProviderOrder {
        ProviderOrder::default()
    }

    /// Bootstrap-time async initialization. See the trait-level lifecycle
    /// convention for what belongs here vs in `register()` / `run()`.
    /// Default no-op so providers that only need `register()` insertion
    /// don't have to implement this.
    async fn boot(
        &self,
        _state: &S,
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
        _state: &S,
    ) -> Result<()> {
        Ok(())
    }

    /// Synchronous preflight validation. Runs in the config-check / startup
    /// validation phase before any `boot()` to fail fast on bad config
    /// (missing files, conflicting settings) without touching the registry.
    fn validate(
        &self,
        _state: &S,
    ) -> Result<()> {
        Ok(())
    }

    /// Downcast hook for typed resolve APIs.
    fn as_any(&self) -> &dyn Any
    where
        Self: Sized,
    {
        self
    }

    /// Optional capability hook.
    fn as_reloadable(&self) -> Option<&dyn Reloadable<S>> {
        None
    }

    /// Optional capability hook.
    fn as_runnable(self: Arc<Self>) -> Option<Arc<dyn Runnable<S>>> {
        None
    }
}

/// Type-erased provider registry used for service discovery and DI-style lookup.
/// Registration happens during bootstrap and runtime access is read-only via typed resolves.
/// We keep the underlying maps behind `RwLock<HashMap<..>>` so registration stays simple while
/// lookup only holds a short-lived read lock long enough to clone the stored `Arc`.
pub struct Registry<S> {
    providers: RwLock<HashMap<TypeId, Arc<dyn Provider<S>>>>,
    by_type: RwLock<HashMap<TypeId, Arc<dyn Any + Send + Sync>>>,
    registration_order: RwLock<Vec<TypeId>>,
    lifecycle_order: RwLock<Option<Vec<TypeId>>>,
}

impl<S: 'static> Registry<S> {
    /// Create the service with an empty registry. You can register later.
    pub fn new() -> Self {
        Self {
            providers: RwLock::new(HashMap::new()),
            by_type: RwLock::new(HashMap::new()),
            registration_order: RwLock::new(Vec::new()),
            lifecycle_order: RwLock::new(None),
        }
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
        item: Arc<C>,
    ) -> &Self
    where
        C: Provider<S> + 'static,
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
        self.registration_order.write().expect("registry order lock poisoned").push(type_id);
        *self.lifecycle_order.write().expect("registry lifecycle order lock poisoned") = None;
        self
    }

    /// Execute a closure with a concrete typed reference `&T` if the service is registered.
    pub fn with_typed<T, R>(
        &self,
        f: impl FnOnce(&T) -> R,
    ) -> Option<R>
    where
        T: Provider<S> + 'static,
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
        T: Provider<S> + 'static,
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

    fn provider_entries_snapshot(&self) -> Vec<ProviderEntry<S>> {
        let providers = self.providers.read().expect("registry providers lock poisoned");
        self.registration_order
            .read()
            .expect("registry order lock poisoned")
            .iter()
            .enumerate()
            .filter_map(|(index, type_id)| {
                providers.get(type_id).cloned().map(|provider| ProviderEntry {
                    type_id: *type_id,
                    index,
                    provider,
                })
            })
            .collect()
    }

    /// Return the cached lifecycle plan, building it once if needed.
    ///
    /// The plan is invalidated on `insert()`. Normal lifecycle phases reuse
    /// the same known list, so reload is a boot emulation over the same
    /// provider order instead of a second ordering universe.
    fn lifecycle_plan(&self) -> Result<Vec<Arc<dyn Provider<S>>>> {
        if let Some(type_ids) = self
            .lifecycle_order
            .read()
            .expect("registry lifecycle order lock poisoned")
            .as_ref()
            .cloned()
        {
            return Ok(self.providers_from_type_ids(&type_ids));
        }

        let ordered = order_provider_entries(self.provider_entries_snapshot())?;
        let type_ids = ordered.iter().map(|entry| entry.type_id).collect::<Vec<_>>();
        let providers = ordered.iter().map(|entry| entry.provider.clone()).collect::<Vec<_>>();
        #[cfg(debug_assertions)]
        tracing::debug!(
            providers = ?providers.iter().map(|provider| provider.name()).collect::<Vec<_>>(),
            "provider lifecycle order"
        );
        *self.lifecycle_order.write().expect("registry lifecycle order lock poisoned") =
            Some(type_ids);
        Ok(providers)
    }

    fn providers_from_type_ids(
        &self,
        type_ids: &[TypeId],
    ) -> Vec<Arc<dyn Provider<S>>> {
        let providers = self.providers.read().expect("registry providers lock poisoned");
        type_ids.iter().filter_map(|type_id| providers.get(type_id).cloned()).collect()
    }

    /// Return the list of provider display names (for diagnostics only).
    #[allow(unused)]
    pub fn list_names(&self) -> Vec<&'static str> {
        self.providers().iter().map(|c| c.name()).collect()
    }

    /// Return provider display names in lifecycle order.
    ///
    /// This is useful for diagnostics and startup logging before running
    /// `boot_all()`.
    pub fn lifecycle_names(&self) -> Result<Vec<&'static str>> {
        Ok(self.lifecycle_plan()?.iter().map(|provider| provider.name()).collect())
    }

    /// Spawn all runnable providers into the given JoinSet.
    ///
    /// Returns the number of tasks spawned.
    pub fn run_all(
        &self,
        state: S,
        join_set: &mut tokio::task::JoinSet<Result<()>>,
    ) -> usize
    where
        S: Clone + Send + 'static,
    {
        let mut spawned = 0usize;
        let mut providers = self.providers();
        providers.sort_by_key(|provider| {
            (provider.run_priority().unwrap_or(priority::NORMAL), provider.name())
        });

        for provider in providers {
            let Some(runnable) = provider.clone().as_runnable() else { continue };

            let name = provider.name();
            let state = state.clone();
            join_set.spawn(
                async move { runnable.run(state).await.map_err(|e| e.into_run(name)) }
                    .instrument(tracing::debug_span!("provider", provider = %name)),
            );
            spawned += 1;
        }

        spawned
    }

    /// Run `validate` hook for all registered providers.
    pub fn validate_all(
        &self,
        state: &S,
    ) -> Result<()> {
        for provider in self.lifecycle_plan()? {
            let name = provider.name();
            provider.validate(state).map_err(|e| e.into_validate(name))?;
        }
        Ok(())
    }

    pub async fn boot_all(
        &self,
        state: &S,
    ) -> Result<()> {
        for provider in self.lifecycle_plan()? {
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
        state: &S,
    ) -> Result<()> {
        let mut providers = self.lifecycle_plan()?;
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
        state: &S,
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
    S: ReloadState + 'static,
{
    pub async fn reload_all(
        &self,
        state: &S,
    ) -> Result<()> {
        state.reload().await?;

        info!("✅ state reloaded");

        for provider in self.lifecycle_plan()? {
            let name = provider.name();
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

struct ProviderEntry<S> {
    type_id: TypeId,
    index: usize,
    provider: Arc<dyn Provider<S>>,
}

impl<S> Clone for ProviderEntry<S> {
    fn clone(&self) -> Self {
        Self { type_id: self.type_id, index: self.index, provider: self.provider.clone() }
    }
}

fn order_provider_entries<S: 'static>(
    entries: Vec<ProviderEntry<S>>
) -> Result<Vec<ProviderEntry<S>>> {
    let len = entries.len();
    let positions: HashMap<TypeId, usize> =
        entries.iter().enumerate().map(|(idx, entry)| (entry.type_id, idx)).collect();
    let priorities: Vec<u8> =
        entries.iter().map(|entry| lifecycle_priority(&entry.provider)).collect();
    let mut outgoing: Vec<HashSet<usize>> = (0..len).map(|_| HashSet::new()).collect();
    let mut indegree = vec![0usize; len];

    let mut add_edge = |from: usize, to: usize| {
        if from != to && outgoing[from].insert(to) {
            indegree[to] += 1;
        }
    };

    for (idx, entry) in entries.iter().enumerate() {
        let order = entry.provider.order();
        for target in order.before_types() {
            if let Some(&target_idx) = positions.get(target) {
                add_edge(idx, target_idx);
            }
        }
        for target in order.after_types() {
            if let Some(&target_idx) = positions.get(target) {
                add_edge(target_idx, idx);
            }
        }
    }

    let mut ready: Vec<usize> = indegree
        .iter()
        .enumerate()
        .filter_map(|(idx, degree)| (*degree == 0).then_some(idx))
        .collect();
    let mut ordered = Vec::with_capacity(len);

    while !ready.is_empty() {
        ready.sort_by_key(|idx| {
            (priorities[*idx], entries[*idx].index, entries[*idx].provider.name())
        });
        let idx = ready.remove(0);
        ordered.push(idx);

        let next: Vec<_> = outgoing[idx].iter().copied().collect();
        for target in next {
            indegree[target] -= 1;
            if indegree[target] == 0 {
                ready.push(target);
            }
        }
    }

    if ordered.len() != len {
        let blocked = indegree
            .iter()
            .enumerate()
            .filter_map(|(idx, degree)| (*degree > 0).then_some(entries[idx].provider.name()))
            .collect::<Vec<_>>()
            .join(", ");
        return Err(Error::msg(format!("provider lifecycle order cycle detected: {blocked}")));
    }

    Ok(ordered.into_iter().map(|idx| entries[idx].clone()).collect())
}

fn lifecycle_priority<S: 'static>(provider: &Arc<dyn Provider<S>>) -> u8 {
    provider
        .boot_priority()
        .or_else(|| provider.as_reloadable().and_then(|reloadable| reloadable.priority()))
        .unwrap_or(priority::NORMAL)
}

impl<S: 'static> Default for Registry<S> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    #[derive(Clone, Default)]
    struct TestState {
        seen: Arc<Mutex<Vec<&'static str>>>,
    }

    struct DbProvider;
    struct CacheProvider;
    struct ApiProvider;

    #[async_trait]
    impl Provider<TestState> for DbProvider {
        fn name(&self) -> &'static str {
            "db"
        }

        fn validate(
            &self,
            state: &TestState,
        ) -> Result<()> {
            state.seen.lock().expect("test log poisoned").push("db");
            Ok(())
        }
    }

    #[async_trait]
    impl Provider<TestState> for CacheProvider {
        fn name(&self) -> &'static str {
            "cache"
        }

        fn order(&self) -> ProviderOrder {
            ProviderOrder::new().after::<DbProvider>()
        }

        fn validate(
            &self,
            state: &TestState,
        ) -> Result<()> {
            state.seen.lock().expect("test log poisoned").push("cache");
            Ok(())
        }
    }

    #[async_trait]
    impl Provider<TestState> for ApiProvider {
        fn name(&self) -> &'static str {
            "api"
        }

        fn order(&self) -> ProviderOrder {
            ProviderOrder::new().after::<CacheProvider>()
        }

        fn validate(
            &self,
            state: &TestState,
        ) -> Result<()> {
            state.seen.lock().expect("test log poisoned").push("api");
            Ok(())
        }
    }

    #[test]
    fn lifecycle_order_uses_type_dependencies() {
        let state = TestState::default();
        let registry = Registry::<TestState>::new();

        registry
            .insert(Arc::new(ApiProvider))
            .insert(Arc::new(CacheProvider))
            .insert(Arc::new(DbProvider));

        registry.validate_all(&state).expect("validation should succeed");

        let seen = state.seen.lock().expect("test log poisoned").clone();
        assert_eq!(seen, vec!["db", "cache", "api"]);
    }

    struct CycleA;
    struct CycleB;

    #[async_trait]
    impl Provider<TestState> for CycleA {
        fn name(&self) -> &'static str {
            "cycle-a"
        }

        fn order(&self) -> ProviderOrder {
            ProviderOrder::new().after::<CycleB>()
        }
    }

    #[async_trait]
    impl Provider<TestState> for CycleB {
        fn name(&self) -> &'static str {
            "cycle-b"
        }

        fn order(&self) -> ProviderOrder {
            ProviderOrder::new().after::<CycleA>()
        }
    }

    #[test]
    fn lifecycle_order_rejects_cycles() {
        let state = TestState::default();
        let registry = Registry::<TestState>::new();

        registry.insert(Arc::new(CycleA)).insert(Arc::new(CycleB));

        let err = registry.validate_all(&state).expect_err("cycle must be rejected");
        assert!(err.to_string().contains("provider lifecycle order cycle detected"));
    }
}
