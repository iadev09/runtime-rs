use tokio_util::sync::CancellationToken;

#[cfg(feature = "events")]
use crate::LifecycleBus;
#[cfg(feature = "registry")]
use crate::Registry;

/// Shared application state passed to providers and runtime tasks.
///
/// Applications keep ownership of their real state. Put config handles,
/// typed services, caches, event buses, and domain state wherever they belong;
/// this trait only says how the runtime observes process shutdown.
///
/// The recommended shape is a cheap cloneable handle around a private `Inner`.
/// This keeps state clones cheap while letting `registry_ref()` and `events()`
/// return ordinary references:
///
/// ```text
/// use std::sync::Arc;
/// use tokio_util::sync::CancellationToken;
/// use runtime_rs::{LifecycleBus, Registry, SharedState};
///
/// #[derive(Clone)]
/// pub struct AppState(Arc<Inner>);
///
/// struct Inner {
///     shutdown_token: CancellationToken,
///     registry: Registry<AppState>,
///     events: LifecycleBus,
/// }
///
/// impl SharedState for AppState {
///     fn shutdown_token(&self) -> CancellationToken {
///         self.0.shutdown_token.clone()
///     }
///
///     fn registry_ref(&self) -> &Registry<Self> {
///         &self.0.registry
///     }
///
///     fn events(&self) -> &LifecycleBus {
///         &self.0.events
///     }
/// }
/// ```
pub trait SharedState: Clone + Send + Sync + 'static {
    fn shutdown_token(&self) -> CancellationToken;

    fn initiate_shutdown(&self) {
        self.shutdown_token().cancel();
    }

    fn is_shutting_down(&self) -> bool {
        self.shutdown_token().is_cancelled()
    }

    #[cfg(feature = "registry")]
    fn registry_ref(&self) -> &Registry<Self>;

    #[cfg(feature = "events")]
    fn events(&self) -> &LifecycleBus;
}
