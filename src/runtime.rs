use std::marker::PhantomData;

use tokio::task::JoinSet;
use tracing::{debug, error};

use crate::registry::Registry;
use crate::state::SharedState;

/// Runtime task manager for runnable providers.
///
/// This keeps JoinSet orchestration out of bootstrap and ensures shutdown
/// is observed immediately via the shared shutdown token.
pub struct Runtime<S> {
    join_set: JoinSet<crate::registry::Result<()>>,
    _state: PhantomData<fn() -> S>,
}

impl<S> Default for Runtime<S> {
    fn default() -> Self {
        Self { join_set: JoinSet::new(), _state: PhantomData }
    }
}

impl<S> Runtime<S>
where
    S: Clone + Send + 'static,
{
    /// Spawn all runnable providers from registry.
    pub fn spawn_all(
        &mut self,
        registry: &Registry<S>,
        state: S,
    ) -> usize {
        registry.run_all(state, &mut self.join_set)
    }
}

impl<S> Runtime<S>
where
    S: SharedState,
{
    /// Run until shutdown is initiated or a critical runnable failure occurs.
    ///
    /// Returns:
    /// - `Ok(())` when the shutdown token is cancelled or all runnables finished.
    /// - `Err(_)` on critical startup/join failures.
    pub async fn wait_until_shutdown(
        &mut self,
        state: &S,
    ) -> crate::registry::Result<()> {
        let shutdown = state.shutdown_token();

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    debug!("runtime observed shutdown signal; leaving runnable wait loop");
                    return Ok(());
                }
                res = self.join_set.join_next() => {
                    let Some(res) = res else {
                        // No runnable tasks left.
                        debug!("runtime join set is empty");
                        return Ok(());
                    };

                    match res {
                        Ok(Ok(())) => {
                            debug!("a runnable finished cleanly");
                        }
                        // Recoverable failures (best-effort tasks)
                        // dependency shouldn't tear the worker down): the
                        // runnable opted in by returning
                        // `Error::run_continue(...)`. Log and keep serving.
                        Ok(Err(crate::registry::Error::Recoverable { name, source })) => {
                            error!(provider = %name, "runnable failed (worker continuing): {}", source);
                        }
                        // Default policy is fatal: bring the worker down so
                        // the supervisor can respawn deterministically. Any
                        // runnable that wants log+continue must explicitly
                        // opt in via `Error::run_continue`.
                        Ok(Err(e)) => {
                            error!("a runnable failed: {}", e);
                            return Err(e);
                        }
                        Err(join_err) => {
                            return Err(join_err.into());
                        }
                    }
                }
            }
        }
    }
}

impl<S> Runtime<S> {
    /// Abort and drain all remaining runnable tasks.
    pub async fn abort_and_drain(&mut self) {
        self.join_set.abort_all();
        while self.join_set.join_next().await.is_some() {}
        debug!("runtime aborted and drained remaining runnable tasks");
    }

    /// Wait for all remaining runnable tasks to finish on their own.
    ///
    /// Shutdown-aware listeners do their protocol-level graceful drain inside
    /// their runnable future. The bootstrap layer must give those futures a
    /// chance to complete before falling back to `abort_and_drain`.
    pub async fn drain(&mut self) -> crate::registry::Result<()> {
        while let Some(res) = self.join_set.join_next().await {
            match res {
                Ok(Ok(())) => {
                    debug!("a runnable finished cleanly during drain");
                }
                Ok(Err(crate::registry::Error::Recoverable { name, source })) => {
                    error!(provider = %name, "runnable failed during drain (continuing): {}", source);
                }
                Ok(Err(e)) => {
                    error!("a runnable failed during drain: {}", e);
                    return Err(e);
                }
                Err(join_err) => {
                    return Err(join_err.into());
                }
            }
        }
        debug!("runtime drained remaining runnable tasks");
        Ok(())
    }
}
