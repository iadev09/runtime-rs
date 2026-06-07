//! Graceful shutdown gate primitive.
//!
//! The design is inspired by axum's server handle pattern
//! (<https://github.com/tokio-rs/axum>): a sticky shutdown signal, tracked
//! in-flight work, and a graceful drain phase before forced shutdown. This
//! module is not tied to HTTP; it lifts that operational shape into a small
//! runtime primitive that any service loop can use.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::{Notify, watch};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};

/// Sentinel value for AtomicU64 representing None (infinite grace period)
const NANOS_NONE: u64 = u64::MAX;

#[derive(Clone, Debug)]
pub struct Gate {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    graceful: NotifyOnce,
    shutdown: NotifyOnce,
    released: Notify,
    count: AtomicUsize,
    all_done: NotifyOnce,
    /// Grace period for shutdown: NANOS_NONE = None (infinite), otherwise nanoseconds
    grace_period_nanos: AtomicU64,
    /// Timeout for acquiring connection slot when at max_connections (during normal operation)
    acquire_timeout: Duration,
    max_count: Option<usize>,
}

#[derive(Debug)]
pub enum Error {
    ShuttingDown, // GracefulShutdown(Duration),
    AcquireTimeout(Duration),
    AtCapacity,
}

impl Gate {
    /// Create a new gate with optional max connections and acquire timeout.
    ///
    /// - `max_count`: Maximum concurrent connections (None = unlimited)
    /// - `acquire_timeout`: How long to wait for a slot when at capacity
    pub fn new(
        max_count: Option<usize>,
        acquire_timeout: Duration,
    ) -> Self {
        let inner = Inner {
            graceful: NotifyOnce::default(),
            shutdown: NotifyOnce::default(),
            released: Notify::new(),
            count: AtomicUsize::new(0),
            all_done: NotifyOnce::default(),
            grace_period_nanos: AtomicU64::new(NANOS_NONE),
            acquire_timeout,
            max_count,
        };
        Self { inner: Arc::new(inner) }
    }

    fn permit(&self) -> Permit {
        Permit::new(self.clone())
    }

    /// Returns the current grace period duration (if any).
    /// Lock-free read using atomic operation.
    pub fn grace_period(&self) -> Option<Duration> {
        match self.inner.grace_period_nanos.load(Ordering::Acquire) {
            NANOS_NONE => None,
            nanos => Some(Duration::from_nanos(nanos)),
        }
    }

    /// Get the number of connections.
    pub fn count(&self) -> usize {
        self.inner.count.load(Ordering::SeqCst)
    }

    /// Trigger a **forced** (hard) shutdown. Wakes all waiters of `wait_shutdown()`.
    /// This is only called by `wait_all_done()` when the grace period elapses.
    /// External code should not call this directly.
    pub fn force_shutdown(&self) {
        self.inner.shutdown.notify_waiters();
    }

    /// Begin a graceful shutdown phase. Notifies `graceful` and records the optional grace period.
    /// This does **not** imply a forced/Hard shutdown signal.
    ///
    /// `None` means indefinite grace period (critical tasks mode).
    /// Lock-free write using atomic operation.
    pub fn graceful_shutdown(
        &self,
        duration: Option<Duration>,
    ) {
        let nanos = duration.map_or(NANOS_NONE, |d| d.as_nanos() as u64);
        self.inner.grace_period_nanos.store(nanos, Ordering::Release);

        self.inner.graceful.notify_waiters();
    }

    pub fn is_shutting_down(&self) -> bool {
        self.inner.graceful.is_notified()
    }

    /// Wait for the **forced** (hard) shutdown signal. This does *not* fire for a clean drain.
    /// Private to this module; external code should use `Permit::wait_forced_shutdown`.
    pub async fn wait_forced_shutdown(&self) {
        self.inner.shutdown.notified().await;
    }

    pub async fn wait_graceful_shutdown(&self) {
        self.inner.graceful.notified().await;
    }

    /// Enter the gate, waiting up to the configured acquire timeout if the
    /// gate is currently at capacity.
    ///
    /// Returns a [`Permit`] token. Dropping the permit leaves the gate.
    pub async fn enter(&self) -> Result<Permit, Error> {
        let wait_timeout = self.inner.acquire_timeout;
        let start = tokio::time::Instant::now();

        loop {
            if self.inner.graceful.is_notified() {
                return Err(Error::ShuttingDown);
            }

            let count = self.inner.count.load(Ordering::SeqCst);

            if let Some(max_count) = self.inner.max_count {
                if count < max_count {
                    return Ok(self.permit());
                }
                // Log when at capacity
                if count == max_count {
                    debug!("Connection limit reached: {}/{} connections in use", count, max_count);
                }
            } else {
                return Ok(self.permit());
            }

            // Calculate remaining timeout
            let elapsed = start.elapsed();
            if elapsed >= wait_timeout {
                warn!(
                    "Connection acquire timeout after {:?}. Current: {}/{}",
                    wait_timeout,
                    count,
                    self.inner.max_count.unwrap_or(usize::MAX)
                );
                return Err(Error::AcquireTimeout(wait_timeout));
            }

            let remaining = wait_timeout - elapsed;

            // Wait until a connection is freed, but let shutdown interrupt
            // overload backpressure immediately.
            tokio::select! {
                biased;

                _ = self.inner.graceful.notified() => {
                    return Err(Error::ShuttingDown);
                }
                _ = self.inner.released.notified() => {
                    // A connection was released, loop again to try acquiring.
                    continue;
                }
                _ = sleep(remaining) => {
                    warn!(
                        "Connection acquire timeout after {:?}. Current: {}/{}",
                        wait_timeout,
                        count,
                        self.inner.max_count.unwrap_or(usize::MAX)
                    );
                    return Err(Error::AcquireTimeout(wait_timeout));
                }
            }
        }
    }

    /// Try to enter the gate immediately without waiting for capacity.
    pub fn try_enter(&self) -> Result<Permit, Error> {
        if self.inner.graceful.is_notified() {
            return Err(Error::ShuttingDown);
        }

        // Hard limit check — sync snapshot only
        if let Some(max) = self.inner.max_count {
            // fetch_add with compare is the only safe pattern
            let prev = self.inner.count.fetch_add(1, Ordering::Relaxed);
            if prev >= max {
                self.inner.count.fetch_sub(1, Ordering::Relaxed);
                return Err(Error::AtCapacity);
            }
        } else {
            self.inner.count.fetch_add(1, Ordering::Relaxed);
        }

        // The slot was already counted above; construct the permit directly.
        Ok(Permit { gate: self.clone() })
    }

    /// Wait until all permits are dropped, respecting the configured grace period.
    /// If the grace period elapses, this triggers `force_shutdown()` and returns immediately.
    /// Note: when returning via the forced path, `count()` may still be > 0 for a short time
    /// until connection tasks observe the hard signal and drop.
    pub async fn wait_all_done(&self) {
        if self.inner.count.load(Ordering::SeqCst) == 0 {
            return;
        }

        // Lock-free read of grace period
        let deadline = self.grace_period();

        match deadline {
            Some(duration) => tokio::select! {
                biased;
                _ = sleep(duration) => {
                    error!("⛔ Graceful timeout exceeded after {:?}; forcing shutdown", duration);
                    self.force_shutdown();
                },
                _ = self.inner.all_done.notified() => {
                    debug!("🍺 All connections finished before graceful timeout");
                },
            },
            None => self.inner.all_done.notified().await,
        }
    }
}

pub struct Permit {
    gate: Gate,
}

#[allow(unused)]
impl Permit {
    fn new(gate: Gate) -> Self {
        gate.inner.count.fetch_add(1, Ordering::SeqCst);

        Self { gate }
    }

    pub async fn wait_graceful_shutdown(&self) {
        self.gate.wait_graceful_shutdown().await
    }

    pub async fn wait_forced_shutdown(&self) {
        self.gate.wait_forced_shutdown().await
    }

    pub fn is_shutting_down(&self) -> bool {
        self.gate.is_shutting_down()
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        let count = self.gate.inner.count.fetch_sub(1, Ordering::SeqCst) - 1;

        if count == 0 && self.gate.inner.graceful.is_notified() {
            self.gate.inner.all_done.notify_waiters();
        }

        // permit isn't dropped yet.
        if let Some(max_count) = self.gate.inner.max_count {
            if count < max_count {
                // Notify waiters that a slot is available
                self.gate.inner.released.notify_waiters();
            }
        }
    }
}

/// Create a gate that automatically initiates graceful shutdown when the token is cancelled.
///
/// # Parameters
/// - `token`: Cancellation token that triggers shutdown
/// - `graceful_timeout`: Grace period for shutdown (None = infinite, for critical tasks)
/// - `max_count`: Maximum concurrent connections (None = unlimited)
/// - `acquire_timeout`: Timeout for acquiring a connection slot
pub fn create_gate(
    token: CancellationToken,
    graceful_timeout: Option<Duration>,
    max_count: Option<usize>,
    acquire_timeout: Duration,
) -> Gate {
    let gate = Gate::new(max_count, acquire_timeout);
    let shutdown_gate = gate.clone();
    tokio::spawn(async move {
        token.cancelled().await;
        shutdown_gate.graceful_shutdown(graceful_timeout);
    });
    gate
}

#[inline]
pub fn default_acquire_timeout() -> Duration {
    Duration::from_millis(100)
}

#[derive(Debug)]
struct NotifyOnce {
    tx: watch::Sender<bool>,
    rx: watch::Receiver<bool>,
}

impl Default for NotifyOnce {
    fn default() -> Self {
        let (tx, rx) = watch::channel(false);
        Self { tx, rx }
    }
}

impl NotifyOnce {
    fn notify_waiters(&self) {
        self.tx.send_replace(true);
    }

    fn is_notified(&self) -> bool {
        *self.rx.borrow()
    }

    async fn notified(&self) {
        let mut rx = self.rx.clone();

        loop {
            if *rx.borrow_and_update() {
                return;
            }

            if rx.changed().await.is_err() {
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Error, Gate};
    use std::time::Duration;

    #[test]
    fn try_enter_counts_one_slot_per_permit() {
        let gate = Gate::new(Some(2), Duration::from_millis(10));

        let first = gate.try_enter().unwrap();
        assert_eq!(gate.count(), 1);

        let second = gate.try_enter().unwrap();
        assert_eq!(gate.count(), 2);

        assert!(matches!(gate.try_enter(), Err(Error::AtCapacity)));

        drop(first);
        assert_eq!(gate.count(), 1);

        drop(second);
        assert_eq!(gate.count(), 0);
    }
}
