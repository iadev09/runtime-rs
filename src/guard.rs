use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::Notify;

/// Small RAII in-flight counter.
///
/// [`GuardGroup::guard`] increments the counter and returns a [`Guard`].
/// Dropping the guard decrements the counter. [`GuardGroup::wait_empty`]
/// resolves when the counter reaches zero.
#[derive(Clone, Debug)]
pub struct GuardGroup(Arc<Inner>);

#[derive(Debug)]
struct Inner {
    count: AtomicUsize,
    notify: Notify,
}

impl GuardGroup {
    pub fn new() -> Self {
        Self(Arc::new(Inner { count: AtomicUsize::new(0), notify: Notify::new() }))
    }

    pub fn guard(&self) -> Guard {
        self.0.count.fetch_add(1, Ordering::Relaxed);
        Guard(self.0.clone())
    }

    pub fn count(&self) -> usize {
        self.0.count.load(Ordering::Acquire)
    }

    pub async fn wait_empty(&self) {
        loop {
            if self.count() == 0 {
                return;
            }
            self.0.notify.notified().await;
        }
    }
}

impl Default for GuardGroup {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub struct Guard(Arc<Inner>);

impl Drop for Guard {
    fn drop(&mut self) {
        if self.0.count.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.0.notify.notify_waiters();
        }
    }
}
