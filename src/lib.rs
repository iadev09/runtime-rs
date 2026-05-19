//! # runtime-rs
//!
//! Agnostic service composition primitives for multi-service Rust applications.
//!
//! No application state, no configuration, no HTTP, no TLS, no DB dependency.
//! Pull this crate into any Rust project with multiple services; bring your
//! own state type, implement [`state::SharedState`] and optionally
//! [`registry::ReloadState`] on it, then use [`registry::Registry`] with the
//! [`registry::Provider`] / [`registry::Reloadable`] /
//! [`registry::Runnable`] traits.
//!
//! The error model (`registry::Error`, `registry::BoxError`, `registry::Result`)
//! lives *inside* the [`registry`] module because it is a registry concern —
//! helper primitives such as [`gate`] have their own semantics and do not
//! share this error type.
#[cfg(feature = "events")]
pub mod events;
#[cfg(feature = "support")]
pub mod gate;
#[cfg(feature = "support")]
pub mod guard;
pub mod registry;
mod runtime;
pub mod state;

#[cfg(feature = "events")]
pub use events::{LifecycleBus, LifecycleEvent, ShutdownInitiated};
#[cfg(feature = "support")]
pub use gate::{Gate, Permit};
#[cfg(feature = "support")]
pub use guard::{Guard, GuardGroup};
pub use registry::{
    BoxError, Error, Provider, Registry, ReloadState, Reloadable, Result, Runnable
};
pub use runtime::Runtime;
pub use state::SharedState;
