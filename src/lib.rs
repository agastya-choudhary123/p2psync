//! p2psync — peer-to-peer file synchronization with streaming CRDT sync.
//!
//! The binary is a thin wrapper over these modules; they're public so the
//! integration tests can exercise the CRDT, the change detector, and the
//! binary delta transfer directly.

pub mod auth;
pub mod binary;
pub mod crdt;
pub mod diff;
pub mod discovery;
pub mod engine;
pub mod ignore;
pub mod net;
pub mod watcher;
pub mod wire;
