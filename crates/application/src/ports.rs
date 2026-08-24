//! Dependency contracts used by the composition root.
//!
//! The vocabulary (port aliases and dependency-group structs) lives in
//! `rstore_application_ports::deps`, next to the traits it names, so the persistence layer can
//! offer the single assembly point without depending on this crate. Everything is re-exported
//! here so existing call sites keep their paths.

pub use rstore_application_ports::deps::*;
