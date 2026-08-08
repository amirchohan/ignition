//! Minimal server entry point.
//!
//! The executable is intentionally small in Milestone 1. Runtime wiring lands
//! behind the library boundary so future clients and replay tooling can share
//! the same protocol path.

fn main() {
    println!("ignition server: protocol and architecture shell initialized");
}
