#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::unreachable))]

use std::{
    alloc::{GlobalAlloc, Layout, System},
    sync::atomic::{AtomicU64, Ordering},
};

use ignition::runtime::mode::benchmark::{AllocationProbe, AllocationSnapshot};

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;
static ALLOC_CALLS: AtomicU64 = AtomicU64::new(0);
static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);

struct CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        // SAFETY: this wrapper delegates allocation to the standard system allocator.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: `ptr` and `layout` were provided by the global allocator contract.
        unsafe { System.dealloc(ptr, layout) }
    }
}

fn main() {
    let probe = AllocationProbe {
        reset: reset_allocations,
        read: read_allocations,
    };
    if let Err(error) = ignition::runtime::mode::benchmark::run(std::env::args().skip(1), probe) {
        eprintln!("benchmark failed: {error}");
        std::process::exit(1);
    }
}

fn reset_allocations() {
    ALLOC_CALLS.store(0, Ordering::Relaxed);
    ALLOC_BYTES.store(0, Ordering::Relaxed);
}

fn read_allocations() -> AllocationSnapshot {
    AllocationSnapshot {
        calls: ALLOC_CALLS.load(Ordering::Relaxed),
        bytes: ALLOC_BYTES.load(Ordering::Relaxed),
    }
}
