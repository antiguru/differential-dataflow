//! Memory-scaling measurement. The prototype materialises the entire live search
//! frontier (every node at every depth), so peak heap is the defining cost. A counting
//! global allocator tracks peak bytes; we sweep problem size (number of variables) and
//! report peak memory, frontier node count, and bytes per node.
//!
//! Usage:
//!   cargo run -p ddsolve --example memscale --release -- <domain> <edge_pct> <seed> <max_vars>

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use ddsolve::instic;
use ddsolve::metrics::count_by_depth;
use ddsolve::solve::search_live;
use ddsolve::types::Forbidden;
use differential_dataflow::AsCollection;
use timely::dataflow::operators::ToStream;

/// System allocator wrapper that tracks current and peak live bytes.
struct Counting;
static CURRENT: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        let p = System.alloc(l);
        if !p.is_null() {
            let cur = CURRENT.fetch_add(l.size(), Ordering::Relaxed) + l.size();
            PEAK.fetch_max(cur, Ordering::Relaxed);
        }
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        CURRENT.fetch_sub(l.size(), Ordering::Relaxed);
        System.dealloc(p, l);
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let p = System.realloc(ptr, layout, new_size);
        if !p.is_null() {
            if new_size >= layout.size() {
                let cur = CURRENT.fetch_add(new_size - layout.size(), Ordering::Relaxed)
                    + (new_size - layout.size());
                PEAK.fetch_max(cur, Ordering::Relaxed);
            } else {
                CURRENT.fetch_sub(layout.size() - new_size, Ordering::Relaxed);
            }
        }
        p
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

/// Run `search_live` to completion for an instance, returning
/// (peak_bytes_during_run, frontier_node_count, solution_count).
fn measure(n_vars: u16, domain: u16, edge_pct: u32, seed: u64) -> (usize, isize, isize) {
    let csp = instic::random_colouring(n_vars, domain, edge_pct, seed);
    let fv = csp.forbidden.clone();

    // Reset the peak to the current live bytes so we attribute only this run's high-water.
    let baseline = CURRENT.load(Ordering::Relaxed);
    PEAK.store(baseline, Ordering::Relaxed);

    let counts = timely::execute_directly(move |worker| {
        worker.dataflow::<u64, _, _>(|scope| {
            let roots = vec![(Vec::<u16>::new(), 0u64, 1isize)]
                .into_iter().to_stream(scope).as_collection();
            let forbidden: Vec<(Forbidden, u64, isize)> =
                fv.iter().map(|f| (*f, 0u64, 1isize)).collect();
            let forbidden = forbidden.into_iter().to_stream(scope).as_collection();
            count_by_depth(&search_live(&roots, &forbidden, csp.clone()))
        })
    });

    let peak = PEAK.load(Ordering::Relaxed).saturating_sub(baseline);
    let map = counts.lock().unwrap();
    let frontier: isize = map.values().copied().sum();
    let solutions = map.get(&(n_vars as usize)).copied().unwrap_or(0);
    (peak, frontier, solutions)
}

fn main() {
    let mut a = std::env::args().skip(1);
    let domain: u16 = a.next().unwrap_or_else(|| "3".into()).parse().unwrap();
    let edge_pct: u32 = a.next().unwrap_or_else(|| "30".into()).parse().unwrap();
    let seed: u64 = a.next().unwrap_or_else(|| "1".into()).parse().unwrap();
    let max_vars: u16 = a.next().unwrap_or_else(|| "13".into()).parse().unwrap();

    println!("# random_colouring domain={domain} edge_pct={edge_pct} seed={seed}");
    println!("n_vars, frontier_nodes, solutions, peak_KB, bytes_per_node");
    for n in (6..=max_vars).step_by(1) {
        let (peak, frontier, solutions) = measure(n, domain, edge_pct, seed);
        let bpn = if frontier > 0 { peak as f64 / frontier as f64 } else { 0.0 };
        println!(
            "{n:>6}, {frontier:>14}, {solutions:>9}, {:>8.1}, {bpn:>8.1}",
            peak as f64 / 1024.0
        );
    }
}
