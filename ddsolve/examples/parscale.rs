//! Parallel speedup test. Runs the incremental update benchmark (single-constraint
//! toggles against a maintained solution collection) across 1/2/4/8 timely workers and
//! reports throughput and speedup. Every worker advances the dataflow frontier each
//! round; only worker 0 feeds the data and records latency (sent back via a channel).
//!
//! Usage:
//!   cargo run -p ddsolve --example parscale --release -- <n_vars> <domain> <n_updates> <seed>

use ddsolve::instic;
use ddsolve::solve::search;
use ddsolve::types::{canon, Csp, Forbidden, Node, Val, VarId};
use differential_dataflow::input::Input;
use timely::dataflow::operators::Probe;
use timely::Config;

use std::collections::HashSet;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::Instant;

struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed.wrapping_add(0x9E3779B97F4A7C15).max(1))
    }
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

fn build_updates(csp: &Csp, n_updates: usize, seed: u64) -> Vec<Forbidden> {
    let base: HashSet<Forbidden> = csp.forbidden.iter().copied().collect();
    let mut rng = Rng::new(seed ^ 0xDEAD_BEEF);
    let pool_target = (n_updates / 8).max(8);
    let mut pool: Vec<Forbidden> = Vec::new();
    let mut seen: HashSet<Forbidden> = HashSet::new();
    let mut guard = 0;
    while pool.len() < pool_target && guard < pool_target * 200 {
        guard += 1;
        let a = (rng.next() % csp.n_vars as u64) as VarId;
        let b = (rng.next() % csp.n_vars as u64) as VarId;
        if a == b {
            continue;
        }
        let xa = (rng.next() % csp.domain as u64) as Val;
        let xb = (rng.next() % csp.domain as u64) as Val;
        let pair = canon(a, xa, b, xb);
        if base.contains(&pair) || seen.contains(&pair) {
            continue;
        }
        seen.insert(pair);
        pool.push(pair);
    }
    (0..n_updates)
        .map(|_| pool[(rng.next() as usize) % pool.len()])
        .collect()
}

/// Run the update stream across `workers` timely workers. Returns worker 0's per-update
/// latencies in microseconds (the initial load is settled but not timed).
fn run_scale(csp: Csp, updates: Vec<Forbidden>, workers: usize) -> Vec<f64> {
    let (send, recv) = mpsc::channel();
    let send = Arc::new(Mutex::new(send));

    timely::execute(Config::process(workers), move |worker| {
        let index = worker.index();
        let csp = csp.clone();
        let updates = updates.clone();
        let send = send.lock().unwrap().clone();

        let (mut root_in, mut forb_in, probe) = worker.dataflow::<u32, _, _>(|scope| {
            let (rh, roots) = scope.new_collection::<Node, isize>();
            let (fh, forbidden) = scope.new_collection::<Forbidden, isize>();
            let probe = search(&roots, &forbidden, csp.clone()).inner.probe().0;
            (rh, fh, probe)
        });

        // Initial load at time 1 (only worker 0 feeds; all workers advance the frontier).
        root_in.advance_to(1);
        forb_in.advance_to(1);
        if index == 0 {
            root_in.insert(Vec::new());
            for f in &csp.forbidden {
                forb_in.insert(*f);
            }
        }
        root_in.flush();
        forb_in.flush();
        worker.step_while(|| probe.less_than(root_in.time()));

        // Timed update stream.
        let mut active: HashSet<Forbidden> = HashSet::new();
        let mut latencies = Vec::new();
        let mut t = 2u32;
        for &pair in &updates {
            let start = Instant::now();
            root_in.advance_to(t);
            forb_in.advance_to(t);
            if index == 0 {
                if active.remove(&pair) {
                    forb_in.remove(pair);
                } else {
                    active.insert(pair);
                    forb_in.insert(pair);
                }
            }
            root_in.flush();
            forb_in.flush();
            worker.step_while(|| probe.less_than(forb_in.time()));
            if index == 0 {
                latencies.push(start.elapsed().as_secs_f64() * 1e6);
            }
            t += 1;
        }

        if index == 0 {
            send.send(latencies).unwrap();
        }
    })
    .unwrap();

    recv.recv().unwrap()
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    let idx = ((p / 100.0) * (sorted.len() as f64 - 1.0)).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn main() {
    let mut a = std::env::args().skip(1);
    let n: u16 = a.next().unwrap_or_else(|| "12".into()).parse().unwrap();
    let d: u16 = a.next().unwrap_or_else(|| "4".into()).parse().unwrap();
    let n_updates: usize = a.next().unwrap_or_else(|| "200".into()).parse().unwrap();
    let seed: u64 = a.next().unwrap_or_else(|| "1".into()).parse().unwrap();

    let csp = instic::random_colouring(n, d, 30, seed);
    let updates = build_updates(&csp, n_updates, seed);

    println!("# random_colouring n_vars={n} domain={d} base_constraints={} updates={n_updates} seed={seed}",
        csp.forbidden.len());
    println!("workers, total_ms, throughput_upd_per_s, p50_us, p99_us, speedup");

    let worker_counts = [1usize, 2, 4, 8];
    let mut baseline_total: Option<f64> = None;
    for &w in &worker_counts {
        let lat = run_scale(csp.clone(), updates.clone(), w);
        let total_us: f64 = lat.iter().sum();
        let total_ms = total_us / 1000.0;
        let throughput = lat.len() as f64 / (total_us / 1e6);
        let mut s = lat.clone();
        s.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let p50 = percentile(&s, 50.0);
        let p99 = percentile(&s, 99.0);
        let base = *baseline_total.get_or_insert(total_us);
        let speedup = base / total_us;
        println!("{w:>7}, {total_ms:>8.1}, {throughput:>18.0}, {p50:>6.1}, {p99:>7.1}, {speedup:>5.2}x");
    }
}
