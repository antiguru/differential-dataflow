//! Incremental latency benchmark. Initialise the solver with a base instance, then feed
//! a stream of single-constraint updates (each toggles one forbidden pair on/off) and
//! record the per-update latency: wall-clock from feeding the delta until the maintained
//! solution collection catches up. Emits a latency CCDF (P[latency > x]) so the tail is
//! visible, for both the plain incremental search and the learning variant, driven by an
//! identical update stream.
//!
//! Usage:
//!   cargo run -p ddsolve --example bench --release -- <n_vars> <domain> <n_updates> <seed>

use ddsolve::instic;
use ddsolve::solve::{search, search_with_learning};
use ddsolve::types::{canon, Csp, Forbidden, Node, Val, VarId};
use differential_dataflow::input::Input;
use timely::dataflow::operators::Probe;

use std::collections::HashSet;
use std::time::Instant;

/// Deterministic xorshift64 (no rand dependency), matching the style in `instic`.
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

/// Build the update stream: a sequence of forbidden pairs drawn (with repetition) from a
/// small pool of distinct candidates NOT in the base instance. Repetition means a pair is
/// inserted on one visit and retracted on the next, keeping the constraint set fluctuating
/// around a steady state rather than monotonically piling up.
fn build_updates(csp: &Csp, n_updates: usize, seed: u64) -> Vec<Forbidden> {
    let base: HashSet<Forbidden> = csp.forbidden.iter().copied().collect();
    let mut rng = Rng::new(seed ^ 0xDEAD_BEEF);

    // Pool of distinct candidate pairs not present in the base.
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

/// Drive one variant over the update stream, returning per-update latencies in
/// microseconds. The initial load (root + all base constraints) is settled but NOT timed.
fn run_variant(csp: Csp, updates: &[Forbidden], learning: bool) -> Vec<f64> {
    let updates = updates.to_vec();
    timely::execute_directly(move |worker| {
        let (mut root_in, mut forb_in, probe) = worker.dataflow::<u32, _, _>(|scope| {
            let (rh, roots) = scope.new_collection::<Node, isize>();
            let (fh, forbidden) = scope.new_collection::<Forbidden, isize>();
            let probe = if learning {
                let (sols, _learned) = search_with_learning(&roots, &forbidden, csp.clone());
                sols.inner.probe().0
            } else {
                search(&roots, &forbidden, csp.clone()).inner.probe().0
            };
            (rh, fh, probe)
        });

        // Initial load at time 1 (untimed).
        root_in.advance_to(1);
        forb_in.advance_to(1);
        root_in.insert(Vec::new());
        for f in &csp.forbidden {
            forb_in.insert(*f);
        }
        root_in.flush();
        forb_in.flush();
        worker.step_while(|| probe.less_than(root_in.time()));

        // Timed updates: each toggles one forbidden pair.
        let mut latencies = Vec::with_capacity(updates.len());
        let mut active: HashSet<Forbidden> = HashSet::new();
        let mut t = 2u32;
        for &pair in &updates {
            let start = Instant::now();
            root_in.advance_to(t);
            forb_in.advance_to(t);
            if active.remove(&pair) {
                forb_in.remove(pair);
            } else {
                active.insert(pair);
                forb_in.insert(pair);
            }
            root_in.flush();
            forb_in.flush();
            worker.step_while(|| probe.less_than(forb_in.time()));
            latencies.push(start.elapsed().as_secs_f64() * 1e6);
            t += 1;
        }
        latencies
    })
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((p / 100.0) * (sorted.len() as f64 - 1.0)).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn report(name: &str, latencies: &[f64]) {
    let mut s = latencies.to_vec();
    s.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = s.len();
    let mean = s.iter().sum::<f64>() / n as f64;
    println!("== {name} ==  updates={n}  mean={mean:.1}us  min={:.1}us  max={:.1}us", s[0], s[n - 1]);
    for p in [50.0, 90.0, 99.0, 99.9] {
        println!("  p{p:<5} = {:.1} us", percentile(&s, p));
    }
    // Latency CCDF: P[latency > x] at log-spaced thresholds between min and max.
    println!("  CCDF  latency_us, P[X>x]");
    let lo = s[0].max(0.1);
    let hi = s[n - 1].max(lo * 1.0001);
    let buckets = 12;
    for i in 0..=buckets {
        let x = lo * (hi / lo).powf(i as f64 / buckets as f64);
        let cnt = s.iter().filter(|&&v| v > x).count();
        println!("        {x:>10.2}, {:.5}", cnt as f64 / n as f64);
    }
}

fn main() {
    let mut a = std::env::args().skip(1);
    let n: u16 = a.next().unwrap_or_else(|| "12".into()).parse().unwrap();
    let d: u16 = a.next().unwrap_or_else(|| "4".into()).parse().unwrap();
    let n_updates: usize = a.next().unwrap_or_else(|| "1000".into()).parse().unwrap();
    let seed: u64 = a.next().unwrap_or_else(|| "1".into()).parse().unwrap();

    let csp = instic::random_colouring(n, d, 30, seed);
    let updates = build_updates(&csp, n_updates, seed);

    println!("# random_colouring n_vars={n} domain={d} base_constraints={} updates={n_updates} seed={seed}",
        csp.forbidden.len());
    println!();

    let plain = run_variant(csp.clone(), &updates, false);
    report("plain incremental search", &plain);
    println!();

    let learned = run_variant(csp.clone(), &updates, true);
    report("search with learning", &learned);
}
