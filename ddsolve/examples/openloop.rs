//! Open-loop (offered-load) latency/throughput benchmark.
//!
//! The closed-loop benchmarks (bench/parscale/tput) feed a fixed batch, block until it
//! completes, then feed the next — so offered load is coupled to latency and throughput is
//! pinned at 1/latency (coordinated omission). This benchmark instead fixes a *virtual*
//! arrival schedule: update `i` arrives at wall time `i / rate`, independent of how fast
//! the system runs. After each batch completes we generate everything that virtually
//! arrived during processing and feed it as the next (dynamically sized) batch. Latency is
//! measured from each update's virtual arrival to its completion, so a backlog shows up as
//! growing latency. Dynamic batching amortizes per-round scheduling overhead and exercises
//! pipelining, revealing true capacity: achieved throughput tracks the offered rate until
//! capacity, then plateaus while latency diverges.
//!
//! Single worker (execute_directly): multi-worker open loop needs cross-worker consensus on
//! logical time, a separate concern; feed-distribution was already shown not to matter.
//!
//! Usage:
//!   cargo run -p ddsolve --example openloop --release -- <n_vars> <domain> <duration_s> <seed>

use ddsolve::instic;
use ddsolve::solve::search;
use ddsolve::types::{canon, Csp, Forbidden, Node, Val, VarId};
use differential_dataflow::input::Input;
use timely::dataflow::operators::Probe;

use std::collections::HashSet;
use std::time::{Duration, Instant};

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

/// A pool of distinct candidate pairs not in the base; the update stream cycles it
/// (`stream[i] = pool[i % len]`), so each pair is toggled on/off on successive visits.
fn pool(csp: &Csp, count: usize, seed: u64) -> Vec<Forbidden> {
    let base: HashSet<Forbidden> = csp.forbidden.iter().copied().collect();
    let mut rng = Rng::new(seed);
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    let mut guard = 0;
    while out.len() < count && guard < count * 500 {
        guard += 1;
        let a = (rng.next() % csp.n_vars as u64) as VarId;
        let b = (rng.next() % csp.n_vars as u64) as VarId;
        if a == b {
            continue;
        }
        let xa = (rng.next() % csp.domain as u64) as Val;
        let xb = (rng.next() % csp.domain as u64) as Val;
        let p = canon(a, xa, b, xb);
        if base.contains(&p) || seen.contains(&p) {
            continue;
        }
        seen.insert(p);
        out.push(p);
    }
    out
}

struct Stats {
    offered: f64,
    achieved: f64,
    fed: usize,
    rounds: usize,
    p50: f64,
    p99: f64,
    max: f64,
}

/// Run the open loop at a fixed offered `rate` (updates/sec) for `duration` seconds.
fn run_rate(csp: Csp, pool: Vec<Forbidden>, rate: f64, duration: f64) -> Stats {
    timely::execute_directly(move |worker| {
        let (mut root_in, mut forb_in, probe) = worker.dataflow::<u64, _, _>(|scope| {
            let (rh, roots) = scope.new_collection::<Node, isize>();
            let (fh, forbidden) = scope.new_collection::<Forbidden, isize>();
            let probe = search(&roots, &forbidden, csp.clone()).inner.probe().0;
            (rh, fh, probe)
        });

        // Untimed initial load.
        root_in.advance_to(1);
        forb_in.advance_to(1);
        root_in.insert(Vec::new());
        for f in &csp.forbidden {
            forb_in.insert(*f);
        }
        root_in.flush();
        forb_in.flush();
        worker.step_while(|| probe.less_than(root_in.time()));

        let mut active: HashSet<Forbidden> = HashSet::new();
        let mut latencies: Vec<f64> = Vec::new();
        let mut next_idx: usize = 0; // next update index to feed
        let mut logical: u64 = 1; // outer timestamp = wall-clock micros (strictly increasing)
        let mut rounds = 0usize;

        let t0 = Instant::now();
        loop {
            let now = t0.elapsed().as_secs_f64();
            if now >= duration {
                break;
            }
            // How many updates have virtually arrived by now?
            let target = (now * rate).floor() as usize;
            if target <= next_idx {
                // Caught up — sleep until the next scheduled arrival.
                let next_arrival = (next_idx as f64 + 1.0) / rate;
                let dt = next_arrival - now;
                if dt > 0.0 {
                    std::thread::sleep(Duration::from_secs_f64(dt.min(0.01)));
                }
                continue;
            }
            // Feed the backlog [next_idx, target) as one batch stamped with the current
            // wall-clock time (the outer timely timestamp IS real time, in microseconds).
            logical = ((now * 1_000_000.0) as u64).max(logical + 1);
            forb_in.advance_to(logical);
            root_in.advance_to(logical);
            for i in next_idx..target {
                let pair = pool[i % pool.len()];
                if active.remove(&pair) {
                    forb_in.remove(pair);
                } else {
                    active.insert(pair);
                    forb_in.insert(pair);
                }
            }
            forb_in.flush();
            root_in.flush();
            worker.step_while(|| probe.less_than(forb_in.time()));

            let done = t0.elapsed().as_secs_f64();
            for i in next_idx..target {
                let arrival = i as f64 / rate;
                latencies.push((done - arrival) * 1000.0); // ms
            }
            next_idx = target;
            rounds += 1;
        }

        let elapsed = t0.elapsed().as_secs_f64();
        latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let pct = |p: f64| -> f64 {
            if latencies.is_empty() {
                0.0
            } else {
                let idx = ((p / 100.0) * (latencies.len() as f64 - 1.0)).round() as usize;
                latencies[idx.min(latencies.len() - 1)]
            }
        };
        Stats {
            offered: rate,
            achieved: next_idx as f64 / elapsed,
            fed: next_idx,
            rounds,
            p50: pct(50.0),
            p99: pct(99.0),
            max: latencies.last().copied().unwrap_or(0.0),
        }
    })
}

fn main() {
    let mut a = std::env::args().skip(1);
    let n: u16 = a.next().unwrap_or_else(|| "10".into()).parse().unwrap();
    let d: u16 = a.next().unwrap_or_else(|| "3".into()).parse().unwrap();
    let duration: f64 = a.next().unwrap_or_else(|| "2".into()).parse().unwrap();
    let seed: u64 = a.next().unwrap_or_else(|| "1".into()).parse().unwrap();

    let csp = instic::random_colouring(n, d, 30, seed);
    let p = pool(&csp, 64, seed ^ 0xABCD);

    println!("# random_colouring n_vars={n} domain={d} duration={duration}s seed={seed}");
    println!("# open loop: virtual arrival rate fixed; batch = backlog accumulated during last round");
    println!("offered_rps, achieved_rps, fed, rounds, avg_batch, p50_ms, p99_ms, max_ms");

    for &rate in &[100.0, 500.0, 1000.0, 2000.0, 5000.0, 10000.0, 20000.0, 50000.0] {
        let s = run_rate(csp.clone(), p.clone(), rate, duration);
        let avg_batch = if s.rounds > 0 { s.fed as f64 / s.rounds as f64 } else { 0.0 };
        println!(
            "{:>11.0}, {:>12.0}, {:>6}, {:>6}, {:>9.1}, {:>6.2}, {:>6.2}, {:>7.2}",
            s.offered, s.achieved, s.fed, s.rounds, avg_batch, s.p50, s.p99, s.max
        );
    }
}
