//! Multi-worker open-loop benchmark + knee finder.
//!
//! Open-loop (offered load) across W timely workers. Every worker derives the SAME logical
//! time from elapsed wall clock (after a shared init barrier — the init `step_while` forces
//! all workers to the same initial frontier, so their clocks start within sub-ms), advances
//! its input to that logical time each round, and feeds ITS shard (update index `i` with
//! `i % W == index`) of the updates that have virtually arrived. Feed-distribution was shown
//! not to matter for work placement; here it is also what lets every worker advance the
//! shared frontier without a scalar broadcast.
//!
//! Finding the knee: in open loop the achieved rate always ≈ offered (we feed all arrivals
//! by end of run), so saturation does NOT show as a throughput drop. It shows as UNBOUNDED
//! LATENCY GROWTH — the backlog builds when offered load exceeds capacity. We report the
//! latency growth ratio (2nd-half p50 / 1st-half p50): ~1 below capacity, >>1 past the knee.
//!
//! Usage:
//!   cargo run -p ddsolve --example openloop_par --release -- \
//!       <n_vars> <domain> <measure_s> <seed> <max_batch> <pool_size> <warmup_s>
//!   warmup_s is excluded from the reported window; max_batch=0 => full drain.
//!   For real capacity use a capped batch BELOW the pool, e.g. ... 12 4 60 1 200 20000 10

use ddsolve::instic;
use ddsolve::solve::search;
use ddsolve::types::{canon, Csp, Forbidden, Node, Val, VarId};
use differential_dataflow::input::Input;
use timely::dataflow::operators::Probe;
use timely::Config;

use std::collections::HashSet;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
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
    achieved: f64,
    avg_batch: f64,
    p50: f64,
    p99: f64,
    max: f64,
    growth: f64, // 2nd-half p50 / 1st-half p50
    measured: bool, // false if warmup never completed (number untrustworthy)
}

fn pct(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((p / 100.0) * (sorted.len() as f64 - 1.0)).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn run(
    csp: Csp,
    pool_pairs: Vec<Forbidden>,
    rate: f64,
    duration: f64,
    workers: usize,
    max_batch: usize,
    warmup: f64,
) -> Stats {
    let (send, recv) = mpsc::channel();
    let send = Arc::new(Mutex::new(send));

    timely::execute(Config::process(workers), move |worker| {
        let index = worker.index();
        let peers = worker.peers();
        let csp = csp.clone();
        let pool_pairs = pool_pairs.clone();
        let send = send.lock().unwrap().clone();

        let (mut root_in, mut forb_in, probe) = worker.dataflow::<u64, _, _>(|scope| {
            let (rh, roots) = scope.new_collection::<Node, isize>();
            let (fh, forbidden) = scope.new_collection::<Forbidden, isize>();
            let probe = search(&roots, &forbidden, csp.clone()).inner.probe().0;
            (rh, fh, probe)
        });

        // Untimed initial load; the step_while is a shared barrier (frontier must close on
        // all workers), so all clocks start within sub-ms of each other.
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

        // Timed open loop. The OUTER timely timestamp IS wall-clock time: microseconds of
        // elapsed real time, forced strictly increasing. All workers use the same formula
        // against a shared monotonic source -> advance together (modulo sub-ms skew).
        //
        // WARMUP: the first `warmup` seconds run the loop normally (frontier builds, the
        // expensive initial full-frontier fixpoint amortizes) but are EXCLUDED from the
        // reported numbers — at the warmup boundary we snapshot prev_target/elapsed and only
        // count drained updates and latency samples from then on. Without this the first
        // fixpoint (~seconds) dominates `achieved` and the latency percentiles for low W.
        let mut active: HashSet<Forbidden> = HashSet::new();
        let mut latencies: Vec<f64> = Vec::new(); // chronological, this worker's shard (measured window only)
        let mut prev_target: usize = 0; // global arrivals consumed so far
        let mut my_fed: usize = 0; // this worker's measured-window updates
        let mut logical: u64 = 1;
        let mut rounds = 0usize; // measured-window rounds
        let mut measuring = false;
        let mut base_target: usize = 0; // prev_target at warmup boundary
        let mut t_meas0: f64 = 0.0; // elapsed at warmup boundary

        let t0 = Instant::now();
        loop {
            let now = t0.elapsed().as_secs_f64();
            if !measuring && now >= warmup {
                measuring = true;
                base_target = prev_target;
                t_meas0 = now;
            }
            if now >= warmup + duration {
                break;
            }
            let arrived = (now * rate).floor() as usize;
            // Cap how many arrivals we drain per round: with a finite cap, a backlog builds
            // once the offered rate exceeds capacity, and latency diverges (the knee).
            let target = arrived.min(prev_target.saturating_add(max_batch));
            if target <= prev_target {
                let next_arrival = (prev_target as f64 + 1.0) / rate;
                let dt = next_arrival - now;
                if dt > 0.0 {
                    std::thread::sleep(Duration::from_secs_f64(dt.min(0.005)));
                }
                continue;
            }
            let ticks = (now * 1_000_000.0) as u64; // current wall-clock time, in microseconds
            logical = ticks.max(logical + 1);
            root_in.advance_to(logical);
            forb_in.advance_to(logical);
            // Feed ONLY this worker's shard, striding by `peers` — each producer touches
            // ~batch/peers indices instead of scanning the whole range (true multi-producer;
            // the prior full-range scan replicated O(batch) work on every worker).
            let first = prev_target + ((index + peers - (prev_target % peers)) % peers);
            let mut i = first;
            while i < target {
                let pair = pool_pairs[i % pool_pairs.len()];
                if active.remove(&pair) {
                    forb_in.remove(pair);
                } else {
                    active.insert(pair);
                    forb_in.insert(pair);
                }
                i += peers;
            }
            root_in.flush();
            forb_in.flush();
            worker.step_while(|| probe.less_than(forb_in.time()));

            let done = t0.elapsed().as_secs_f64();
            let mut j = first;
            while j < target {
                if measuring {
                    let arrival = j as f64 / rate;
                    latencies.push((done - arrival) * 1000.0); // ms
                    my_fed += 1;
                }
                j += peers;
            }
            prev_target = target;
            if measuring {
                rounds += 1;
            }
        }

        let elapsed = t0.elapsed().as_secs_f64();
        let meas_elapsed = (elapsed - t_meas0).max(1e-9);
        // Latency growth: compare first vs second half (chronological order preserved).
        let half = latencies.len() / 2;
        let mut first: Vec<f64> = latencies[..half].to_vec();
        let mut second: Vec<f64> = latencies[half..].to_vec();
        first.sort_by(|a, b| a.partial_cmp(b).unwrap());
        second.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let growth = if !first.is_empty() && pct(&first, 50.0) > 0.0 {
            pct(&second, 50.0) / pct(&first, 50.0)
        } else {
            0.0
        };
        let mut all = latencies.clone();
        all.sort_by(|a, b| a.partial_cmp(b).unwrap());

        if index == 0 {
            // Achieved over the MEASURED window only (warmup excluded): updates drained
            // between the warmup boundary and the end, over the measured wall time.
            send.send(Stats {
                achieved: (prev_target - base_target) as f64 / meas_elapsed,
                avg_batch: if rounds > 0 { my_fed as f64 / rounds as f64 } else { 0.0 },
                p50: pct(&all, 50.0),
                p99: pct(&all, 99.0),
                max: all.last().copied().unwrap_or(0.0),
                growth,
                measured: measuring && !all.is_empty(),
            })
            .unwrap();
        }
    })
    .unwrap();

    recv.recv().unwrap()
}

fn main() {
    let mut a = std::env::args().skip(1);
    let n: u16 = a.next().unwrap_or_else(|| "10".into()).parse().unwrap();
    let d: u16 = a.next().unwrap_or_else(|| "3".into()).parse().unwrap();
    let duration: f64 = a.next().unwrap_or_else(|| "1.5".into()).parse().unwrap();
    let seed: u64 = a.next().unwrap_or_else(|| "1".into()).parse().unwrap();
    // max_batch caps arrivals drained per round; 0 => unbounded (full drain).
    let max_batch_arg: usize = a.next().unwrap_or_else(|| "0".into()).parse().unwrap();
    let max_batch = if max_batch_arg == 0 { usize::MAX } else { max_batch_arg };
    // Pool of distinct candidate constraints. A LARGE pool (relative to the per-round batch)
    // means toggles rarely revisit a pair, so they do not cancel via consolidation -> each
    // update does real per-element frontier work (vs a tiny pool where batches collapse to a
    // few net diffs and the dataflow does ~no work).
    let pool_size: usize = a.next().unwrap_or_else(|| "64".into()).parse().unwrap();
    // Warmup seconds, EXCLUDED from the reported window. Lets the first full-frontier fixpoint
    // and DD arrangements amortize before measurement; without it low-W rows are pure warmup.
    let warmup: f64 = a.next().unwrap_or_else(|| "10".into()).parse().unwrap();

    let csp = instic::random_colouring(n, d, 30, seed);
    let p = pool(&csp, pool_size, seed ^ 0xABCD);

    let cap_label = if max_batch == usize::MAX { "unbounded".to_string() } else { max_batch.to_string() };
    let full_drain = max_batch == usize::MAX;
    println!("# random_colouring n_vars={n} domain={d} warmup={warmup}s measure={duration}s seed={seed} max_batch={cap_label} pool={} (asked {pool_size})", p.len());
    println!("# open loop, multi-worker, warmup-excluded. capacity = capped run where achieved < offered");
    println!("# validity: full-drain row trustworthy iff ratio≈1; capped row reports capacity iff ratio<1 (saturated)");
    println!("workers, offered_rps, achieved_rps,  ratio, avg_batch, p50_ms, p99_ms, max_ms, growth, valid");

    let rates = [50_000.0, 500_000.0, 2_000_000.0];
    for &w in &[1usize, 4, 8, 16] {
        for &rate in &rates {
            let s = run(csp.clone(), p.clone(), rate, duration, w, max_batch, warmup);
            let ratio = s.achieved / rate;
            // Trustworthy: warmup completed with samples, and either full-drain kept up
            // (ratio≈1) or capped saturated (ratio<1 => achieved is the real capacity).
            let valid = s.measured && if full_drain { ratio >= 0.95 } else { ratio < 0.95 };
            println!(
                "{w:>7}, {rate:>11.0}, {:>12.0}, {:>6.2}, {:>9.1}, {:>6.1}, {:>6.1}, {:>7.1}, {:>6.2}, {}",
                s.achieved, ratio, s.avg_batch, s.p50, s.p99, s.max, s.growth,
                if valid { "yes" } else { "NO" }
            );
        }
    }
}
