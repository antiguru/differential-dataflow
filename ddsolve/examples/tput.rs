//! Throughput scaling + feed-distribution test. Applies a BATCH of constraint toggles
//! per round (not one at a time) so updates expose data parallelism, and compares two
//! feed strategies at identical total work:
//!   - "w0":  only worker 0 feeds the whole batch (the parscale strategy)
//!   - "all": each worker feeds its own shard of the batch
//! If "all" scales better than "w0", single-worker feeding was a bottleneck.
//!
//! Each worker owns a fixed pool of distinct candidate pairs; a round toggles whole pools
//! (insert if currently out, retract if in), keeping the constraint set in steady state.
//!
//! Usage:
//!   cargo run -p ddsolve --example tput --release -- <n_vars> <domain> <rounds> <batch> <seed>

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

/// Distinct candidate pairs (not in base), `count` of them, deterministic per `salt`.
fn pool(csp: &Csp, count: usize, salt: u64) -> Vec<Forbidden> {
    let base: HashSet<Forbidden> = csp.forbidden.iter().copied().collect();
    let mut rng = Rng::new(salt);
    let mut out = Vec::new();
    let mut seen: HashSet<Forbidden> = HashSet::new();
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

/// Returns wall-clock seconds for `rounds` batched rounds. `pools[w]` is worker w's pool.
/// `feed_all=false` => worker 0 feeds every pool; `feed_all=true` => worker w feeds pools[w].
fn run(csp: Csp, pools: Vec<Vec<Forbidden>>, workers: usize, rounds: u32, feed_all: bool) -> f64 {
    let (send, recv) = mpsc::channel();
    let send = Arc::new(Mutex::new(send));

    timely::execute(Config::process(workers), move |worker| {
        let index = worker.index();
        let csp = csp.clone();
        let pools = pools.clone();
        let send = send.lock().unwrap().clone();

        let (mut root_in, mut forb_in, probe) = worker.dataflow::<u32, _, _>(|scope| {
            let (rh, roots) = scope.new_collection::<Node, isize>();
            let (fh, forbidden) = scope.new_collection::<Forbidden, isize>();
            let probe = search(&roots, &forbidden, csp.clone()).inner.probe().0;
            (rh, fh, probe)
        });

        // Untimed initial load (worker 0 feeds root + base).
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

        // Which pools does this worker feed?
        let my_pools: Vec<usize> = if feed_all {
            if index < pools.len() { vec![index] } else { vec![] }
        } else if index == 0 {
            (0..pools.len()).collect()
        } else {
            vec![]
        };
        let mut inserted = vec![false; pools.len()];

        let start = Instant::now();
        let mut t = 2u32;
        for _ in 0..rounds {
            root_in.advance_to(t);
            forb_in.advance_to(t);
            for &pi in &my_pools {
                if inserted[pi] {
                    for &p in &pools[pi] {
                        forb_in.remove(p);
                    }
                    inserted[pi] = false;
                } else {
                    for &p in &pools[pi] {
                        forb_in.insert(p);
                    }
                    inserted[pi] = true;
                }
            }
            root_in.flush();
            forb_in.flush();
            worker.step_while(|| probe.less_than(forb_in.time()));
            t += 1;
        }
        let secs = start.elapsed().as_secs_f64();
        if index == 0 {
            send.send(secs).unwrap();
        }
    })
    .unwrap();

    recv.recv().unwrap()
}

fn main() {
    let mut a = std::env::args().skip(1);
    let n: u16 = a.next().unwrap_or_else(|| "12".into()).parse().unwrap();
    let d: u16 = a.next().unwrap_or_else(|| "4".into()).parse().unwrap();
    let rounds: u32 = a.next().unwrap_or_else(|| "40".into()).parse().unwrap();
    let batch: usize = a.next().unwrap_or_else(|| "16".into()).parse().unwrap();
    let seed: u64 = a.next().unwrap_or_else(|| "1".into()).parse().unwrap();

    let csp = instic::random_colouring(n, d, 30, seed);
    let worker_counts = [1usize, 2, 4, 8];

    println!("# random_colouring n_vars={n} domain={d} rounds={rounds} batch_per_worker={batch} seed={seed}");
    println!("# each round applies W*batch toggles; total work scales with W (weak scaling)");
    println!("workers, feed, updates, time_s, throughput_upd_per_s");

    for &w in &worker_counts {
        // One distinct pool per worker (worker0-mode feeds all of them too).
        let pools: Vec<Vec<Forbidden>> =
            (0..w).map(|i| pool(&csp, batch, seed ^ (0x100 + i as u64))).collect();
        let total = rounds as usize * w * batch;
        for (label, feed_all) in [("w0", false), ("all", true)] {
            let secs = run(csp.clone(), pools.clone(), w, rounds, feed_all);
            let tput = total as f64 / secs;
            println!("{w:>7}, {label:>4}, {total:>7}, {secs:>6.2}, {tput:>20.0}");
        }
    }
}
