//! Head-to-head reaction benchmark exporter.
//!
//! Builds a CSP instance + a deterministic stream of constraint changes (each toggles one
//! forbidden value-pair on/off), measures ddsolve's INCREMENTAL per-change reaction latency
//! (time for the maintained full solution set to re-stabilise after one change), and writes
//! the instance + change stream + ddsolve latencies to JSON. A companion Python script reads
//! the SAME instance and measures OR-Tools CP-SAT's reaction (re-solve from scratch per
//! change), so both react to an identical workload.
//!
//! Single worker on purpose: CP-SAT solves on one search (no model-level parallel reaction),
//! so W=1 is the apples-to-apples comparison. ddsolve's parallel speedup (~4x to W=4) is a
//! separate upside, measured in openloop_par.
//!
//! Usage:
//!   cargo run -p ddsolve --example compare --release -- \
//!       <n_vars> <domain> <edge_pct> <seed> <n_changes> <pool_size> <out.json>

use ddsolve::instic;
use ddsolve::solve::search;
use ddsolve::types::{canon, Csp, Forbidden, Node, Val, VarId};
use differential_dataflow::input::Input;
use timely::dataflow::operators::Probe;

use std::collections::HashSet;
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

/// Distinct candidate forbidden pairs not already in the base instance.
fn pool(csp: &Csp, count: usize, seed: u64) -> Vec<Forbidden> {
    let base: HashSet<Forbidden> = csp.forbidden.iter().copied().collect();
    let mut rng = Rng::new(seed);
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    let mut guard = 0;
    while out.len() < count && guard < count * 1000 {
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

/// ddsolve incremental reaction: build + warm the maintained solution set once, then apply
/// each change as a single +/- diff and time the step-to-fixpoint. Returns per-change µs.
fn ddsolve_react(csp: &Csp, changes: &[Forbidden]) -> Vec<f64> {
    let csp = csp.clone();
    let changes = changes.to_vec();
    timely::execute_directly(move |worker| {
        let (mut root_in, mut forb_in, probe) = worker.dataflow::<u64, _, _>(|scope| {
            let (rh, roots) = scope.new_collection::<Node, isize>();
            let (fh, forbidden) = scope.new_collection::<Forbidden, isize>();
            let probe = search(&roots, &forbidden, csp.clone()).inner.probe().0;
            (rh, fh, probe)
        });

        // Untimed warm-up: build the initial maintained solution set.
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
        let mut lat = Vec::with_capacity(changes.len());
        let mut t = 2u64;
        for &pair in &changes {
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
            let s = Instant::now();
            worker.step_while(|| probe.less_than(forb_in.time()));
            lat.push(s.elapsed().as_secs_f64() * 1e6); // µs
            t += 1;
        }
        lat
    })
}

fn write_json(path: &str, csp: &Csp, changes: &[Forbidden], ddsolve_us: &[f64]) {
    let pairs = |v: &[Forbidden]| {
        let items: Vec<String> = v
            .iter()
            .map(|(a, xa, b, xb)| format!("[{a},{xa},{b},{xb}]"))
            .collect();
        format!("[{}]", items.join(","))
    };
    let us: Vec<String> = ddsolve_us.iter().map(|x| format!("{x:.3}")).collect();
    let json = format!(
        "{{\"n\":{},\"d\":{},\"base\":{},\"changes\":{},\"ddsolve_us\":[{}]}}",
        csp.n_vars,
        csp.domain,
        pairs(&csp.forbidden),
        pairs(changes),
        us.join(",")
    );
    std::fs::write(path, json).expect("write json");
}

fn pct(xs: &[f64], p: f64) -> f64 {
    if xs.is_empty() {
        return 0.0;
    }
    let mut s = xs.to_vec();
    s.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let idx = ((p / 100.0) * (s.len() as f64 - 1.0)).round() as usize;
    s[idx.min(s.len() - 1)]
}

fn main() {
    let mut a = std::env::args().skip(1);
    let n: u16 = a.next().unwrap_or_else(|| "10".into()).parse().unwrap();
    let d: u16 = a.next().unwrap_or_else(|| "3".into()).parse().unwrap();
    let edge_pct: u32 = a.next().unwrap_or_else(|| "30".into()).parse().unwrap();
    let seed: u64 = a.next().unwrap_or_else(|| "1".into()).parse().unwrap();
    let n_changes: usize = a.next().unwrap_or_else(|| "200".into()).parse().unwrap();
    let pool_size: usize = a.next().unwrap_or_else(|| "40".into()).parse().unwrap();
    let out = a.next().unwrap_or_else(|| "/tmp/ddcmp.json".into());

    let csp = instic::random_colouring(n, d, edge_pct, seed);
    let p = pool(&csp, pool_size, seed ^ 0xABCD);
    assert!(!p.is_empty(), "empty candidate pool");
    // Deterministic change stream: pool is smaller than n_changes, so pairs are revisited =>
    // a mix of additions and retractions (the symmetric workload ddsolve targets).
    let changes: Vec<Forbidden> = (0..n_changes).map(|i| p[i % p.len()]).collect();

    let ddsolve_us = ddsolve_react(&csp, &changes);
    write_json(&out, &csp, &changes, &ddsolve_us);

    eprintln!(
        "ddsolve n={n} d={d} base={} changes={n_changes} pool={} | reaction p50={:.1}us p99={:.1}us -> {out}",
        csp.forbidden.len(),
        p.len(),
        pct(&ddsolve_us, 50.0),
        pct(&ddsolve_us, 99.0),
    );
}
