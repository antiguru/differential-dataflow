//! Incremental worst-case-optimal *delta join* for the CSP all-solutions task.
//!
//! Maintains the join Q = ⨝_{(j,k)∈E} R_jk(x_j,x_k) (all solutions) under content changes to the
//! edge relations R_jk (toggling a forbidden value-pair on an existing edge). Built as a proper
//! delta query (McSherry / dogsdogsdogs `AltNeu`): one delta rule per edge, driven by that edge's
//! updates, joined against the other edges' relations — earlier edges in the order read at ALT
//! (old) time, later edges at NEU (new) time, so each output change is produced exactly once.
//! Each rule is itself a generic-join (count-propose-validate), so every update is processed
//! worst-case-optimally; work tracks the *output delta*, not the whole pipeline.
//!
//! Compared with the reified-tree `search` reaction (re-stabilise the maintained set per change)
//! and validated against a host-side brute count after every change.
//!
//! Usage: cargo run -p ddsolve --example wcoj_delta --release -- <n> <domain> <edge_pct> <seed> <changes>

use ddsolve::instic;
use ddsolve::solve::search;
use ddsolve::types::{Csp, Forbidden, Node, Val};

use differential_dataflow::input::Input;

use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex};

struct Rng(u64);
impl Rng {
    fn new(s: u64) -> Self {
        Rng(s.wrapping_add(0x9E3779B97F4A7C15).max(1))
    }
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

/// The constraint graph (sorted edges) and, per edge, the initially-forbidden value pairs.
fn graph(csp: &Csp) -> (Vec<(u16, u16)>, BTreeMap<(u16, u16), HashSet<(Val, Val)>>) {
    let mut forb: BTreeMap<(u16, u16), HashSet<(Val, Val)>> = BTreeMap::new();
    for &(a, xa, b, xb) in &csp.forbidden {
        forb.entry((a, b)).or_default().insert((xa, xb));
    }
    let edges: Vec<(u16, u16)> = forb.keys().copied().collect();
    (edges, forb)
}

/// Host-side brute solution count over the current forbidden set (validation oracle).
fn brute_count(n: u16, d: Val, forb: &BTreeMap<(u16, u16), HashSet<(Val, Val)>>) -> i64 {
    fn rec(
        var: u16,
        n: u16,
        d: Val,
        asn: &mut Vec<Val>,
        forb: &BTreeMap<(u16, u16), HashSet<(Val, Val)>>,
    ) -> i64 {
        if var == n {
            return 1;
        }
        let mut c = 0;
        'val: for x in 0..d {
            for (j, &xj) in asn.iter().enumerate() {
                if let Some(bad) = forb.get(&(j as u16, var)) {
                    if bad.contains(&(xj, x)) {
                        continue 'val;
                    }
                }
            }
            asn.push(x);
            c += rec(var + 1, n, d, asn, forb);
            asn.pop();
        }
        c
    }
    let mut asn = Vec::with_capacity(n as usize);
    rec(0, n, d, &mut asn, forb)
}

fn pct(xs: &[f64], p: f64) -> f64 {
    if xs.is_empty() {
        return 0.0;
    }
    let mut s = xs.to_vec();
    s.sort_by(|a, b| a.partial_cmp(b).unwrap());
    s[(((p / 100.0) * (s.len() as f64 - 1.0)).round() as usize).min(s.len() - 1)]
}

fn main() {
    let mut a = std::env::args().skip(1);
    let n: u16 = a.next().unwrap_or_else(|| "8".into()).parse().unwrap();
    let d: u16 = a.next().unwrap_or_else(|| "3".into()).parse().unwrap();
    let edge_pct: u32 = a.next().unwrap_or_else(|| "40".into()).parse().unwrap();
    let seed: u64 = a.next().unwrap_or_else(|| "1".into()).parse().unwrap();
    let n_changes: usize = a.next().unwrap_or_else(|| "100".into()).parse().unwrap();
    let dump_path: Option<String> = a.next();

    let csp = instic::random_colouring(n, d, edge_pct, seed);
    let (edges, base_forb) = graph(&csp);
    let m = edges.len();
    let edge_pos: BTreeMap<(u16, u16), usize> =
        edges.iter().enumerate().map(|(i, &e)| (e, i)).collect();
    println!(
        "instance n={n} d={d} edge_pct={edge_pct} seed={seed} | edges={m} changes={n_changes}"
    );
    if m == 0 {
        println!("no edges; nothing to do");
        return;
    }

    // Deterministic change stream: toggle one value-pair on one existing edge.
    let mut rng = Rng::new(seed ^ 0xDEAD);
    let changes: Vec<(usize, Val, Val)> = (0..n_changes)
        .map(|_| {
            let g = (rng.next() % m as u64) as usize;
            let c = (rng.next() % d as u64) as Val;
            let cp = (rng.next() % d as u64) as Val;
            (g, c, cp)
        })
        .collect();

    // Optional: dump instance + change stream for the CP-SAT count-per-change comparison.
    if let Some(path) = &dump_path {
        let edges_j: Vec<String> = edges.iter().map(|(j, k)| format!("[{j},{k}]")).collect();
        let base_j: Vec<String> = csp
            .forbidden
            .iter()
            .map(|(a, xa, b, xb)| format!("[{a},{xa},{b},{xb}]"))
            .collect();
        let ch_j: Vec<String> = changes
            .iter()
            .map(|(g, c, cp)| format!("[{g},{c},{cp}]"))
            .collect();
        let json = format!(
            "{{\"n\":{n},\"d\":{d},\"edges\":[{}],\"base\":[{}],\"changes\":[{}]}}",
            edges_j.join(","),
            base_j.join(","),
            ch_j.join(",")
        );
        std::fs::write(path, json).expect("write dump");
    }

    // ---- delta-join reaction ----
    let (dj_lat, dj_final) = delta_join_react(n, d, &edges, &edge_pos, &base_forb, &changes);

    // ---- reified-tree search reaction (same change stream) ----
    let (sr_lat, sr_final) = search_react(&csp, &edges, &base_forb, &changes);

    // ---- host oracle: final count ----
    let mut forb = base_forb.clone();
    for &(g, c, cp) in &changes {
        let e = edges[g];
        let set = forb.entry(e).or_default();
        if !set.remove(&(c, cp)) {
            set.insert((c, cp));
        }
    }
    let oracle = brute_count(n, d, &forb);

    println!("final solutions: delta_join={dj_final} search={sr_final} oracle={oracle}");
    assert_eq!(dj_final, oracle, "delta-join final count != oracle");
    assert_eq!(sr_final, oracle, "search final count != oracle");
    println!(
        "delta_join reaction: p50={:.1}us p99={:.1}us  |  search reaction: p50={:.1}us p99={:.1}us",
        pct(&dj_lat, 50.0),
        pct(&dj_lat, 99.0),
        pct(&sr_lat, 50.0),
        pct(&sr_lat, 99.0),
    );
    println!(
        "speedup search/delta_join: p50={:.2}x p99={:.2}x",
        pct(&sr_lat, 50.0) / pct(&dj_lat, 50.0).max(1e-9),
        pct(&sr_lat, 99.0) / pct(&dj_lat, 99.0).max(1e-9),
    );
}

/// Build + drive the delta-join dataflow. Returns (per-change reaction µs, final solution count).
fn delta_join_react(
    n: u16,
    d: Val,
    edges: &[(u16, u16)],
    edge_pos: &BTreeMap<(u16, u16), usize>,
    base_forb: &BTreeMap<(u16, u16), HashSet<(Val, Val)>>,
    changes: &[(usize, Val, Val)],
) -> (Vec<f64>, i64) {
    let edges = edges.to_vec();
    let edge_pos = edge_pos.clone();
    let base_forb = base_forb.clone();
    let changes = changes.to_vec();
    let acc = Arc::new(Mutex::new(0i64));
    let acc2 = acc.clone();

    let lat = timely::execute_directly(move |worker| {
        let acc = acc2.clone();
        let m = edges.len();
        let nn = n as usize;

        let (mut edge_ins, probe) = worker.dataflow::<u64, _, _>(|scope| {
            // One input per edge: its currently-allowed value pairs.
            let mut edge_ins = Vec::with_capacity(m);
            let mut edge_cols = Vec::with_capacity(m);
            for _ in 0..m {
                let (h, c) = scope.new_collection::<(Val, Val), isize>();
                edge_ins.push(h);
                edge_cols.push(c);
            }

            let acc = acc.clone();
            let sols = ddsolve::wcoj::delta_join_solutions(&edge_cols, &edges, &edge_pos, nn, d);

            let probe = sols
                .map(|_| ())
                .consolidate()
                .inspect(move |((), _t, diff)| {
                    *acc.lock().unwrap() += *diff as i64;
                })
                .probe()
                .0;

            (edge_ins, probe)
        });

        // Initial load: allowed = full domain^2 minus base-forbidden, per edge.
        let mut allowed: Vec<HashSet<(Val, Val)>> = Vec::with_capacity(m);
        for (g, &e) in edges.iter().enumerate() {
            let bad = base_forb.get(&e).cloned().unwrap_or_default();
            let mut set = HashSet::new();
            for c in 0..d {
                for cp in 0..d {
                    if !bad.contains(&(c, cp)) {
                        set.insert((c, cp));
                        edge_ins[g].insert((c, cp));
                    }
                }
            }
            allowed.push(set);
        }
        let mut t = 1u64;
        for h in edge_ins.iter_mut() {
            h.advance_to(t);
            h.flush();
        }
        worker.step_while(|| probe.less_than(&t));

        // Apply each change, timing the reaction (step to fixpoint).
        let mut lat = Vec::with_capacity(changes.len());
        for &(g, c, cp) in &changes {
            t += 1;
            // Toggle (c,cp) on edge g: present in `allowed` => now forbidden (remove); else add.
            if allowed[g].remove(&(c, cp)) {
                edge_ins[g].remove((c, cp));
            } else {
                allowed[g].insert((c, cp));
                edge_ins[g].insert((c, cp));
            }
            for h in edge_ins.iter_mut() {
                h.advance_to(t);
                h.flush();
            }
            let t0 = std::time::Instant::now();
            worker.step_while(|| probe.less_than(&t));
            lat.push(t0.elapsed().as_secs_f64() * 1e6);
        }
        lat
    });

    let c = *acc.lock().unwrap();
    (lat, c)
}

/// Reified-tree `search` reaction to the same change stream (single worker).
fn search_react(
    csp: &Csp,
    edges: &[(u16, u16)],
    base_forb: &BTreeMap<(u16, u16), HashSet<(Val, Val)>>,
    changes: &[(usize, Val, Val)],
) -> (Vec<f64>, i64) {
    let csp = csp.clone();
    let edges = edges.to_vec();
    let base_forb = base_forb.clone();
    let changes = changes.to_vec();
    let acc = Arc::new(Mutex::new(0i64));
    let acc2 = acc.clone();

    let lat = timely::execute_directly(move |worker| {
        let acc = acc2.clone();
        let csp = csp.clone();
        let (mut root_in, mut forb_in, probe) = worker.dataflow::<u64, _, _>(|scope| {
            let (rh, roots) = scope.new_collection::<Node, isize>();
            let (fh, forbidden) = scope.new_collection::<Forbidden, isize>();
            let probe = search(&roots, &forbidden, csp.clone())
                .map(|_| ())
                .consolidate()
                .inspect(move |((), _t, diff)| {
                    *acc.lock().unwrap() += *diff as i64;
                })
                .probe()
                .0;
            (rh, fh, probe)
        });

        root_in.insert(Vec::new());
        for f in &csp.forbidden {
            forb_in.insert(*f);
        }
        let mut t = 1u64;
        root_in.advance_to(t);
        forb_in.advance_to(t);
        root_in.flush();
        forb_in.flush();
        worker.step_while(|| probe.less_than(&t));

        // Mirror the toggles as forbidden-pair deltas (canon form, j<k since edges are sorted).
        let mut forbidden_now: HashSet<Forbidden> = csp.forbidden.iter().copied().collect();
        let mut lat = Vec::with_capacity(changes.len());
        for &(g, c, cp) in &changes {
            t += 1;
            let (j, k) = edges[g];
            let pair: Forbidden = (j, c, k, cp);
            if forbidden_now.remove(&pair) {
                forb_in.remove(pair);
            } else {
                forbidden_now.insert(pair);
                forb_in.insert(pair);
            }
            root_in.advance_to(t);
            forb_in.advance_to(t);
            root_in.flush();
            forb_in.flush();
            let t0 = std::time::Instant::now();
            worker.step_while(|| probe.less_than(&t));
            lat.push(t0.elapsed().as_secs_f64() * 1e6);
        }
        let _ = &base_forb;
        lat
    });

    let c = *acc.lock().unwrap();
    (lat, c)
}
