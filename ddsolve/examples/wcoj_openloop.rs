//! Open-loop latency CCDF for the incremental WCOJ delta join.
//!
//! Drives constraint changes (toggle a forbidden value-pair on an existing edge) at a fixed
//! *virtual* arrival rate, into the delta-join dataflow that incrementally maintains the full
//! solution set (`ddsolve::wcoj::delta_join_solutions`). Open loop: change `i` arrives at virtual
//! time `i/rate` regardless of system speed; after each batch completes we feed everything that
//! virtually arrived during processing. Latency = completion − virtual arrival, so a backlog
//! shows up as growing latency (no coordinated omission). The outer timely timestamp is wall-clock
//! microseconds. A warmup window is excluded from the reported distribution.
//!
//! Prints the latency CCDF (P[latency > x]) and writes the raw per-change latencies to a CSV.
//!
//! Usage: cargo run -p ddsolve --example wcoj_openloop --release -- \
//!            <n> <domain> <edge_pct> <seed> <rate> <measure_s> <warmup_s> [out.csv]

use ddsolve::instic;
use ddsolve::types::{Csp, Val};
use ddsolve::wcoj::delta_join_solutions;

use differential_dataflow::input::Input;

use std::collections::{BTreeMap, HashSet};
use std::time::{Duration, Instant};

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

fn graph(csp: &Csp) -> (Vec<(u16, u16)>, BTreeMap<(u16, u16), HashSet<(Val, Val)>>) {
    let mut forb: BTreeMap<(u16, u16), HashSet<(Val, Val)>> = BTreeMap::new();
    for &(a, xa, b, xb) in &csp.forbidden {
        forb.entry((a, b)).or_default().insert((xa, xb));
    }
    let edges: Vec<(u16, u16)> = forb.keys().copied().collect();
    (edges, forb)
}

fn pct(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((p / 100.0) * (sorted.len() as f64 - 1.0)).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn main() {
    let mut a = std::env::args().skip(1);
    let n: u16 = a.next().unwrap_or_else(|| "10".into()).parse().unwrap();
    let d: u16 = a.next().unwrap_or_else(|| "4".into()).parse().unwrap();
    let edge_pct: u32 = a.next().unwrap_or_else(|| "25".into()).parse().unwrap();
    let seed: u64 = a.next().unwrap_or_else(|| "1".into()).parse().unwrap();
    let rate: f64 = a.next().unwrap_or_else(|| "2000".into()).parse().unwrap();
    let measure: f64 = a.next().unwrap_or_else(|| "20".into()).parse().unwrap();
    let warmup: f64 = a.next().unwrap_or_else(|| "3".into()).parse().unwrap();
    let out_csv: Option<String> = a.next();

    let csp = instic::random_colouring(n, d, edge_pct, seed);
    let (edges, base_forb) = graph(&csp);
    let m = edges.len();
    let edge_pos: BTreeMap<(u16, u16), usize> =
        edges.iter().enumerate().map(|(i, &e)| (e, i)).collect();
    println!(
        "instance n={n} d={d} edge_pct={edge_pct} seed={seed} edges={m} | offered={rate:.0}/s warmup={warmup}s measure={measure}s"
    );
    if m == 0 {
        println!("no edges; nothing to do");
        return;
    }

    let lats = run(n, d, &edges, &edge_pos, &base_forb, rate, measure, warmup);

    let mut sorted = lats.clone();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let achieved = lats.len() as f64 / measure;
    println!(
        "measured changes={} achieved={achieved:.0}/s  (ratio {:.2})",
        lats.len(),
        achieved / rate
    );
    if sorted.is_empty() {
        println!("no measured samples (warmup too long or rate too low)");
        return;
    }

    // Latency CCDF: P[latency > x], printed as the inverse-CCDF (tail probability -> latency).
    println!("\n# latency CCDF  P[latency > x]");
    println!("{:>10}  {:>12}", "P[X>x]", "latency_ms");
    let tail_levels = [
        1.0, 0.5, 0.25, 0.1, 0.05, 0.01, 0.005, 0.001, 0.0005, 0.0001,
    ];
    for &q in &tail_levels {
        // CCDF level q corresponds to the (1-q) quantile.
        let lat = pct(&sorted, (1.0 - q) * 100.0);
        println!("{q:>10.4}  {lat:>12.3}");
    }
    println!("{:>10}  {:>12.3}  (max)", "min", sorted.last().copied().unwrap());

    // Compact ASCII CCDF: latency on x (log10 ms buckets), bar length ~ log10 of tail count.
    println!("\n# CCDF sketch (x = latency, height = log10 #samples with latency >= x)");
    ascii_ccdf(&sorted);

    if let Some(path) = out_csv {
        use std::io::Write;
        let mut f = std::fs::File::create(&path).expect("create csv");
        writeln!(f, "latency_ms").unwrap();
        for v in &lats {
            writeln!(f, "{v:.4}").unwrap();
        }
        // Also emit ready-to-plot CCDF points (sorted): latency_ms, ccdf.
        let mut g = std::fs::File::create(format!("{path}.ccdf")).expect("create ccdf");
        writeln!(g, "latency_ms,ccdf").unwrap();
        let total = sorted.len() as f64;
        for (i, v) in sorted.iter().enumerate() {
            let ccdf = 1.0 - (i as f64) / total; // P[X >= v]
            writeln!(g, "{v:.4},{ccdf:.6}").unwrap();
        }
        println!("\nwrote {} latencies to {path} (+ {path}.ccdf)", lats.len());
    }
}

/// A small log-x / log-y ASCII rendering of the CCDF.
fn ascii_ccdf(sorted: &[f64]) {
    let n = sorted.len();
    let min = sorted.first().copied().unwrap().max(1e-3);
    let max = sorted.last().copied().unwrap().max(min * 1.0001);
    let lmin = min.log10();
    let lmax = max.log10();
    let cols = 50usize;
    for r in 0..12 {
        // y axis: tail probability 10^{-r/2} roughly; print latency at that tail.
        let ccdf_level = 10f64.powf(-(r as f64) / 3.0);
        let lat = pct_ccdf(sorted, ccdf_level);
        // bar position by latency on log-x
        let frac = if lmax > lmin {
            ((lat.max(min).log10() - lmin) / (lmax - lmin)).clamp(0.0, 1.0)
        } else {
            0.0
        };
        let pos = (frac * cols as f64) as usize;
        let mut line = vec![b' '; cols + 1];
        line[pos.min(cols)] = b'*';
        println!(
            "P>{:>8.4} |{}  {:.2}ms",
            ccdf_level,
            String::from_utf8(line).unwrap(),
            lat
        );
    }
    println!(
        "           +{}",
        "-".repeat(cols + 1)
    );
    println!(
        "            {:.2}ms{}{:.2}ms   (n={})",
        min,
        " ".repeat(cols.saturating_sub(12)),
        max,
        n
    );
}

/// Latency at a given CCDF level q = P[X >= lat].
fn pct_ccdf(sorted: &[f64], q: f64) -> f64 {
    pct(sorted, (1.0 - q.clamp(0.0, 1.0)) * 100.0)
}

/// Open-loop driver: returns the per-change latencies (ms) in the measured window.
fn run(
    n: u16,
    d: Val,
    edges: &[(u16, u16)],
    edge_pos: &BTreeMap<(u16, u16), usize>,
    base_forb: &BTreeMap<(u16, u16), HashSet<(Val, Val)>>,
    rate: f64,
    measure: f64,
    warmup: f64,
) -> Vec<f64> {
    let edges = edges.to_vec();
    let edge_pos = edge_pos.clone();
    let base_forb = base_forb.clone();

    timely::execute_directly(move |worker| {
        let m = edges.len();
        let nn = n as usize;

        let (mut edge_ins, probe) = worker.dataflow::<u64, _, _>(|scope| {
            let mut edge_ins = Vec::with_capacity(m);
            let mut edge_cols = Vec::with_capacity(m);
            for _ in 0..m {
                let (h, c) = scope.new_collection::<(Val, Val), isize>();
                edge_ins.push(h);
                edge_cols.push(c);
            }
            let sols = delta_join_solutions(&edge_cols, &edges, &edge_pos, nn, d);
            let probe = sols.map(|_| ()).consolidate().probe().0;
            (edge_ins, probe)
        });

        // Untimed warm-up load: allowed = full domain^2 minus base-forbidden, per edge.
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
        for h in edge_ins.iter_mut() {
            h.advance_to(1);
            h.flush();
        }
        worker.step_while(|| probe.less_than(&1u64));

        // Deterministic change generator: arrival i toggles one pair on one edge.
        let mut pick = Rng::new(0xC0FFEE ^ rate.to_bits());
        let mut next_change = move || {
            let g = (pick.next() % m as u64) as usize;
            let c = (pick.next() % d as u64) as Val;
            let cp = (pick.next() % d as u64) as Val;
            (g, c, cp)
        };

        let mut lat: Vec<f64> = Vec::new();
        let mut prev_target: usize = 0; // arrivals consumed
        let mut logical: u64 = 1;
        let mut measuring = false;
        let total = warmup + measure;

        let t0 = Instant::now();
        loop {
            let now = t0.elapsed().as_secs_f64();
            if !measuring && now >= warmup {
                measuring = true;
            }
            if now >= total {
                break;
            }
            let arrived = (now * rate).floor() as usize;
            if arrived <= prev_target {
                let dt = ((prev_target as f64 + 1.0) / rate - now).min(0.005);
                if dt > 0.0 {
                    std::thread::sleep(Duration::from_secs_f64(dt));
                }
                continue;
            }
            // Feed all arrivals [prev_target, arrived) as toggles AT the current open time
            // `logical`, then advance past it to close (and compute) them — matching the
            // working incremental drive. Inserting at an already-closed/advanced frontier would
            // never close, stalling the probe.
            for _ in prev_target..arrived {
                let (g, c, cp) = next_change();
                if allowed[g].remove(&(c, cp)) {
                    edge_ins[g].remove((c, cp));
                } else {
                    allowed[g].insert((c, cp));
                    edge_ins[g].insert((c, cp));
                }
            }
            // Outer timestamp = wall-clock micros, strictly increasing.
            let next = ((now * 1e6) as u64).max(logical + 1);
            for h in edge_ins.iter_mut() {
                h.advance_to(next);
                h.flush();
            }
            worker.step_while(|| probe.less_than(&next));
            logical = next;
            let done = t0.elapsed().as_secs_f64();
            if measuring {
                for i in prev_target..arrived {
                    let arrival = i as f64 / rate;
                    lat.push((done - arrival) * 1000.0);
                }
            }
            prev_target = arrived;
        }
        lat
    })
}
