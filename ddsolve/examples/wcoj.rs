//! Worst-case-optimal-join formulation of the CSP all-solutions task.
//!
//! The set of all solutions is the natural join of the per-edge *allowed* relations
//! `R_jk(x_j, x_k)` over the constraint graph. The reified-tree `search` evaluates this with a
//! binary-join plan that materialises every partial assignment (the frontier) — the
//! intermediate blow-up. Here we instead bind variables in order and extend each `x_k` against
//! all edge-relations `(j,k), j<k` via dogsdogsdogs' count-propose-validate (generic/leapfrog
//! join), whose work is bounded by the AGM output bound — it never materialises dead prefixes.
//!
//! This isolates the thesis: on over-constrained instances (few solutions, big search tree)
//! WCOJ should track the small output while `search` pays for the huge frontier. On
//! many-solution instances both are output-bound (the answer itself is exponential).
//!
//! Usage: cargo run -p ddsolve --example wcoj --release -- <n> <domain> <edge_pct> <seed>

use ddsolve::instic;
use ddsolve::solve::search;
use ddsolve::types::{Csp, Forbidden, Node, Val};

use differential_dataflow::input::Input;
use differential_dataflow::VecCollection;

use std::sync::{Arc, Mutex};

use differential_dogs3::{CollectionIndex, PrefixExtender, ProposeExtensionMethod};

type Pre = Vec<Val>;

/// For every constrained variable-pair (j,k) with j<k, the *allowed* value pairs
/// (full domain^2 minus the forbidden pairs for that pair). Unconstrained pairs are omitted
/// (they impose nothing — no extender needed).
fn allowed_edges(csp: &Csp) -> Vec<(u16, u16, Vec<(Val, Val)>)> {
    use std::collections::{BTreeMap, BTreeSet};
    let mut forb: BTreeMap<(u16, u16), BTreeSet<(Val, Val)>> = BTreeMap::new();
    for &(a, xa, b, xb) in &csp.forbidden {
        forb.entry((a, b)).or_default().insert((xa, xb));
    }
    let d = csp.domain;
    forb.into_iter()
        .map(|((j, k), bad)| {
            let mut allow = Vec::new();
            for c in 0..d {
                for cp in 0..d {
                    if !bad.contains(&(c, cp)) {
                        allow.push((c, cp));
                    }
                }
            }
            (j, k, allow)
        })
        .collect()
}

/// Count all solutions via the WCOJ join. Returns (count, micros to fixpoint).
fn solve_wcoj_count(csp: &Csp) -> (i64, f64) {
    let csp = csp.clone();
    let edges = allowed_edges(&csp);
    let acc = Arc::new(Mutex::new(0i64));
    let acc2 = acc.clone();

    let elapsed = timely::execute_directly(move |worker| {
        let acc = acc2.clone();
        let n = csp.n_vars;
        let d = csp.domain;
        let edges = edges.clone();

        let (mut dom_in, mut edge_ins, probe) = worker.dataflow::<u64, _, _>(|scope| {
            // Domain relation, used to seed x_0 and any isolated variable.
            let (dom_in, dom) = scope.new_collection::<Val, isize>();

            // One input collection of allowed (x_j, x_k) pairs per constrained edge, indexed.
            let mut edge_ins = Vec::new();
            let mut indices = Vec::new(); // (j, k, CollectionIndex over R_jk)
            for &(j, k, _) in &edges {
                let (h, rel) = scope.new_collection::<(Val, Val), isize>();
                edge_ins.push(h);
                indices.push((j, k, CollectionIndex::index(rel)));
            }

            // Seed: x_0 over the full domain.
            let mut prefix: VecCollection<_, Pre, isize> = dom.map(|c| vec![c]);

            // Bind x_1..x_{n-1} in order, extending against every edge (j,k) with j<k.
            for k in 1..n {
                // Extenders are leaked to 'static: they own clones of the relation traces, are
                // only consulted while building the operators, and this example runs once.
                let mut refs: Vec<&mut dyn PrefixExtender<'_, u64, isize, Prefix = Pre, Extension = Val>> =
                    Vec::new();
                for (j, kk, idx) in &indices {
                    if *kk == k {
                        let j = *j as usize;
                        let ext = Box::leak(Box::new(idx.extend_using(move |p: &Pre| p[j])));
                        refs.push(ext);
                    }
                }
                prefix = if refs.is_empty() {
                    // Isolated variable: cross with full domain.
                    prefix.flat_map(move |p| {
                        (0..d).map(move |c| {
                            let mut q = p.clone();
                            q.push(c);
                            q
                        })
                    })
                } else {
                    prefix.extend(&mut refs).map(|(mut p, v)| {
                        p.push(v);
                        p
                    })
                };
            }

            let probe = prefix
                .map(|_| ())
                .consolidate()
                .inspect(move |((), _t, diff)| {
                    *acc.lock().unwrap() += *diff as i64;
                })
                .probe().0;

            (dom_in, edge_ins, probe)
        });

        // Load static data.
        for c in 0..d {
            dom_in.insert(c);
        }
        for (h, (_, _, allow)) in edge_ins.iter_mut().zip(edges.iter()) {
            for &p in allow {
                h.insert(p);
            }
        }
        dom_in.advance_to(1);
        dom_in.flush();
        for h in edge_ins.iter_mut() {
            h.advance_to(1);
            h.flush();
        }

        let t0 = std::time::Instant::now();
        worker.step_while(|| probe.less_than(dom_in.time()));
        t0.elapsed().as_secs_f64() * 1e6
    });

    let c = *acc.lock().unwrap();
    (c, elapsed)
}

/// Count all solutions via the reified-tree search. Returns (count, micros).
fn search_count(csp: &Csp) -> (i64, f64) {
    let csp = csp.clone();
    let acc = Arc::new(Mutex::new(0i64));
    let acc2 = acc.clone();
    let elapsed = timely::execute_directly(move |worker| {
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
                .probe().0;
            (rh, fh, probe)
        });
        root_in.insert(Vec::new());
        for f in &csp.forbidden {
            forb_in.insert(*f);
        }
        root_in.advance_to(1);
        forb_in.advance_to(1);
        root_in.flush();
        forb_in.flush();
        let t0 = std::time::Instant::now();
        worker.step_while(|| probe.less_than(root_in.time()));
        t0.elapsed().as_secs_f64() * 1e6
    });
    let c = *acc.lock().unwrap();
    (c, elapsed)
}

fn main() {
    let mut a = std::env::args().skip(1);
    let n: u16 = a.next().unwrap_or_else(|| "8".into()).parse().unwrap();
    let d: u16 = a.next().unwrap_or_else(|| "3".into()).parse().unwrap();
    let edge_pct: u32 = a.next().unwrap_or_else(|| "50".into()).parse().unwrap();
    let seed: u64 = a.next().unwrap_or_else(|| "1".into()).parse().unwrap();

    let csp = instic::random_colouring(n, d, edge_pct, seed);
    let edges = allowed_edges(&csp);
    println!(
        "instance n={n} d={d} edge_pct={edge_pct} seed={seed} | constrained_pairs={} forbidden={}",
        edges.len(),
        csp.forbidden.len()
    );

    let (wc, wus) = solve_wcoj_count(&csp);
    let (sc, sus) = search_count(&csp);

    println!("WCOJ:   solutions={wc} solve={wus:.0}us");
    println!("search: solutions={sc} solve={sus:.0}us");
    assert_eq!(wc, sc, "WCOJ and search disagree on solution count");
    println!("OK (counts match). speedup search/WCOJ = {:.2}x", sus / wus.max(1e-9));
}
