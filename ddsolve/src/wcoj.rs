//! Worst-case-optimal *delta join* for the CSP all-solutions task.
//!
//! The all-solutions set is the natural join of the per-edge allowed relations
//! `R_jk(x_j,x_k)`. This builds it as a proper delta query (McSherry / dogsdogsdogs `AltNeu`):
//! one delta rule per edge, driven by that edge's updates, joined against the other edges'
//! relations — edges earlier in the order read at ALT (old) time, later at NEU (new) time, so
//! each output change is produced exactly once. Each rule is a count-propose-validate generic
//! join, so every update is processed worst-case-optimally and work tracks the output delta,
//! not the whole search frontier.
//!
//! Specialised to a `u64` outer timestamp (the only timestamp the drivers use) to keep the
//! trait bounds concrete.

use std::collections::BTreeMap;

use differential_dataflow::VecCollection;

use differential_dogs3::altneu::AltNeu;
use differential_dogs3::{CollectionIndex, PrefixExtender, ProposeExtensionMethod};

use crate::types::Val;

/// Sentinel for an unbound variable in a partial assignment prefix.
const SENT: Val = Val::MAX;
/// A (partial) assignment: `prefix[v]` is the value of variable `v`, or `SENT` if unbound.
type Pre = Vec<Val>;

/// Incrementally maintain all CSP solutions as a delta join over the edge relations.
///
/// `edge_cols[g]` is the *allowed* value-pair relation `R_{edges[g]}` (each a collection of
/// `(x_j, x_k)` pairs); feeding it deltas (toggling allowed pairs) maintains the output
/// incrementally. `edge_pos` maps each edge `(j,k)` (with `j<k`) to its index in `edges`.
/// Returns the collection of complete assignments (`Vec<Val>` of length `n`) = all solutions.
pub fn delta_join_solutions<'a>(
    edge_cols: &[VecCollection<'a, u64, (Val, Val), isize>],
    edges: &[(u16, u16)],
    edge_pos: &BTreeMap<(u16, u16), usize>,
    n: usize,
    d: Val,
) -> VecCollection<'a, u64, Pre, isize> {
    let m = edges.len();
    let edge_cols: Vec<VecCollection<'a, u64, (Val, Val), isize>> = edge_cols.to_vec();
    let edges = edges.to_vec();
    let edge_pos = edge_pos.clone();

    let outer = edge_cols[0].scope();
    let outer_for_leave = outer.clone();

    outer.scoped::<AltNeu<u64>, _, _>("delta_join", move |inner| {
        // Per edge: ALT/NEU × forward/reverse indices, plus the ALT driver collection.
        let mut alt_fwd = Vec::with_capacity(m);
        let mut alt_rev = Vec::with_capacity(m);
        let mut neu_fwd = Vec::with_capacity(m);
        let mut neu_rev = Vec::with_capacity(m);
        let mut drivers = Vec::with_capacity(m);
        for c in &edge_cols {
            let fwd = c.clone().enter(inner);
            let rev = c.clone().enter(inner).map(|(x, y)| (y, x));
            let nfwd = c.clone().enter(inner).delay(|t| AltNeu::neu(t.time.clone()));
            let nrev = c
                .clone()
                .enter(inner)
                .map(|(x, y)| (y, x))
                .delay(|t| AltNeu::neu(t.time.clone()));
            alt_fwd.push(CollectionIndex::index(fwd));
            alt_rev.push(CollectionIndex::index(rev));
            neu_fwd.push(CollectionIndex::index(nfwd));
            neu_rev.push(CollectionIndex::index(nrev));
            drivers.push(c.clone().enter(inner));
        }

        // Accumulate the per-rule contributions (the total delta-query output).
        let mut total: Option<VecCollection<_, Pre, isize>> = None;
        for i in 0..m {
            let (a, b) = (edges[i].0 as usize, edges[i].1 as usize);

            // Seed: the driver edge's pairs as partial assignments binding a and b.
            let mut prefix: VecCollection<_, Pre, isize> = drivers[i].clone().map(move |(xa, xb)| {
                let mut p = vec![SENT; n];
                p[a] = xa;
                p[b] = xb;
                p
            });
            let mut bound = vec![false; n];
            bound[a] = true;
            bound[b] = true;

            // Bind remaining variables in index order via generic join.
            for w in 0..n {
                if bound[w] {
                    continue;
                }
                let mut refs: Vec<
                    &mut dyn PrefixExtender<'_, AltNeu<u64>, isize, Prefix = Pre, Extension = Val>,
                > = Vec::new();
                for u in 0..n {
                    if !bound[u] || u == w {
                        continue;
                    }
                    let e = (w.min(u) as u16, w.max(u) as u16);
                    if let Some(&g) = edge_pos.get(&e) {
                        // Earlier edges read OLD (alt), later edges NEW (neu): the delta-query
                        // diagonal that produces each output change exactly once.
                        let use_alt = g < i;
                        // Key by the bound endpoint u; propose the value of the new var w.
                        let ext: Box<
                            dyn PrefixExtender<'_, AltNeu<u64>, isize, Prefix = Pre, Extension = Val>,
                        > = if u < w {
                            // u == e.0 -> forward index keyed by e.0
                            let idx = if use_alt { &alt_fwd[g] } else { &neu_fwd[g] };
                            Box::new(idx.extend_using(move |p: &Pre| p[u]))
                        } else {
                            // u == e.1 -> reverse index keyed by e.1
                            let idx = if use_alt { &alt_rev[g] } else { &neu_rev[g] };
                            Box::new(idx.extend_using(move |p: &Pre| p[u]))
                        };
                        // Leaked to 'static: extenders own clones of the relation traces, are only
                        // consulted while wiring the operators, and live for the dataflow's life.
                        refs.push(Box::leak(ext));
                    }
                }

                prefix = if refs.is_empty() {
                    // Variable not yet constrained by a bound neighbour: cross the full domain;
                    // edges to it are validated when a later-bound endpoint is processed.
                    prefix.flat_map(move |p| {
                        (0..d).map(move |c| {
                            let mut q = p.clone();
                            q[w] = c;
                            q
                        })
                    })
                } else {
                    prefix.extend(&mut refs).map(move |(mut p, vw)| {
                        p[w] = vw;
                        p
                    })
                };
                bound[w] = true;
            }

            total = Some(match total {
                None => prefix,
                Some(t) => t.concat(prefix),
            });
        }

        total.unwrap().leave(outer_for_leave)
    })
}
