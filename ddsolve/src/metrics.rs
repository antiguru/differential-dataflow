//! Instrumentation: count live nodes per depth.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use differential_dataflow::VecCollection;
use differential_dataflow::lattice::Lattice;
use timely::progress::Timestamp;

use crate::types::Node;

/// Shared counter of net live nodes grouped by depth, accumulated across all batches.
/// Uses `Arc<Mutex<_>>` (not `Rc`) so it is `Send` — required to return it out of
/// `timely::execute_directly`.
pub type DepthCounts = Arc<Mutex<BTreeMap<usize, isize>>>;

/// Attach a probe that tallies net diffs per node depth into a shared map.
/// Returns the map; read it after the worker has run to completion.
pub fn count_by_depth<'a, T>(nodes: &VecCollection<'a, T, Node>) -> DepthCounts
where
    T: Timestamp + Lattice + Ord,
{
    let counts: DepthCounts = Arc::new(Mutex::new(BTreeMap::new()));
    let sink = counts.clone();
    nodes.clone().inspect_batch(move |_time, batch| {
        let mut map = sink.lock().unwrap();
        for (node, _t, r) in batch.iter() {
            *map.entry(node.len()).or_insert(0) += *r;
        }
    });
    counts
}

/// Per-round counter keyed by outer timestamp (round). Value semantics depend on the
/// recorder: net signed diff for [`net_by_round`], absolute update volume for [`churn_by_round`].
pub type RoundCounts = Arc<Mutex<BTreeMap<u32, isize>>>;

/// Record the *net* signed diff of a `u32`-rounded collection per round. Summing the
/// values up to round `r` gives the collection's net size after round `r` settles.
pub fn net_by_round<'a>(nodes: &VecCollection<'a, u32, Node>) -> RoundCounts {
    let counts: RoundCounts = Arc::new(Mutex::new(BTreeMap::new()));
    let sink = counts.clone();
    nodes.clone().inspect_batch(move |t, batch| {
        let mut m = sink.lock().unwrap();
        for (_n, _tt, r) in batch.iter() {
            *m.entry(*t).or_insert(0) += *r;
        }
    });
    counts
}

/// Record the *churn* per round: the number of update tuples (|+| + |−|) flowing on a
/// `u32`-rounded collection at each round. This is the work a constraint delta induces.
pub fn churn_by_round<'a>(nodes: &VecCollection<'a, u32, Node>) -> RoundCounts {
    let counts: RoundCounts = Arc::new(Mutex::new(BTreeMap::new()));
    let sink = counts.clone();
    nodes.clone().inspect_batch(move |t, batch| {
        let mut m = sink.lock().unwrap();
        *m.entry(*t).or_insert(0) += batch.len() as isize;
    });
    counts
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::solve::search;
    use crate::types::{Csp, Forbidden, Node as NodeTy};
    use differential_dataflow::AsCollection;
    use differential_dataflow::input::Input;
    use timely::dataflow::operators::ToStream;
    use timely::dataflow::operators::Probe;

    #[test]
    fn depth_counts_full_solutions() {
        // 2-colour one edge => 2 solutions at depth 2.
        let counts = timely::execute_directly(|worker| {
            worker.dataflow::<u64, _, _>(|scope| {
                let roots = vec![(Vec::<u16>::new(), 0u64, 1isize)]
                    .into_iter().to_stream(scope).as_collection();
                let forbidden: Vec<(Forbidden, u64, isize)> = vec![
                    ((0u16, 0u16, 1u16, 0u16), 0, 1),
                    ((0u16, 1u16, 1u16, 1u16), 0, 1),
                ];
                let forbidden = forbidden.into_iter().to_stream(scope).as_collection();
                let csp = Csp { n_vars: 2, domain: 2, forbidden: vec![] };
                count_by_depth(&search(&roots, &forbidden, csp))
            })
        });
        assert_eq!(counts.lock().unwrap().get(&2).copied(), Some(2));
    }

    #[test]
    fn incremental_add_constraint_shrinks_solutions() {
        // Round 1: 2 vars, domain 2, no constraints => 4 solutions (net +4).
        // Round 2: forbid (var0=0, var1=0) => removes [0,0] (net -1).
        // Verifies the search re-solves incrementally: a constraint delta produces a
        // small signed change in the solution set rather than a full recomputation.
        let net = timely::execute_directly(|worker| {
            let (mut roots_in, mut forb_in, probe, net) = worker.dataflow::<u32, _, _>(|scope| {
                let (rh, roots) = scope.new_collection::<NodeTy, isize>();
                let (fh, forbidden) = scope.new_collection::<Forbidden, isize>();
                let csp = Csp { n_vars: 2, domain: 2, forbidden: vec![] };
                let sols = search(&roots, &forbidden, csp);
                let net = net_by_round(&sols);
                let probe = sols.inner.probe().0;
                (rh, fh, probe, net)
            });

            // Round 1 (time 1): insert the root, no constraints. Advance first so the
            // insert lands at time 1 (InputSession inserts at its current time).
            roots_in.advance_to(1); forb_in.advance_to(1);
            roots_in.insert(Vec::new());
            roots_in.flush(); forb_in.flush();
            worker.step_while(|| probe.less_than(roots_in.time()));

            // Round 2 (time 2): forbid (var0=0, var1=0).
            roots_in.advance_to(2); forb_in.advance_to(2);
            forb_in.insert((0, 0, 1, 0));
            roots_in.flush(); forb_in.flush();
            worker.step_while(|| probe.less_than(roots_in.time()));

            net
            // roots_in / forb_in dropped here -> dataflow closes
        });

        let m = net.lock().unwrap();
        assert_eq!(m.get(&1).copied(), Some(4), "round 1 should add 4 solutions");
        assert_eq!(m.get(&2).copied(), Some(-1), "round 2 should retract 1 solution");
    }

    #[test]
    fn churn_recorded_per_round() {
        // Same scenario as the net test, but measuring absolute update volume:
        // round 1 emits 4 solution tuples, round 2 emits 1 retraction.
        let churn = timely::execute_directly(|worker| {
            let (mut roots_in, mut forb_in, probe, churn) = worker.dataflow::<u32, _, _>(|scope| {
                let (rh, roots) = scope.new_collection::<NodeTy, isize>();
                let (fh, forbidden) = scope.new_collection::<Forbidden, isize>();
                let csp = Csp { n_vars: 2, domain: 2, forbidden: vec![] };
                let sols = search(&roots, &forbidden, csp);
                let churn = churn_by_round(&sols);
                let probe = sols.inner.probe().0;
                (rh, fh, probe, churn)
            });

            roots_in.advance_to(1); forb_in.advance_to(1);
            roots_in.insert(Vec::new());
            roots_in.flush(); forb_in.flush();
            worker.step_while(|| probe.less_than(roots_in.time()));

            roots_in.advance_to(2); forb_in.advance_to(2);
            forb_in.insert((0, 0, 1, 0));
            roots_in.flush(); forb_in.flush();
            worker.step_while(|| probe.less_than(roots_in.time()));

            churn
        });

        let c = churn.lock().unwrap();
        assert_eq!(c.get(&1).copied(), Some(4), "round 1 emits 4 solution tuples");
        assert_eq!(c.get(&2).copied(), Some(1), "round 2 emits 1 retraction");
    }
}
