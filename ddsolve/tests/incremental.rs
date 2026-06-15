//! Characterise incremental re-solve churn (Item 2). For a fixed static variable order,
//! a constraint delta on a leaf-ward variable should disturb no more of the solution set
//! than the same delta on a root-ward variable. This encodes the hypothesis as an
//! assertion; if it ever fails it is a research finding to record, not silently a bug.

use ddsolve::instic;
use ddsolve::metrics::churn_by_round;
use ddsolve::solve::search;
use ddsolve::types::{canon, Forbidden, Node};
use differential_dataflow::input::Input;
use timely::dataflow::operators::Probe;

/// Round-2 churn when an extra same-value forbidden pair is added on `edge`.
fn churn_for_edge(n_vars: u16, domain: u16, seed: u64, edge: (u16, u16)) -> isize {
    let base = instic::random_colouring(n_vars, domain, 35, seed);
    let churn = timely::execute_directly(move |worker| {
        let (mut roots_in, mut forb_in, probe, churn) = worker.dataflow::<u32, _, _>(|scope| {
            let (rh, roots) = scope.new_collection::<Node, isize>();
            let (fh, forbidden) = scope.new_collection::<Forbidden, isize>();
            let sols = search(&roots, &forbidden, base.clone());
            let churn = churn_by_round(&sols);
            let probe = sols.inner.probe().0;
            (rh, fh, probe, churn)
        });

        roots_in.advance_to(1); forb_in.advance_to(1);
        roots_in.insert(Vec::new());
        for f in &base.forbidden {
            forb_in.insert(*f);
        }
        roots_in.flush(); forb_in.flush();
        worker.step_while(|| probe.less_than(roots_in.time()));

        roots_in.advance_to(2); forb_in.advance_to(2);
        forb_in.insert(canon(edge.0, 0, edge.1, 0));
        roots_in.flush(); forb_in.flush();
        worker.step_while(|| probe.less_than(roots_in.time()));

        churn
    });
    let c = churn.lock().unwrap();
    c.get(&2).copied().unwrap_or(0)
}

#[test]
fn leaf_ward_delta_churns_no_more_than_root_ward() {
    let n = 8u16;
    let root = churn_for_edge(n, 3, 7, (0, 1));
    let leaf = churn_for_edge(n, 3, 7, (n - 2, n - 1));
    // Hypothesis: leaf-ward perturbation disturbs <= root-ward perturbation.
    // Deterministic for a fixed seed. Observed (seed 7): leaf <= root.
    assert!(
        leaf <= root,
        "expected leaf-ward churn ({leaf}) <= root-ward churn ({root}) — \
         if this fails it is a research finding: record the numbers and the seed."
    );
}
