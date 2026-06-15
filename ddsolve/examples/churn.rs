//! Incremental-churn experiment. Solve a random colouring instance, then perturb one
//! constraint and measure the solution-set churn, varying whether the perturbed
//! constraint touches an early (root-ward) or late (leaf-ward) variable in the static
//! order. Item 2 hypothesis: leaf-ward deltas disturb no more than root-ward deltas.
//!
//! Usage:
//!   cargo run -p ddsolve --example churn --release -- <n_vars> <domain> <seed>

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

        // Round 1: root + all base constraints.
        roots_in.advance_to(1); forb_in.advance_to(1);
        roots_in.insert(Vec::new());
        for f in &base.forbidden {
            forb_in.insert(*f);
        }
        roots_in.flush(); forb_in.flush();
        worker.step_while(|| probe.less_than(roots_in.time()));

        // Round 2: add one same-value (colour 0) forbidden pair on `edge`.
        roots_in.advance_to(2); forb_in.advance_to(2);
        forb_in.insert(canon(edge.0, 0, edge.1, 0));
        roots_in.flush(); forb_in.flush();
        worker.step_while(|| probe.less_than(roots_in.time()));

        churn
    });
    let c = churn.lock().unwrap();
    c.get(&2).copied().unwrap_or(0)
}

fn main() {
    let mut args = std::env::args().skip(1);
    let n_vars: u16 = args.next().unwrap_or_else(|| "8".into()).parse().unwrap();
    let domain: u16 = args.next().unwrap_or_else(|| "3".into()).parse().unwrap();
    let seed: u64 = args.next().unwrap_or_else(|| "1".into()).parse().unwrap();

    let root = churn_for_edge(n_vars, domain, seed, (0, 1));
    let leaf = churn_for_edge(n_vars, domain, seed, (n_vars - 2, n_vars - 1));

    println!("# random_colouring n_vars={n_vars} domain={domain} seed={seed}");
    println!("root_ward edge (0,1):              round-2 churn = {root}");
    println!("leaf_ward edge ({},{}): round-2 churn = {leaf}", n_vars - 2, n_vars - 1);
}
