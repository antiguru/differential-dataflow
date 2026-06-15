//! Learning must not change the solution set (soundness): nogoods only prune partial
//! assignments with no consistent completion. Assert solution-set equality between
//! `search` and `search_with_learning` on a scheduling instance.

use ddsolve::instic;
use ddsolve::solve::{search, search_with_learning};
use ddsolve::types::{Csp, Forbidden, Node};
use differential_dataflow::AsCollection;
use timely::dataflow::operators::{Capture, ToStream};
use timely::dataflow::operators::capture::Extract;

fn extract_pos(
    data: Vec<(u64, Vec<(Node, u64, isize)>)>,
) -> Vec<Node> {
    let mut s: Vec<Node> = data.into_iter()
        .flat_map(|(_, b)| b.into_iter().filter(|(_, _, r)| *r > 0).map(|(n, _, _)| n))
        .collect();
    s.sort();
    s
}

fn solutions_plain(csp: Csp) -> Vec<Node> {
    let fv = csp.forbidden.clone();
    let data = timely::example(move |scope| {
        let roots = vec![(Vec::<u16>::new(), 0u64, 1isize)]
            .into_iter().to_stream(scope).as_collection();
        let forbidden: Vec<(Forbidden, u64, isize)> =
            fv.iter().map(|f| (*f, 0u64, 1isize)).collect();
        let forbidden = forbidden.into_iter().to_stream(scope).as_collection();
        search(&roots, &forbidden, csp.clone()).inner.capture()
    });
    extract_pos(data.extract())
}

fn solutions_learning(csp: Csp) -> Vec<Node> {
    let fv = csp.forbidden.clone();
    let data = timely::example(move |scope| {
        let roots = vec![(Vec::<u16>::new(), 0u64, 1isize)]
            .into_iter().to_stream(scope).as_collection();
        let forbidden: Vec<(Forbidden, u64, isize)> =
            fv.iter().map(|f| (*f, 0u64, 1isize)).collect();
        let forbidden = forbidden.into_iter().to_stream(scope).as_collection();
        let (sols, _learned) = search_with_learning(&roots, &forbidden, csp.clone());
        sols.inner.capture()
    });
    extract_pos(data.extract())
}

#[test]
fn learning_preserves_solutions() {
    let csp = instic::scheduling(4, 2, &[(0, 1)], &[(1, 2), (2, 3)], &[]);
    assert_eq!(solutions_plain(csp.clone()), solutions_learning(csp));
}

#[test]
fn learning_preserves_solutions_colouring() {
    let csp = instic::random_colouring(6, 3, 40, 2);
    assert_eq!(solutions_plain(csp.clone()), solutions_learning(csp));
}
