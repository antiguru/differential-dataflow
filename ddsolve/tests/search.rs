use ddsolve::instic;
use ddsolve::solve::{search, search_beam};
use ddsolve::types::{Csp, Forbidden, Node};
use differential_dataflow::AsCollection;
use timely::dataflow::operators::{Capture, ToStream};
use timely::dataflow::operators::capture::Extract;

/// Enumerate proper 2-colourings of a single edge (var0 - var1), domain {0,1}.
/// Valid colourings: [0,1] and [1,0]. Forbidden: equal colours on the edge.
#[test]
fn two_colour_one_edge() {
    let data = timely::example(|scope| {
        let roots = vec![(Vec::<u16>::new(), 0u64, 1isize)]
            .into_iter().to_stream(scope).as_collection();
        // Forbid equal colours: (var0=0,var1=0) and (var0=1,var1=1).
        let forbidden: Vec<(Forbidden, u64, isize)> = vec![
            ((0u16, 0u16, 1u16, 0u16), 0u64, 1isize),
            ((0u16, 1u16, 1u16, 1u16), 0u64, 1isize),
        ];
        let forbidden = forbidden.into_iter().to_stream(scope).as_collection();
        let csp = Csp { n_vars: 2, domain: 2, forbidden: vec![] };
        search(&roots, &forbidden, csp).inner.capture()
    });

    let mut sols: Vec<Node> = data.extract().into_iter()
        .flat_map(|(_, b)| b.into_iter().filter(|(_, _, r)| *r > 0).map(|(n, _, _)| n))
        .collect();
    sols.sort();
    assert_eq!(sols, vec![vec![0, 1], vec![1, 0]]);
}

/// 3 tasks, 2 nodes. task0 & task1 affinity (same node); task1 & task2 anti-affinity
/// (different nodes). Every valid placement must obey both rules; concretely the only
/// solutions are [0,0,1] and [1,1,0].
#[test]
fn scheduling_affinity_antiaffinity() {
    let csp = instic::scheduling(3, 2, &[(0, 1)], &[(1, 2)], &[]);
    let fv = csp.forbidden.clone();

    let data = timely::example(move |scope| {
        let roots = vec![(Vec::<u16>::new(), 0u64, 1isize)]
            .into_iter().to_stream(scope).as_collection();
        let forbidden: Vec<(Forbidden, u64, isize)> =
            fv.iter().map(|f| (*f, 0u64, 1isize)).collect();
        let forbidden = forbidden.into_iter().to_stream(scope).as_collection();
        search(&roots, &forbidden, csp.clone()).inner.capture()
    });

    let mut sols: Vec<Node> = data.extract().into_iter()
        .flat_map(|(_, b)| b.into_iter().filter(|(_, _, r)| *r > 0).map(|(n, _, _)| n))
        .collect();
    sols.sort();

    assert!(!sols.is_empty());
    for s in &sols {
        assert_eq!(s.len(), 3);
        assert_eq!(s[0], s[1], "affinity violated: {s:?}");
        assert_ne!(s[1], s[2], "anti-affinity violated: {s:?}");
    }
    assert_eq!(sols, vec![vec![0, 0, 1], vec![1, 1, 0]]);
}

fn beam_solutions(csp: Csp, k: usize) -> Vec<Node> {
    let fv = csp.forbidden.clone();
    let data = timely::example(move |scope| {
        let roots = vec![(Vec::<u16>::new(), 0u64, 1isize)]
            .into_iter().to_stream(scope).as_collection();
        let forbidden: Vec<(Forbidden, u64, isize)> =
            fv.iter().map(|f| (*f, 0u64, 1isize)).collect();
        let forbidden = forbidden.into_iter().to_stream(scope).as_collection();
        search_beam(&roots, &forbidden, csp.clone(), k, |_n: &Node| 0).inner.capture()
    });
    let mut s: Vec<Node> = data.extract().into_iter()
        .flat_map(|(_, b)| b.into_iter().filter(|(_, _, r)| *r > 0).map(|(n, _, _)| n))
        .collect();
    s.sort();
    s
}

fn plain_solutions(csp: Csp) -> Vec<Node> {
    let fv = csp.forbidden.clone();
    let data = timely::example(move |scope| {
        let roots = vec![(Vec::<u16>::new(), 0u64, 1isize)]
            .into_iter().to_stream(scope).as_collection();
        let forbidden: Vec<(Forbidden, u64, isize)> =
            fv.iter().map(|f| (*f, 0u64, 1isize)).collect();
        let forbidden = forbidden.into_iter().to_stream(scope).as_collection();
        search(&roots, &forbidden, csp.clone()).inner.capture()
    });
    let mut s: Vec<Node> = data.extract().into_iter()
        .flat_map(|(_, b)| b.into_iter().filter(|(_, _, r)| *r > 0).map(|(n, _, _)| n))
        .collect();
    s.sort();
    s
}

/// With a beam wider than any depth's frontier, beam search equals full search.
/// With a narrow beam, beam solutions are a (possibly empty) subset — never a superset.
#[test]
fn beam_solutions_subset_of_full() {
    let csp = instic::scheduling(3, 2, &[(0, 1)], &[(1, 2)], &[]);
    let full = plain_solutions(csp.clone());

    let wide = beam_solutions(csp.clone(), 1000);
    assert_eq!(wide, full, "wide beam must equal full search");

    let narrow = beam_solutions(csp.clone(), 1);
    for s in &narrow {
        assert!(full.contains(s), "beam produced a non-solution {s:?}");
    }
}
