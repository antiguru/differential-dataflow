//! Measure conflict learning's pruning effect: peak net live-frontier size with vs
//! without learned nogoods on the same instance, plus the number of nogoods learned.
//!
//! Usage:
//!   cargo run -p ddsolve --example learning --release -- <n_vars> <domain> <seed>

use ddsolve::instic;
use ddsolve::metrics::count_by_depth;
use ddsolve::solve::{search_live, search_with_learning_live};
use ddsolve::types::Forbidden;
use differential_dataflow::AsCollection;
use timely::dataflow::operators::ToStream;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

fn peak(map: &BTreeMap<usize, isize>) -> isize {
    map.values().copied().max().unwrap_or(0)
}

fn main() {
    let mut a = std::env::args().skip(1);
    let n: u16 = a.next().unwrap_or_else(|| "7".into()).parse().unwrap();
    // Default domain 2: low domain relative to edge density produces domain wipeouts,
    // which is where conflict learning actually prunes (domain 3 is often too loose).
    let d: u16 = a.next().unwrap_or_else(|| "2".into()).parse().unwrap();
    let seed: u64 = a.next().unwrap_or_else(|| "1".into()).parse().unwrap();
    let csp = instic::random_colouring(n, d, 35, seed);

    // Plain search: instrument the full live frontier.
    let csp_p = csp.clone();
    let plain = timely::execute_directly(move |worker| {
        worker.dataflow::<u64, _, _>(|scope| {
            let roots = vec![(Vec::<u16>::new(), 0u64, 1isize)]
                .into_iter().to_stream(scope).as_collection();
            let forbidden: Vec<(Forbidden, u64, isize)> =
                csp_p.forbidden.iter().map(|f| (*f, 0u64, 1isize)).collect();
            let forbidden = forbidden.into_iter().to_stream(scope).as_collection();
            count_by_depth(&search_live(&roots, &forbidden, csp_p.clone()))
        })
    });

    // Learning search: instrument the live frontier and count learned nogoods.
    let csp_l = csp.clone();
    let (learned, nogood_count) = timely::execute_directly(move |worker| {
        worker.dataflow::<u64, _, _>(|scope| {
            let roots = vec![(Vec::<u16>::new(), 0u64, 1isize)]
                .into_iter().to_stream(scope).as_collection();
            let forbidden: Vec<(Forbidden, u64, isize)> =
                csp_l.forbidden.iter().map(|f| (*f, 0u64, 1isize)).collect();
            let forbidden = forbidden.into_iter().to_stream(scope).as_collection();
            let (live, nogoods) = search_with_learning_live(&roots, &forbidden, csp_l.clone());
            let depths = count_by_depth(&live);
            // Tally net learned nogoods.
            let ng_count: Arc<Mutex<isize>> = Arc::new(Mutex::new(0));
            let sink = ng_count.clone();
            nogoods.inspect_batch(move |_t, batch| {
                let mut c = sink.lock().unwrap();
                for (_ng, _tt, r) in batch.iter() {
                    *c += *r;
                }
            });
            (depths, ng_count)
        })
    });

    let plain_peak = peak(&plain.lock().unwrap());
    let learned_peak = peak(&learned.lock().unwrap());
    let ng = *nogood_count.lock().unwrap();
    println!("# random_colouring n_vars={n} domain={d} seed={seed}");
    println!("peak live frontier  plain={plain_peak}  learned={learned_peak}");
    println!("nogoods learned     {ng}");
}
