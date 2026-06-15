//! VSIDS-style activity measurement. Runs the learning search on a colouring instance,
//! tallies how often each variable appears in a learned nogood, and prints the ranking
//! (descending). High-activity variables are the ones a dynamic branching order would
//! prioritise — compare against the natural index order to judge whether dynamic
//! ordering would diverge. Measurement only; the search itself uses a static order.
//!
//! Usage:
//!   cargo run -p ddsolve --example activity --release -- <n_vars> <domain> <seed>

use ddsolve::instic;
use ddsolve::learn::bump_activity;
use ddsolve::solve::search_with_learning_live;
use ddsolve::types::{Forbidden, VarId};
use differential_dataflow::AsCollection;
use timely::dataflow::operators::ToStream;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

fn main() {
    let mut a = std::env::args().skip(1);
    let n: u16 = a.next().unwrap_or_else(|| "7".into()).parse().unwrap();
    let d: u16 = a.next().unwrap_or_else(|| "2".into()).parse().unwrap();
    let seed: u64 = a.next().unwrap_or_else(|| "1".into()).parse().unwrap();
    let csp = instic::random_colouring(n, d, 35, seed);

    let activity: Arc<Mutex<BTreeMap<VarId, isize>>> = Arc::new(Mutex::new(BTreeMap::new()));
    let act_out = activity.clone();

    timely::execute_directly(move |worker| {
        worker.dataflow::<u64, _, _>(|scope| {
            let roots = vec![(Vec::<u16>::new(), 0u64, 1isize)]
                .into_iter().to_stream(scope).as_collection();
            let forbidden: Vec<(Forbidden, u64, isize)> =
                csp.forbidden.iter().map(|f| (*f, 0u64, 1isize)).collect();
            let forbidden = forbidden.into_iter().to_stream(scope).as_collection();
            let (_live, nogoods) = search_with_learning_live(&roots, &forbidden, csp.clone());
            let activity = bump_activity(&nogoods);
            let sink = act_out.clone();
            activity.inspect_batch(move |_t, batch| {
                let mut m = sink.lock().unwrap();
                for ((var, count), _tt, r) in batch.iter() {
                    if *r > 0 {
                        m.insert(*var, *count);
                    }
                }
            });
        })
    });

    println!("# random_colouring n_vars={n} domain={d} seed={seed}");
    let m = activity.lock().unwrap();
    let mut ranked: Vec<(VarId, isize)> = m.iter().map(|(v, c)| (*v, *c)).collect();
    ranked.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    println!("var, activity (descending)");
    for (var, count) in &ranked {
        println!("{var}, {count}");
    }
    if ranked.is_empty() {
        println!("# no nogoods learned on this instance (no domain wipeouts)");
    } else {
        let order: Vec<VarId> = ranked.iter().map(|(v, _)| *v).collect();
        println!("# dynamic order would prioritise: {order:?}  (index order: 0..{n})");
    }
}
