//! Frontier blow-up experiment. Builds a random graph-colouring instance and reports
//! the peak net live-frontier size per depth, illustrating that pure DD search
//! materialises the whole feasible frontier (mid-depths exceed the final solution count).
//!
//! Usage:
//!   cargo run -p ddsolve --example blowup --release -- <n_vars> <domain> <edge_pct> <seed>

use ddsolve::instic;
use ddsolve::metrics::count_by_depth;
use ddsolve::solve::search_live;
use ddsolve::types::Forbidden;
use differential_dataflow::AsCollection;
use timely::dataflow::operators::ToStream;

fn main() {
    let mut args = std::env::args().skip(1);
    let n_vars: u16 = args.next().unwrap_or_else(|| "6".into()).parse().unwrap();
    let domain: u16 = args.next().unwrap_or_else(|| "3".into()).parse().unwrap();
    let edge_pct: u32 = args.next().unwrap_or_else(|| "40".into()).parse().unwrap();
    let seed: u64 = args.next().unwrap_or_else(|| "0".into()).parse().unwrap();

    let csp = instic::random_colouring(n_vars, domain, edge_pct, seed);
    let fv = csp.forbidden.clone();

    // Instrument the full live frontier (all depths), not just the solution leaves.
    let counts = timely::execute_directly(move |worker| {
        worker.dataflow::<u64, _, _>(|scope| {
            let roots = vec![(Vec::<u16>::new(), 0u64, 1isize)]
                .into_iter().to_stream(scope).as_collection();
            let forbidden: Vec<(Forbidden, u64, isize)> =
                fv.iter().map(|f| (*f, 0u64, 1isize)).collect();
            let forbidden = forbidden.into_iter().to_stream(scope).as_collection();
            count_by_depth(&search_live(&roots, &forbidden, csp.clone()))
        })
    });

    println!("# random_colouring n_vars={n_vars} domain={domain} edge_pct={edge_pct} seed={seed}");
    println!("depth, net_live_nodes");
    let map = counts.lock().unwrap();
    for (depth, n) in map.iter() {
        println!("{depth}, {n}");
    }
    let peak = map.values().copied().max().unwrap_or(0);
    let solutions = map.get(&(n_vars as usize)).copied().unwrap_or(0);
    println!("# peak_frontier={peak} solutions={solutions}");
}
