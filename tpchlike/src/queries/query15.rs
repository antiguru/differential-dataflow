use timely::dataflow::*;
use timely::dataflow::operators::*;
use timely::dataflow::operators::probe::Handle as ProbeHandle;

use differential_dataflow::AsCollection;
use differential_dataflow::operators::*;
use differential_dataflow::lattice::Lattice;

use ::Collections;
use ::types::create_date;

// -- $ID$
// -- TPC-H/TPC-R Top Supplier Query (Q15)
// -- Functional Query Definition
// -- Approved February 1998
// :x
// create view revenue:s (supplier_no, total_revenue) as
//     select
//         l_suppkey,
//         sum(l_extendedprice * (1 - l_discount))
//     from
//         lineitem
//     where
//         l_shipdate >= date ':1'
//         and l_shipdate < date ':1' + interval '3' month
//     group by
//         l_suppkey;
//
// :o
// select
//     s_suppkey,
//     s_name,
//     s_address,
//     s_phone,
//     total_revenue
// from
//     supplier,
//     revenue:s
// where
//     s_suppkey = supplier_no
//     and total_revenue = (
//         select
//             max(total_revenue)
//         from
//             revenue:s
//     )
// order by
//     s_suppkey;
//
// drop view revenue:s;
// :n -1

pub fn query<G: Scope>(collections: &mut Collections<G>) -> ProbeHandle<G::Timestamp> 
where G::Timestamp: Lattice+Ord {

    // revenue by supplier
    let revenue = 
        collections
            .lineitems()
            .inner
            .flat_map(|(item, time, diff)| 
                if create_date(1996, 1, 1) <= item.ship_date && item.ship_date < create_date(1996,4,1) {
                    Some((item.supp_key, time, item.extended_price * diff as i64)).into_iter()
                }
                else { None.into_iter() }
            )
            // .filter(|x| ::types::create_date(1996, 1, 1) <= x.ship_date && x.ship_date < ::types::create_date(1996,4,1))
            // .inner
            // .map(|(item, time, diff)| (item.supp_key, time, item.extended_price * diff as i64))
            .as_collection()
            .count();

    // suppliers with maximum revenue
    let top_suppliers =
        revenue
            .map(|(supp, total)| ((), (-total, supp)))
            .group(|_k, s, t| {
               let target = (s[0].0).0;    // <-- largest revenue
               t.extend(s.iter().take_while(|x| (x.0).0 == target));
            })
            .map(|(_,(total, supp))| (supp, -total));

    collections
        .suppliers()
        .map(|s| (s.supp_key, (s.name, s.address.to_string(), s.phone)))
        .join(&top_suppliers)
        .probe()
        .0
}