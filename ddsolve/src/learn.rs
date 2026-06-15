//! Conflict learning: detect dead-end nodes, project to a minimal nogood core, and
//! (Task 12) maintain learned nogoods to prune the frontier across iterations.

use differential_dataflow::VecCollection;
use differential_dataflow::lattice::Lattice;
use timely::progress::Timestamp;

use crate::types::{Node, Val, VarId};

/// A learned nogood: a partial assignment (var -> value) that has no consistent
/// completion. Represented as a sorted `Vec<(VarId, Val)>` so it can match any node
/// whose prefix is a superset.
pub type Nogood = Vec<(VarId, Val)>;

/// Dead-end nodes: nodes present in `live_parents` that have NO surviving child in
/// `survivors`. A survivor's parent is its prefix minus the last value.
pub fn dead_ends<'a, T>(
    live_parents: &VecCollection<'a, T, Node>,
    survivors: &VecCollection<'a, T, Node>,
) -> VecCollection<'a, T, Node>
where
    T: Timestamp + Lattice + Ord,
{
    // Parents that DO have a surviving child.
    let parents_with_child = survivors
        .clone()
        .flat_map(|child| {
            if child.is_empty() {
                None
            } else {
                let mut p = child.clone();
                p.pop();
                Some(p)
            }
        })
        .distinct();

    // live_parents that lack any surviving child = dead ends.
    live_parents
        .clone()
        .map(|n| (n, ()))
        .antijoin(parents_with_child)
        .map(|(n, ())| n)
}

/// Conservative full-prefix projection: the entire prefix is always a valid nogood.
/// Used as a sound fallback when a dead end is not a clean domain wipeout.
pub fn project_core(prefix: &Node) -> Nogood {
    prefix.iter().enumerate().map(|(i, &v)| (i as VarId, v)).collect()
}

/// Compute a nogood core for a dead-end `prefix`, given that the next variable
/// `next_var = prefix.len()` suffered domain wipeout (every value in `0..next_domain`
/// is forbidden by some already-assigned variable). A subset `S` of the prefix is a
/// valid nogood if, for every value `v` of `next_var`, some assignment in `S` forbids
/// `(next_var = v)`. We greedily pick, per value `v`, one prefix variable that forbids
/// it, and union those picks.
///
/// Sound: every returned variable genuinely rules out at least one value. Not guaranteed
/// globally minimal (greedy set cover), but strictly shrinks the prefix whenever some
/// prefix variables are irrelevant to the wipeout. If some value has no blame in the
/// prefix it is not a real wipeout, so we fall back to the full prefix (still sound).
pub fn project_core_against(
    prefix: &Node,
    next_domain: Val,
    next_var: VarId,
    forbidden: &[crate::types::Forbidden],
) -> Nogood {
    use crate::types::canon;
    use std::collections::BTreeSet;

    let forbidden_set: BTreeSet<crate::types::Forbidden> = forbidden.iter().copied().collect();
    let mut core: BTreeSet<(VarId, Val)> = BTreeSet::new();

    for v in 0..next_domain {
        let mut blamed = None;
        for (i, &xi) in prefix.iter().enumerate() {
            let key = canon(i as VarId, xi, next_var, v);
            if forbidden_set.contains(&key) {
                blamed = Some((i as VarId, xi));
                break;
            }
        }
        match blamed {
            Some(pair) => {
                core.insert(pair);
            }
            None => return project_core(prefix),
        }
    }
    core.into_iter().collect()
}

/// Count how often each variable appears across a nogood collection, as a VSIDS-style
/// activity signal. Higher activity = a variable implicated in more conflicts; a dynamic
/// variable order would branch on high-activity variables first. Returns `(VarId, count)`
/// where `count` is carried in the data (multiplicity is +1 per variable at convergence).
///
/// MEASUREMENT ONLY: the search uses a static index order; this reports what a dynamic
/// order *would* prioritise without rewiring `expand`. Rewiring is future work.
pub fn bump_activity<'a, T>(
    nogoods: &VecCollection<'a, T, Nogood>,
) -> VecCollection<'a, T, (VarId, isize)>
where
    T: Timestamp + Lattice + Ord,
{
    nogoods
        .clone()
        .flat_map(|ng| ng.into_iter().map(|(var, _val)| (var, ())).collect::<Vec<_>>())
        .reduce(|_var, input, output| {
            let total: isize = input.iter().map(|(_v, r)| *r).sum();
            output.push((total, 1));
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use differential_dataflow::AsCollection;
    use timely::dataflow::operators::{Capture, ToStream};
    use timely::dataflow::operators::capture::Extract;

    #[test]
    fn detect_dead_end() {
        // live parents [0] and [1]; survivors include [1,0] (child of [1]) but NO child
        // of [0]. => [0] is a dead end.
        let data = timely::example(|scope| {
            let parents = vec![(vec![0u16], 0u64, 1isize), (vec![1u16], 0u64, 1isize)]
                .into_iter().to_stream(scope).as_collection();
            let survivors = vec![(vec![1u16, 0u16], 0u64, 1isize)]
                .into_iter().to_stream(scope).as_collection();
            dead_ends(&parents, &survivors).consolidate().inner.capture()
        });

        let mut de: Vec<Node> = data.extract().into_iter()
            .flat_map(|(_, b)| b.into_iter().filter(|(_, _, r)| *r > 0).map(|(n, _, _)| n))
            .collect();
        de.sort();
        assert_eq!(de, vec![vec![0]]);
    }

    #[test]
    fn core_is_sound_and_shrinks() {
        use crate::types::canon;
        // Prefix [a=0, b=0, c=1] (vars 0,1,2). Next var = 3, domain 2.
        // Forbidden makes {a=0, b=0} a wipeout for var 3 regardless of c:
        //   a=0 forbids 3=0; b=0 forbids 3=1.  => minimal core {a=0, b=0}, dropping c.
        let forbidden = vec![
            canon(0, 0, 3, 0), // a=0 forbids d=0
            canon(1, 0, 3, 1), // b=0 forbids d=1
        ];
        let prefix = vec![0u16, 0, 1]; // a=0, b=0, c=1
        let mut core = super::project_core_against(&prefix, 2, 3, &forbidden);
        core.sort();
        assert_eq!(core, vec![(0u16, 0u16), (1, 0)], "core must be {{a=0,b=0}}, dropping c");
    }
}
