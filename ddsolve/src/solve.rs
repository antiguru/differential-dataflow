//! The search dataflow: branching, consistency checking, and the `iterate` core.

use differential_dataflow::VecCollection;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::*;
use differential_dataflow::operators::iterate::Variable;
use timely::order::Product;
use timely::progress::Timestamp;
use crate::types::{Csp, Node, Val, Forbidden};
use crate::learn::{dead_ends, project_core_against, Nogood};

/// Branch one node into its children. A node at depth `d < n_vars` produces one
/// child per domain value (the prefix extended by that value). A node at full
/// depth produces nothing (it is a solution leaf, not expanded).
pub fn expand<'a, T>(nodes: &VecCollection<'a, T, Node>, n_vars: u16, domain: Val) -> VecCollection<'a, T, Node>
where
    T: Timestamp + Lattice + Ord,
{
    nodes.clone().flat_map(move |node| {
        let depth = node.len() as u16;
        let lo = if depth < n_vars { 0 } else { domain }; // empty range when at full depth
        (lo..domain).map(move |v| {
            let mut child = node.clone();
            child.push(v);
            child
        })
    })
}

/// Keep only the children that are consistent with the forbidden-pair relation.
///
/// A child assigns variable `d = node.len() - 1` to value `node[d]`. By induction
/// the prefix `node[0..d]` is already consistent, so we only test the *new* pairs
/// the last assignment introduces: for each `j < d`, the pair `(j, node[j], d, node[d])`
/// (canonicalised). We emit those pairs keyed for a join against `forbidden`; any
/// match marks the node as conflicting, and an `antijoin` removes conflicting nodes.
pub fn check_consistent<'a, T>(
    children: &VecCollection<'a, T, Node>,
    forbidden: &VecCollection<'a, T, Forbidden>,
) -> VecCollection<'a, T, Node>
where
    T: Timestamp + Lattice + Ord,
{
    use crate::types::canon;

    // For each child, emit (forbidden_key, node) for every new pair it introduces.
    let introduced = children.clone().flat_map(|node| {
        let d = node.len();
        let mut pairs = Vec::new();
        if d >= 1 {
            let last = d - 1;
            let xd = node[last];
            for j in 0..last {
                let key = canon(j as u16, node[j], last as u16, xd);
                pairs.push((key, node.clone()));
            }
        }
        pairs.into_iter()
    });

    // Key the forbidden relation by its full tuple (it *is* the key) with unit value.
    let forbidden_keyed = forbidden.clone().map(|f| (f, ()));

    // Nodes that hit at least one forbidden pair.
    let conflicting = introduced
        .join_map(forbidden_keyed, |_key, node, ()| node.clone())
        .distinct();

    // children \ conflicting  (set difference by node identity).
    children.clone()
        .map(|n| (n, ()))
        .antijoin(conflicting)
        .map(|(n, ())| n)
}

/// Grow the frontier of *all* live nodes (every depth) to a fixed point.
///
/// Each round expands every non-leaf live node (`expand`), prunes inconsistent children
/// (`check_consistent`), and unions survivors with the roots, deduped. The fixed point
/// is reached once no new nodes appear (all live nodes are full-depth leaves or were
/// pruned). The returned collection contains nodes at *all* depths — instrument it with
/// `metrics::count_by_depth` to observe frontier blow-up.
pub fn search_live<'a, T>(
    roots: &VecCollection<'a, T, Node>,
    forbidden: &VecCollection<'a, T, Forbidden>,
    csp: Csp,
) -> VecCollection<'a, T, Node>
where
    T: Timestamp + Lattice + Ord,
{
    let n_vars = csp.n_vars;
    let domain = csp.domain;

    let roots_outer = roots.clone();
    let forbidden_outer = forbidden.clone();

    roots.clone().iterate(|scope, live| {
        let roots = roots_outer.enter(scope);
        let forbidden = forbidden_outer.enter(scope);

        let children = expand(&live, n_vars, domain);
        let survivors = check_consistent(&children, &forbidden);

        roots.concat(survivors).distinct()
    })
}

/// Run the full search. Returns the collection of complete, consistent assignments
/// (solution leaves at depth `n_vars`) — the full-depth members of the `search_live`
/// fixed point.
pub fn search<'a, T>(
    roots: &VecCollection<'a, T, Node>,
    forbidden: &VecCollection<'a, T, Forbidden>,
    csp: Csp,
) -> VecCollection<'a, T, Node>
where
    T: Timestamp + Lattice + Ord,
{
    let n_vars = csp.n_vars;
    search_live(roots, forbidden, csp)
        .filter(move |node| node.len() as u16 == n_vars)
        .consolidate()
}

/// Keep the top-`k` nodes per depth, by a caller-supplied score (higher = keep).
///
/// WARNING: this makes search INCOMPLETE — it may discard the subtree containing a
/// solution. It is a deliberate research knob (beam search), not a default. The number
/// of nodes dropped per depth is `frontier_before − min(frontier_before, k)`.
pub fn beam<'a, T, F>(live: &VecCollection<'a, T, Node>, k: usize, score: F) -> VecCollection<'a, T, Node>
where
    T: Timestamp + Lattice + Ord,
    F: Fn(&Node) -> i64 + 'static,
{
    live.clone()
        .map(|n| (n.len(), n))
        .reduce(move |_depth, input, output| {
            let mut scored: Vec<(Node, i64)> = input
                .iter()
                .map(|pair| {
                    let n: &Node = pair.0;
                    (n.clone(), score(n))
                })
                .collect();
            scored.sort_by(|a, b| b.1.cmp(&a.1)); // descending score
            for (n, _) in scored.into_iter().take(k) {
                output.push((n, 1));
            }
        })
        .map(|(_d, n)| n)
}

/// Beam-bounded search: like [`search`], but each round retains only the top-`k` nodes
/// per depth (by `score`). Returns the solution leaves found within the beam. INCOMPLETE:
/// with small `k` it may miss solutions; with `k` larger than any depth's frontier it
/// equals [`search`].
pub fn search_beam<'a, T, F>(
    roots: &VecCollection<'a, T, Node>,
    forbidden: &VecCollection<'a, T, Forbidden>,
    csp: Csp,
    k: usize,
    score: F,
) -> VecCollection<'a, T, Node>
where
    T: Timestamp + Lattice + Ord,
    F: Fn(&Node) -> i64 + 'static,
{
    let n_vars = csp.n_vars;
    let domain = csp.domain;
    let roots_outer = roots.clone();
    let forbidden_outer = forbidden.clone();

    let live = roots.clone().iterate(move |scope, live| {
        let roots = roots_outer.enter(scope);
        let forbidden = forbidden_outer.enter(scope);

        let children = expand(&live, n_vars, domain);
        let survivors = check_consistent(&children, &forbidden);
        let bounded = beam(&survivors, k, score);

        roots.concat(bounded).distinct()
    });

    live.filter(move |node| node.len() as u16 == n_vars).consolidate()
}

/// Drop children for which some learned nogood is a subset of the prefix.
///
/// Key learned nogoods by their first `(var, val)` and children by each `(var, val)`
/// they contain; join on that, then keep a child only if the FULL nogood is a subset of
/// its prefix. `antijoin` removes the blocked children. A nogood is an unsatisfiable
/// partial assignment, so this never removes a node with a consistent completion.
fn prune_by_nogoods<'a, T>(
    children: &VecCollection<'a, T, Node>,
    nogoods: &VecCollection<'a, T, Nogood>,
) -> VecCollection<'a, T, Node>
where
    T: Timestamp + Lattice + Ord,
{
    let nogood_keyed = nogoods
        .clone()
        .filter(|ng| !ng.is_empty())
        .map(|ng| (ng[0], ng));

    let child_pairs = children.clone().flat_map(|node| {
        node.iter()
            .enumerate()
            .map(|(i, &v)| ((i as u16, v), node.clone()))
            .collect::<Vec<_>>()
    });

    let blocked = child_pairs
        .join_map(nogood_keyed, |_k, node, ng| (node.clone(), ng.clone()))
        .filter(|(node, ng)| {
            ng.iter()
                .all(|&(var, val)| (var as usize) < node.len() && node[var as usize] == val)
        })
        .map(|(node, _ng)| node)
        .distinct();

    children
        .clone()
        .map(|n| (n, ()))
        .antijoin(blocked)
        .map(|(n, ())| n)
}

/// Search with conflict learning, returning the full live frontier (all depths) and the
/// learned-nogood set. Mutual recursion: the live frontier and the nogood set co-evolve
/// in one iterative scope. Each round expands the frontier, prunes by base constraints
/// AND learned nogoods, derives nogoods from dead ends (greedy core projection), and
/// feeds both back. See [`search_with_learning`] for the solution-only wrapper.
pub fn search_with_learning_live<'a, T>(
    roots: &VecCollection<'a, T, Node>,
    forbidden: &VecCollection<'a, T, Forbidden>,
    csp: Csp,
) -> (VecCollection<'a, T, Node>, VecCollection<'a, T, Nogood>)
where
    T: Timestamp + Lattice + Ord,
{
    let n_vars = csp.n_vars;
    let domain = csp.domain;
    let forbidden_vec = csp.forbidden.clone();

    let roots_c = roots.clone();
    let forbidden_c = forbidden.clone();
    let scope = roots.scope();

    scope.iterative::<u64, _, _>(move |nested| {
        let roots_in = roots_c.enter(nested);
        let forbidden_in = forbidden_c.enter(nested);
        // Two feedback variables (no source): the live frontier and the learned nogoods.
        let (live_var, live) = Variable::new(nested.clone(), Product::new(Default::default(), 1));
        let (nogood_var, nogoods) = Variable::new(nested.clone(), Product::new(Default::default(), 1));

        let children = expand(&live, n_vars, domain);
        let consistent = check_consistent(&children, &forbidden_in);
        let survivors = prune_by_nogoods(&consistent, &nogoods);

        // Dead ends among the current frontier -> projected nogood cores. Exclude
        // full-depth solution leaves: they legitimately have no children, so they would
        // otherwise be misread as dead ends and pruned as nogoods (removing solutions).
        let non_full = live.clone().filter(move |n| (n.len() as u16) < n_vars);
        let de = dead_ends(&non_full, &survivors);
        let fv = forbidden_vec.clone();
        let new_nogoods = de.map(move |prefix| {
            let next_var = prefix.len() as u16;
            project_core_against(&prefix, domain, next_var, &fv)
        });

        let next_live = roots_in.concat(survivors).distinct();
        let all_nogoods = nogoods.concat(new_nogoods).distinct();

        live_var.set(next_live.clone());
        nogood_var.set(all_nogoods.clone());

        (next_live.leave(scope), all_nogoods.leave(scope))
    })
}

/// Search with conflict learning. Returns the complete consistent assignments (solution
/// leaves) and the learned-nogood set. Learning never changes the solution set — nogoods
/// only prune partial assignments that have no consistent completion.
pub fn search_with_learning<'a, T>(
    roots: &VecCollection<'a, T, Node>,
    forbidden: &VecCollection<'a, T, Forbidden>,
    csp: Csp,
) -> (VecCollection<'a, T, Node>, VecCollection<'a, T, Nogood>)
where
    T: Timestamp + Lattice + Ord,
{
    let n_vars = csp.n_vars;
    let (live, learned) = search_with_learning_live(roots, forbidden, csp);
    let solutions = live
        .filter(move |node| node.len() as u16 == n_vars)
        .consolidate();
    (solutions, learned)
}

#[cfg(test)]
mod tests {
    use super::*;
    use differential_dataflow::AsCollection;
    use timely::dataflow::operators::{Capture, ToStream};
    use timely::dataflow::operators::capture::Extract;

    #[test]
    fn prune_forbidden_child() {
        // Two vars, domain 2. Parent assigns var0 = 1 (node = [1]).
        // Forbid (var0=1, var1=1). Expanding [1] gives children [1,0] and [1,1];
        // [1,1] must be pruned, leaving exactly [1,0].
        let data = timely::example(|scope| {
            let parents = vec![(vec![1u16], 0, 1isize)]
                .into_iter().to_stream(scope).as_collection();
            let forbidden = vec![((0u16, 1u16, 1u16, 1u16), 0, 1isize)]
                .into_iter().to_stream(scope).as_collection();
            let children = expand(&parents, 2, 2);
            check_consistent(&children, &forbidden).consolidate().inner.capture()
        });

        let mut survivors: Vec<Node> = data.extract().into_iter()
            .flat_map(|(_, b)| b.into_iter().filter(|(_, _, r)| *r > 0).map(|(n, _, _)| n))
            .collect();
        survivors.sort();
        assert_eq!(survivors, vec![vec![1, 0]]);
    }

    #[test]
    fn expand_one_level() {
        // Root = empty assignment. n_vars = 2, domain = 3.
        // One expansion step must yield exactly 3 depth-1 children.
        //
        // Use static `to_stream` data (auto-closing) rather than an InputSession:
        // an InputSession created inside `timely::example` is never driven/closed,
        // so the dataflow frontier never empties and the test hangs.
        let data = timely::example(|scope| {
            let roots = vec![(Vec::<Val>::new(), 0, 1isize)]
                .into_iter()
                .to_stream(scope)
                .as_collection();
            expand(&roots, 2, 3).consolidate().inner.capture()
        });

        let total: isize = data.extract().iter()
            .flat_map(|(_, batch)| batch.iter().map(|(_, _, r)| *r)).sum();
        assert_eq!(total, 3, "expected 3 children of the empty root");
    }
}
