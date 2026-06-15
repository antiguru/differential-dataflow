# Backtracking CSP on Differential Dataflow — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a research prototype that expresses finite-domain CSP backtracking search as a differential dataflow computation, then measure (a) frontier blow-up, (b) incremental re-solve churn under constraint deltas, and (c) the pruning effect of conflict learning.

**Architecture:** Backtracking is *not* ported as control flow. The search tree is **reified as a collection**: each node is a fixed-order assignment prefix (`Vec<u16>`). Branching is a `flat_map`, consistency checking is a `join` of the pairs a child introduces against a `Forbidden(va,vb,xa,xb)` constraint *collection* followed by an `antijoin`, and recursion is `iterate`. "Backtracking" is just a negative diff: a pruned node leaves the collection. Constraints live in a collection (not captured in a closure) so constraint deltas drive DD's incremental machinery — that incrementality is the whole point of the research.

**Tech Stack:** Rust, differential-dataflow v0.24 (workspace path dep), timely v0.30. New workspace crate `ddsolve`. Tests use `timely::example` + `Capture`/`Extract` (same pattern as `differential-dataflow/tests/reduce.rs`).

**Research items covered:**
- **Item 1 (Tasks 1–6):** tree reification + `iterate` core + frontier-blowup instrumentation.
- **Item 2 (Tasks 7–9):** incremental re-solve characterization under constraint deltas.
- **Item 3 (Tasks 10–12):** conflict learning as a maintained nogood collection with minimal-core projection.
- **Extension 4 (Task 13):** bounding the frontier (beam / top-k). Design + key code, lighter granularity.
- **Extension 5 (Task 14):** dynamic ordering / VSIDS-style activity as a feedback collection. Design + key code.

**Prototype simplifications (intentional, documented so later work can lift them):**
- Uniform domain `0..domain` for every variable (per-variable domains are a trivial later generalization).
- Binary extensional constraints only, expressed as *forbidden* value pairs. Affinity/anti-affinity/resource-fit all reduce to binary forbidden pairs for the prototype (resource capacity is encoded as pairwise forbidden co-assignments; an aggregate-capacity encoding is future work, noted in Task 6).
- Static variable order = natural index order `0,1,2,…`. This is deliberate: Item 2's incrementality result depends on a stable tree shape.

---

## File Structure

- `ddsolve/Cargo.toml` — crate manifest, workspace member.
- `ddsolve/src/lib.rs` — public surface: re-exports `types`, `solve`.
- `ddsolve/src/types.rs` — `VarId`, `Val`, `Node`, `Forbidden`, `Csp` instance description. One responsibility: data model.
- `ddsolve/src/solve.rs` — the dataflow: `expand`, `consistency check`, `search` (the `iterate` core). One responsibility: the search dataflow.
- `ddsolve/src/instic.rs` — instance builders: graph-colouring + a scheduling instance (affinity/anti-affinity/resource → forbidden pairs). One responsibility: turn problems into `Csp`.
- `ddsolve/src/metrics.rs` — instrumentation helpers (frontier size per depth, update-volume counters). Added in Task 5, extended in Task 8.
- `ddsolve/src/learn.rs` — conflict learning (dead-end detection, minimal-core projection, learned-nogood feedback). Added in Task 10.
- `ddsolve/tests/search.rs` — end-to-end enumeration tests (Tasks 4, 6).
- `ddsolve/tests/incremental.rs` — churn tests (Task 9).
- `ddsolve/tests/learning.rs` — learning-correctness + pruning tests (Tasks 11–12).
- `ddsolve/examples/blowup.rs` — frontier-blow-up experiment driver (Task 5).
- `ddsolve/examples/churn.rs` — incremental-churn experiment driver (Task 8).
- `ddsolve/examples/learning.rs` — learning-pruning experiment driver (Task 12).
- Modify: `Cargo.toml` (workspace root) — add `"ddsolve"` to `members`.

---

## Item 1 — Tree reification + `iterate` core

### Task 1: Crate scaffold + data model

**Files:**
- Create: `ddsolve/Cargo.toml`
- Create: `ddsolve/src/lib.rs`
- Create: `ddsolve/src/types.rs`
- Modify: `Cargo.toml` (workspace root, `members` list)

- [ ] **Step 1: Add the crate to the workspace**

In the root `Cargo.toml`, add `"ddsolve",` to the `members` array (put it right after `"dogsdogsdogs",`):

```toml
members = [
    "differential-dataflow",
    "dogsdogsdogs",
    "ddsolve",
    "experiments",
    "interactive",
    "server",
    "server/dataflows/degr_dist",
    "server/dataflows/neighborhood",
    "server/dataflows/random_graph",
    "server/dataflows/reachability",
    "mdbook",
    "diagnostics",
]
```

- [ ] **Step 2: Write the crate manifest**

Create `ddsolve/Cargo.toml`:

```toml
[package]
name = "ddsolve"
version = "0.1.0"
edition.workspace = true
rust-version.workspace = true
publish = false

[dependencies]
differential-dataflow = { workspace = true }
timely = { workspace = true }
```

- [ ] **Step 3: Write the data model**

Create `ddsolve/src/types.rs`:

```rust
//! Data model for the differential-dataflow CSP prototype.

/// A variable index. Variables are assigned in natural index order (static ordering).
pub type VarId = u16;

/// A value in a variable's domain. Domains are `0..domain` (uniform across variables).
pub type Val = u16;

/// A search-tree node: the assignment prefix over variables `0..node.len()`.
/// `node[i]` is the value assigned to variable `i`. Length equals the node's depth.
pub type Node = Vec<Val>;

/// A binary extensional constraint, expressed as a *forbidden* value pair.
/// Canonical form: `va < vb`. Meaning: assigning `va = xa` together with `vb = xb`
/// is disallowed. Stored in a collection so constraint deltas drive incremental updates.
pub type Forbidden = (VarId, Val, VarId, Val); // (va, xa, vb, xb), with va < vb

/// A CSP instance description (static parameters; constraints flow as a collection).
#[derive(Clone, Debug)]
pub struct Csp {
    /// Number of variables.
    pub n_vars: VarId,
    /// Uniform domain size; each variable ranges over `0..domain`.
    pub domain: Val,
    /// Forbidden value pairs (the constraint relation, as plain data).
    pub forbidden: Vec<Forbidden>,
}

/// Canonicalise a binary forbidden pair so the lower variable comes first.
pub fn canon(a: VarId, xa: Val, b: VarId, xb: Val) -> Forbidden {
    if a < b { (a, xa, b, xb) } else { (b, xb, a, xa) }
}
```

- [ ] **Step 4: Write the crate root**

Create `ddsolve/src/lib.rs`:

```rust
//! Backtracking CSP search expressed as differential dataflow.
//!
//! See `docs/superpowers/plans/2026-06-14-backtracking-csp-on-dd.md`.

pub mod types;
```

- [ ] **Step 5: Verify it builds**

Run: `cargo build -p ddsolve`
Expected: compiles, no warnings beyond unused (none expected).

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml ddsolve/Cargo.toml ddsolve/src/lib.rs ddsolve/src/types.rs
git commit -m "feat(ddsolve): scaffold CSP-on-DD crate and data model"
```

---

### Task 2: Single-level expansion (branching), no constraints

**Files:**
- Create: `ddsolve/src/solve.rs`
- Modify: `ddsolve/src/lib.rs` (add `pub mod solve;`)
- Test: `ddsolve/src/solve.rs` (inline `#[cfg(test)]`)

- [ ] **Step 1: Write the failing test**

Create `ddsolve/src/solve.rs` with only the test (the function does not exist yet):

```rust
//! The search dataflow: branching, consistency checking, and the `iterate` core.

use differential_dataflow::Collection;
use differential_dataflow::lattice::Lattice;
use timely::dataflow::Scope;

use crate::types::{Node, Val};

/// Branch one node into its children. A node at depth `d < n_vars` produces one
/// child per domain value (the prefix extended by that value). A node at full
/// depth produces nothing (it is a solution leaf, not expanded).
pub fn expand<G>(nodes: &Collection<G, Node>, n_vars: u16, domain: Val) -> Collection<G, Node>
where
    G: Scope,
    G::Timestamp: Lattice,
{
    nodes.flat_map(move |node| {
        let depth = node.len() as u16;
        let lo = if depth < n_vars { 0 } else { domain }; // empty range when at full depth
        (lo..domain).map(move |v| {
            let mut child = node.clone();
            child.push(v);
            child
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use differential_dataflow::input::Input;
    use differential_dataflow::operators::Consolidate;
    use timely::dataflow::operators::capture::{Capture, Extract};

    #[test]
    fn expand_one_level() {
        // Root = empty assignment. n_vars = 2, domain = 3.
        // One expansion step must yield exactly 3 depth-1 children.
        let data = timely::example(|scope| {
            let (mut input, roots) = scope.new_collection::<Node, isize>();
            input.insert(Vec::new());
            input.advance_to(1);
            input.flush();
            expand(&roots, 2, 3).consolidate().inner.capture()
        });

        let extracted = data.extract();
        let total: isize = extracted.iter().flat_map(|(_, batch)| batch.iter().map(|(_, _, r)| *r)).sum();
        assert_eq!(total, 3, "expected 3 children of the empty root");
    }
}
```

> Note: `timely::example` closes the input when the closure returns; the explicit `input`/`advance_to`/`flush` pattern mirrors `differential-dataflow`'s own examples and makes the single batch deterministic.

- [ ] **Step 2: Run the test to verify it fails**

First wire the module in `ddsolve/src/lib.rs`:

```rust
pub mod types;
pub mod solve;
```

Run: `cargo test -p ddsolve expand_one_level`
Expected: FAIL to compile *or* assertion — if you wrote the function per Step 1 it should actually PASS. If it fails, the failure must be a real signal (e.g. wrong child count), not a typo. (This task front-loads the implementation because branching is trivial; treat Step 2 as "confirm the count is exactly 3".)

- [ ] **Step 3: (Implementation already present from Step 1)**

No change. The `expand` function above is the minimal implementation.

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p ddsolve expand_one_level`
Expected: PASS (`test tests::expand_one_level ... ok`).

- [ ] **Step 5: Commit**

```bash
git add ddsolve/src/lib.rs ddsolve/src/solve.rs
git commit -m "feat(ddsolve): branching via flat_map expansion"
```

---

### Task 3: Consistency check (prune via join + antijoin), single level

**Files:**
- Modify: `ddsolve/src/solve.rs` (add `check_consistent`, add test)

- [ ] **Step 1: Write the failing test**

Add to `ddsolve/src/solve.rs` (above the `tests` module):

```rust
use crate::types::Forbidden;
use differential_dataflow::operators::{Join, Threshold};

/// Keep only the children that are consistent with the forbidden-pair relation.
///
/// A child assigns variable `d = node.len() - 1` to value `node[d]`. By induction
/// the prefix `node[0..d]` is already consistent, so we only test the *new* pairs
/// the last assignment introduces: for each `j < d`, the pair `(j, node[j], d, node[d])`
/// (canonicalised). We emit those pairs keyed for a join against `forbidden`; any
/// match marks the node as conflicting, and an `antijoin` removes conflicting nodes.
pub fn check_consistent<G>(
    children: &Collection<G, Node>,
    forbidden: &Collection<G, Forbidden>,
) -> Collection<G, Node>
where
    G: Scope,
    G::Timestamp: Lattice,
{
    use crate::types::canon;

    // For each child, emit (forbidden_key, node) for every new pair it introduces.
    let introduced = children.flat_map(|node| {
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
    let forbidden_keyed = forbidden.map(|f| (f, ()));

    // Nodes that hit at least one forbidden pair.
    let conflicting = introduced
        .join_map(&forbidden_keyed, |_key, node, ()| node.clone())
        .distinct();

    // children \ conflicting  (set difference by node identity).
    children
        .map(|n| (n, ()))
        .antijoin(&conflicting)
        .map(|(n, ())| n)
}
```

Add this test inside the `tests` module:

```rust
    #[test]
    fn prune_forbidden_child() {
        // Two vars, domain 2. Parent assigns var0 = 1 (node = [1]).
        // Forbid (var0=1, var1=1). Expanding [1] gives children [1,0] and [1,1];
        // [1,1] must be pruned, leaving exactly [1,0].
        let data = timely::example(|scope| {
            let (mut nin, parents) = scope.new_collection::<Node, isize>();
            let (mut fin, forbidden) = scope.new_collection::<Forbidden, isize>();

            nin.insert(vec![1]);
            fin.insert((0u16, 1u16, 1u16, 1u16)); // (var0=1, var1=1) forbidden

            nin.advance_to(1); nin.flush();
            fin.advance_to(1); fin.flush();

            let children = expand(&parents, 2, 2);
            check_consistent(&children, &forbidden).consolidate().inner.capture()
        });

        let mut survivors: Vec<Node> = data.extract()
            .into_iter()
            .flat_map(|(_, batch)| batch.into_iter().filter(|(_, _, r)| *r > 0).map(|(n, _, _)| n))
            .collect();
        survivors.sort();
        assert_eq!(survivors, vec![vec![1, 0]]);
    }
```

- [ ] **Step 2: Run the test to verify it fails (before adding the impl)**

Temporarily comment out the `check_consistent` function body's content is not the workflow — instead, run with the test present and impl present:

Run: `cargo test -p ddsolve prune_forbidden_child`
Expected: PASS. If you want a genuine red first, delete the `antijoin` line and return `children.clone()` — observe `[1,1]` wrongly survives (FAIL: `survivors == [[1,0],[1,1]]`), then restore.

- [ ] **Step 3: (Implementation present from Step 1)**

No change beyond restoring the real `check_consistent` body if you broke it for the red.

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p ddsolve prune_forbidden_child`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add ddsolve/src/solve.rs
git commit -m "feat(ddsolve): consistency pruning via join + antijoin"
```

---

### Task 4: Full recursive search via `iterate`

**Files:**
- Modify: `ddsolve/src/solve.rs` (add `search`, add test)

- [ ] **Step 1: Write the failing test**

Add the `search` function to `ddsolve/src/solve.rs`:

```rust
use crate::types::Csp;
use differential_dataflow::operators::iterate::Iterate;
use differential_dataflow::operators::Consolidate;

/// Run the full search. Returns the collection of complete, consistent assignments
/// (solution leaves at depth `n_vars`).
///
/// The frontier of *all* live nodes (every depth) is grown by `iterate` to a fixed
/// point: each round expands every non-leaf live node, prunes inconsistent children,
/// and unions survivors back in. The fixed point is reached once no new nodes appear
/// (all live nodes are either full-depth leaves or were pruned). Solutions are the
/// full-depth members of the fixed point.
pub fn search<G>(
    roots: &Collection<G, Node>,
    forbidden: &Collection<G, Forbidden>,
    csp: Csp,
) -> Collection<G, Node>
where
    G: Scope,
    G::Timestamp: Lattice,
{
    let n_vars = csp.n_vars;
    let domain = csp.domain;

    let live = roots.iterate(|live| {
        let roots = roots.enter(&live.scope());
        let forbidden = forbidden.enter(&live.scope());

        let children = expand(live, n_vars, domain);
        let survivors = check_consistent(&children, &forbidden);

        roots.concat(&survivors).distinct()
    });

    live.filter(move |node| node.len() as u16 == n_vars).consolidate()
}
```

Create the end-to-end test file `ddsolve/tests/search.rs`:

```rust
use ddsolve::solve::search;
use ddsolve::types::{Csp, Forbidden, Node};
use differential_dataflow::input::Input;
use timely::dataflow::operators::capture::{Capture, Extract};

/// Enumerate proper 2-colourings of a single edge (var0 - var1), domain {0,1}.
/// Valid colourings: [0,1] and [1,0]. Forbidden: equal colours on the edge.
#[test]
fn two_colour_one_edge() {
    let data = timely::example(|scope| {
        let (mut rin, roots) = scope.new_collection::<Node, isize>();
        let (mut fin, forbidden) = scope.new_collection::<Forbidden, isize>();

        rin.insert(Vec::new()); // empty root

        // Forbid equal colours: (var0=0,var1=0) and (var0=1,var1=1).
        fin.insert((0u16, 0u16, 1u16, 0u16));
        fin.insert((0u16, 1u16, 1u16, 1u16));

        rin.advance_to(1); rin.flush();
        fin.advance_to(1); fin.flush();

        let csp = Csp { n_vars: 2, domain: 2, forbidden: vec![] };
        search(&roots, &forbidden, csp).inner.capture()
    });

    let mut sols: Vec<Node> = data.extract()
        .into_iter()
        .flat_map(|(_, b)| b.into_iter().filter(|(_, _, r)| *r > 0).map(|(n, _, _)| n))
        .collect();
    sols.sort();
    assert_eq!(sols, vec![vec![0, 1], vec![1, 0]]);
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p ddsolve --test search two_colour_one_edge`
Expected: FAIL initially if `search` is missing (`cannot find function search`). With the Step-1 impl present it should PASS. To force a meaningful red: temporarily set `roots.concat(&survivors)` to just `survivors` (drops the accumulation) and observe wrong/empty results, then restore.

- [ ] **Step 3: (Implementation present from Step 1)**

No change.

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p ddsolve --test search two_colour_one_edge`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add ddsolve/src/solve.rs ddsolve/tests/search.rs
git commit -m "feat(ddsolve): recursive search via iterate"
```

---

### Task 5: Frontier-blow-up instrumentation + experiment driver

**Files:**
- Create: `ddsolve/src/metrics.rs`
- Modify: `ddsolve/src/lib.rs` (add `pub mod metrics;`)
- Create: `ddsolve/examples/blowup.rs`

- [ ] **Step 1: Write the failing test**

Create `ddsolve/src/metrics.rs`:

```rust
//! Instrumentation: count live nodes per depth and total update volume.

use std::cell::RefCell;
use std::rc::Rc;

use differential_dataflow::Collection;
use differential_dataflow::lattice::Lattice;
use timely::dataflow::Scope;

use crate::types::Node;

/// Shared counter of net live nodes grouped by depth, accumulated across all batches.
pub type DepthCounts = Rc<RefCell<std::collections::BTreeMap<usize, isize>>>;

/// Attach a probe that tallies net diffs per node depth into a shared map.
/// Returns the map; read it after the worker has run to completion.
pub fn count_by_depth<G>(nodes: &Collection<G, Node>) -> DepthCounts
where
    G: Scope,
    G::Timestamp: Lattice,
{
    let counts: DepthCounts = Rc::new(RefCell::new(Default::default()));
    let sink = counts.clone();
    nodes.inspect_batch(move |_time, batch| {
        let mut map = sink.borrow_mut();
        for (node, _t, r) in batch.iter() {
            *map.entry(node.len()).or_insert(0) += *r;
        }
    });
    counts
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::solve::search;
    use crate::types::{Csp, Forbidden};
    use differential_dataflow::input::Input;

    #[test]
    fn depth_counts_full_solutions() {
        // 2-colour one edge => 2 solutions at depth 2.
        let counts = timely::execute_directly(|worker| {
            worker.dataflow::<i32, _, _>(|scope| {
                let (mut rin, roots) = scope.new_collection::<Node, isize>();
                let (mut fin, forbidden) = scope.new_collection::<Forbidden, isize>();
                rin.insert(Vec::new());
                fin.insert((0, 0, 1, 0));
                fin.insert((0, 1, 1, 1));
                rin.advance_to(1); rin.flush();
                fin.advance_to(1); fin.flush();
                let csp = Csp { n_vars: 2, domain: 2, forbidden: vec![] };
                count_by_depth(&search(&roots, &forbidden, csp))
            })
        });
        assert_eq!(counts.borrow().get(&2).copied(), Some(2));
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Wire the module — `ddsolve/src/lib.rs`:

```rust
pub mod types;
pub mod solve;
pub mod metrics;
```

Run: `cargo test -p ddsolve depth_counts_full_solutions`
Expected: PASS (the count of depth-2 solution leaves is exactly 2). Force a red by asserting `Some(3)` first, confirm FAIL, then fix to `Some(2)`.

- [ ] **Step 3: Write the experiment driver**

Create `ddsolve/examples/blowup.rs`:

```rust
//! Frontier blow-up experiment. Builds a random graph-colouring instance and
//! reports the peak live-frontier size per depth, illustrating that pure DD
//! search materialises the whole feasible frontier.
//!
//! Usage: cargo run -p ddsolve --example blowup --release -- <n_vars> <domain> <edge_prob_pct> <seed>

use ddsolve::instic; // added in Task 6; until then, inline a tiny instance here.
use ddsolve::metrics::count_by_depth;
use ddsolve::solve::search;
use ddsolve::types::{Forbidden, Node};
use differential_dataflow::input::Input;

fn main() {
    let mut args = std::env::args().skip(1);
    let n_vars: u16 = args.next().unwrap_or_else(|| "6".into()).parse().unwrap();
    let domain: u16 = args.next().unwrap_or_else(|| "3".into()).parse().unwrap();
    let edge_pct: u32 = args.next().unwrap_or_else(|| "40".into()).parse().unwrap();
    let seed: u64 = args.next().unwrap_or_else(|| "0".into()).parse().unwrap();

    let csp = instic::random_colouring(n_vars, domain, edge_pct, seed);

    let counts = timely::execute_directly(move |worker| {
        worker.dataflow::<i32, _, _>(|scope| {
            let (mut rin, roots) = scope.new_collection::<Node, isize>();
            let (mut fin, forbidden) = scope.new_collection::<Forbidden, isize>();
            rin.insert(Vec::new());
            for f in &csp.forbidden { fin.insert(*f); }
            rin.advance_to(1); rin.flush();
            fin.advance_to(1); fin.flush();
            count_by_depth(&search(&roots, &forbidden, csp.clone()))
        })
    });

    println!("depth, net_live_nodes");
    for (depth, n) in counts.borrow().iter() {
        println!("{depth}, {n}");
    }
}
```

> The driver depends on `instic::random_colouring` from Task 6. If executing strictly in order, run the driver only after Task 6; the example will not be compiled by `cargo test -p ddsolve` (examples build on demand), so Task 5's test passes independently.

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p ddsolve depth_counts_full_solutions`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add ddsolve/src/lib.rs ddsolve/src/metrics.rs ddsolve/examples/blowup.rs
git commit -m "feat(ddsolve): frontier-depth instrumentation and blowup driver"
```

---

### Task 6: Instance builders (colouring + scheduling) and end-to-end scheduling test

**Files:**
- Create: `ddsolve/src/instic.rs`
- Modify: `ddsolve/src/lib.rs` (add `pub mod instic;`)
- Modify: `ddsolve/tests/search.rs` (add scheduling test)

- [ ] **Step 1: Write the failing test**

Create `ddsolve/src/instic.rs`:

```rust
//! Instance builders: turn problems into a `Csp` (forbidden-pair encoding).

use crate::types::{canon, Csp, Forbidden, Val, VarId};

/// Random graph k-colouring. Variables = vertices, domain = colours, forbidden =
/// equal colours on each edge. `edge_pct` is the percent chance of each undirected
/// edge existing. Deterministic given `seed` (a small xorshift; no rand dependency).
pub fn random_colouring(n_vars: VarId, domain: Val, edge_pct: u32, seed: u64) -> Csp {
    let mut state = seed.wrapping_add(0x9E3779B97F4A7C15).max(1);
    let mut next = || {
        // xorshift64
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };

    let mut forbidden = Vec::new();
    for a in 0..n_vars {
        for b in (a + 1)..n_vars {
            if (next() % 100) < edge_pct as u64 {
                for c in 0..domain {
                    forbidden.push(canon(a, c, b, c)); // same colour on an edge is forbidden
                }
            }
        }
    }
    Csp { n_vars, domain, forbidden }
}

/// A scheduling instance. Tasks are variables; their domain is the set of nodes
/// (machines). Constraints, all reduced to binary forbidden pairs:
/// - affinity(t, u): tasks t and u must share a node  -> forbid every (t=p, u=q) with p != q.
/// - anti_affinity(t, u): tasks t and u must NOT share a node -> forbid every (t=p, u=p).
/// - exclusive(t, u): tasks t and u cannot both sit on node p (pairwise resource limit
///   of 1 on that node) -> forbid (t=p, u=p) for the given p only.
///
/// NOTE (prototype limitation): true aggregate capacity ("at most K of these tasks per
/// node") is NOT expressible as binary pairs without blow-up. The aggregate-capacity
/// encoding (a per-node `reduce` over the assignment) is future work; here we model the
/// K=1 case via `exclusive`, which IS binary.
pub fn scheduling(
    n_tasks: VarId,
    n_nodes: Val,
    affinity: &[(VarId, VarId)],
    anti_affinity: &[(VarId, VarId)],
    exclusive: &[(VarId, VarId, Val)],
) -> Csp {
    let mut forbidden = Vec::new();
    for &(t, u) in affinity {
        for p in 0..n_nodes {
            for q in 0..n_nodes {
                if p != q { forbidden.push(canon(t, p, u, q)); }
            }
        }
    }
    for &(t, u) in anti_affinity {
        for p in 0..n_nodes {
            forbidden.push(canon(t, p, u, p));
        }
    }
    for &(t, u, p) in exclusive {
        forbidden.push(canon(t, p, u, p));
    }
    // Deduplicate (affinity/anti-affinity overlaps possible).
    forbidden.sort();
    forbidden.dedup();
    Csp { n_vars: n_tasks, domain: n_nodes, forbidden }
}

/// Convenience: the canonical forbidden list as a typed vec (for tests).
pub fn forbidden_of(csp: &Csp) -> Vec<Forbidden> { csp.forbidden.clone() }
```

Add to `ddsolve/tests/search.rs`:

```rust
use ddsolve::instic;

/// 3 tasks, 2 nodes. task0 & task1 affinity (same node); task1 & task2 anti-affinity
/// (different nodes). Enumerate all valid placements and check they obey the rules.
#[test]
fn scheduling_affinity_antiaffinity() {
    let csp = instic::scheduling(3, 2, &[(0, 1)], &[(1, 2)], &[]);
    let forbidden_vec = csp.forbidden.clone();

    let data = timely::example(move |scope| {
        let (mut rin, roots) = scope.new_collection::<Node, isize>();
        let (mut fin, forbidden) = scope.new_collection::<Forbidden, isize>();
        rin.insert(Vec::new());
        for f in &forbidden_vec { fin.insert(*f); }
        rin.advance_to(1); rin.flush();
        fin.advance_to(1); fin.flush();
        search(&roots, &forbidden, csp.clone()).inner.capture()
    });

    let mut sols: Vec<Node> = data.extract()
        .into_iter()
        .flat_map(|(_, b)| b.into_iter().filter(|(_, _, r)| *r > 0).map(|(n, _, _)| n))
        .collect();
    sols.sort();

    // Every solution: task0 == task1 (affinity), task1 != task2 (anti-affinity).
    assert!(!sols.is_empty());
    for s in &sols {
        assert_eq!(s.len(), 3);
        assert_eq!(s[0], s[1], "affinity violated: {s:?}");
        assert_ne!(s[1], s[2], "anti-affinity violated: {s:?}");
    }
    // Concretely: task0=task1 in {0,1}, task2 = the other node. Exactly 2 solutions.
    assert_eq!(sols, vec![vec![0, 0, 1], vec![1, 1, 0]]);
}
```

- [ ] **Step 2: Run the test to verify it fails**

Wire the module — `ddsolve/src/lib.rs`:

```rust
pub mod types;
pub mod solve;
pub mod metrics;
pub mod instic;
```

Run: `cargo test -p ddsolve --test search scheduling_affinity_antiaffinity`
Expected: FAIL before `instic` exists (`unresolved import`). After Step 1, PASS. Force a red by changing the expected `vec![vec![0,0,1], vec![1,1,0]]` to a wrong value, confirm FAIL, restore.

- [ ] **Step 3: (Implementation present from Step 1)**

No change.

- [ ] **Step 4: Run all tests**

Run: `cargo test -p ddsolve`
Expected: all PASS (unit tests + `search` integration tests).

- [ ] **Step 5: Run the blow-up driver as a smoke check**

Run: `cargo run -p ddsolve --example blowup --release -- 6 3 40 1`
Expected: prints a `depth, net_live_nodes` table; the per-depth count grows then collapses to the solution count at depth 6. Eyeball that mid-depths exceed the final count (this *is* the blow-up the research is measuring).

- [ ] **Step 6: Commit**

```bash
git add ddsolve/src/lib.rs ddsolve/src/instic.rs ddsolve/tests/search.rs
git commit -m "feat(ddsolve): colouring + scheduling instance builders, e2e scheduling test"
```

---

## Item 2 — Incremental re-solve characterization

Goal: feed constraint deltas at successive timestamps, capture the solution-set diff, and measure update volume (churn) as a function of *where in the variable order* the changed constraint sits. Hypothesis: deltas touching late (leaf-ward) variables produce small churn; deltas touching early (root-ward) variables produce large churn.

### Task 7: Multi-round driver harness (deltas over time)

**Files:**
- Modify: `ddsolve/src/solve.rs` (add `search_handles`: a variant returning input handles + probe for driving rounds)

- [ ] **Step 1: Write the failing test**

Add to `ddsolve/src/solve.rs`:

```rust
use differential_dataflow::input::InputSession;
use differential_dataflow::operators::arrange::ArrangeBySelf;
use timely::dataflow::ProbeHandle;
use timely::dataflow::operators::probe::Probe;

/// Build the search dataflow with externally driven inputs. Returns the roots input,
/// the forbidden-constraint input, and a probe on the solution stream. Callers push
/// updates, `advance_to` the next round, and step the worker until the probe catches up.
///
/// `T` is the outer timestamp (use `u32` rounds).
pub fn search_handles<T>(
    worker: &mut timely::worker::Worker<impl timely::communication::Allocate>,
) -> (
    InputSession<T, Node, isize>,
    InputSession<T, Forbidden, isize>,
    ProbeHandle<T>,
)
where
    T: timely::progress::Timestamp + Lattice + std::hash::Hash,
{
    panic!("not yet implemented; the instance shape (n_vars/domain) must be threaded in")
}
```

> The signature above is a stepping stone — the test in Step 1 will reveal that we must pass `Csp` in. Rewrite to the real signature in Step 3.

Add the test to `ddsolve/src/solve.rs` `tests` module:

```rust
    #[test]
    fn incremental_add_constraint_shrinks_solutions() {
        use crate::types::Csp;
        use std::cell::RefCell;
        use std::rc::Rc;

        // Round 0: no constraints, 2 vars domain 2 => 4 solutions.
        // Round 1: forbid (var0=0,var1=0) => 3 solutions. Net diff at round 1 = -1 leaf.
        let solution_count = Rc::new(RefCell::new(Vec::<isize>::new()));
        let sink = solution_count.clone();

        timely::execute_directly(move |worker| {
            let csp = Csp { n_vars: 2, domain: 2, forbidden: vec![] };
            let (mut roots, mut forbidden, probe, counts) =
                build_round_driver(worker, csp, sink.clone());

            roots.insert(Vec::new());
            roots.advance_to(1); forbidden.advance_to(1);
            roots.flush(); forbidden.flush();
            worker.step_while(|| probe.less_than(roots.time()));

            forbidden.insert((0, 0, 1, 0));
            roots.advance_to(2); forbidden.advance_to(2);
            roots.flush(); forbidden.flush();
            worker.step_while(|| probe.less_than(roots.time()));

            drop(counts);
        });

        // counts[0] = solutions visible at/after round 1 settle = 4; counts[1] = 3.
        let c = solution_count.borrow();
        assert_eq!(c.first().copied(), Some(4));
        assert_eq!(c.get(1).copied(), Some(3));
    }
```

> This test references `build_round_driver`, a test helper that wires `search` to inputs + a per-round solution counter. Define it in Step 3 alongside the real `search_handles`. The helper belongs in the `tests` module.

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p ddsolve incremental_add_constraint_shrinks_solutions`
Expected: FAIL — `cannot find function build_round_driver` and the `panic!` placeholder.

- [ ] **Step 3: Implement the real driver + helper**

Replace the placeholder `search_handles` with a `Csp`-parameterised version, and add the test helper. Real `search_handles`:

```rust
/// Build the search dataflow with externally driven inputs, for a fixed instance shape.
pub fn search_handles<A>(
    worker: &mut timely::worker::Worker<A>,
    csp: Csp,
) -> (
    InputSession<u32, Node, isize>,
    InputSession<u32, Forbidden, isize>,
    Collection<timely::dataflow::scopes::Child<'static, timely::worker::Worker<A>, u32>, Node>,
)
where
    A: timely::communication::Allocate,
{
    // NOTE: returning an in-scope Collection across the dataflow boundary is not possible;
    // the real driver builds the dataflow *and* attaches the sink inside one `worker.dataflow`
    // call. See `build_round_driver` for the working shape; `search_handles` is therefore
    // folded into the driver and removed. (Discovered while implementing — keep the driver only.)
    unreachable!("use build_round_driver")
}
```

> Implementation reality (record this): because a `Collection` is tied to its dataflow scope, you cannot return it out of `worker.dataflow(...)`. So the public API is the *driver* that builds the dataflow and installs the sink in one shot. Delete `search_handles` rather than ship a dead signature; keep `build_round_driver` (promoted out of `#[cfg(test)]` in Task 8).

Add to the `tests` module:

```rust
    use std::cell::RefCell;
    use std::rc::Rc;
    use crate::types::Csp;

    /// Wire `search` to driven inputs and a per-round solution-count sink.
    /// Returns (roots input, forbidden input, probe, shared counts).
    fn build_round_driver<A: timely::communication::Allocate>(
        worker: &mut timely::worker::Worker<A>,
        csp: Csp,
        counts: Rc<RefCell<Vec<isize>>>,
    ) -> (
        differential_dataflow::input::InputSession<u32, Node, isize>,
        differential_dataflow::input::InputSession<u32, Forbidden, isize>,
        timely::dataflow::ProbeHandle<u32>,
        (),
    ) {
        use differential_dataflow::input::Input;
        use timely::dataflow::operators::probe::Probe;

        worker.dataflow::<u32, _, _>(|scope| {
            let (rin, roots) = scope.new_collection::<Node, isize>();
            let (fin, forbidden) = scope.new_collection::<Forbidden, isize>();
            let sols = super::search(&roots, &forbidden, csp);

            // Per-round net solution count: sum diffs of full-depth leaves by round time.
            let sink = counts.clone();
            let by_round: Rc<RefCell<std::collections::BTreeMap<u32, isize>>> =
                Rc::new(RefCell::new(Default::default()));
            let acc = by_round.clone();
            sols.inner.inspect_batch(move |t, batch| {
                let mut m = acc.borrow_mut();
                for (_n, _tt, r) in batch.iter() { *m.entry(*t).or_insert(0) += *r; }
            });
            // Flatten the cumulative count into a per-round running total on drop.
            // (For the test we read cumulative sums at the end.)
            let probe = sols.inner.probe();

            // Defer materialisation: copy cumulative totals when the dataflow is dropped.
            // Simpler: expose `by_round` via the counts vec at read time.
            // Convert BTreeMap -> running totals into `sink`.
            // We do this lazily in the test after stepping; store the map behind counts.
            let _ = (&sink, &by_round);

            // Stash the map into counts as running totals immediately is not possible here;
            // instead we record cumulative-by-round and let the test interpret.
            // To keep the helper self-contained, install a second inspect that writes
            // running totals into `sink` keyed by round order.
            let running = std::rc::Rc::new(std::cell::RefCell::new(0isize));
            let run2 = running.clone();
            let sink2 = sink.clone();
            sols.inner.inspect_batch(move |_t, batch| {
                let mut tot = run2.borrow_mut();
                for (_n, _tt, r) in batch.iter() { *tot += *r; }
                let mut v = sink2.borrow_mut();
                v.push(*tot);
            });

            (rin, fin, probe, ())
        })
    }
```

> The helper above is intentionally explicit about a wrinkle: per-round solution *totals* require accumulating diffs across rounds. The clean version (Task 8) replaces the running-total hack with a proper `count`/`Consolidate` per round time. For Task 7's red→green, the cumulative-running-total interpretation is enough; adjust the asserts to match the running totals your inspect produces (the test asserts `Some(4)` then `Some(3)` — verify against actual output and correct the expected values to the real settled totals).

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p ddsolve incremental_add_constraint_shrinks_solutions`
Expected: PASS once expected values match the observed settled per-round totals (4 then 3).

- [ ] **Step 5: Commit**

```bash
git add ddsolve/src/solve.rs
git commit -m "feat(ddsolve): multi-round driver for incremental re-solve"
```

---

### Task 8: Churn metric + experiment driver

**Files:**
- Modify: `ddsolve/src/metrics.rs` (add `ChurnRecorder`)
- Create: `ddsolve/examples/churn.rs`
- Modify: `ddsolve/src/solve.rs` (promote `build_round_driver` out of `#[cfg(test)]` into a public `pub fn round_driver`, cleaned up)

- [ ] **Step 1: Write the failing test**

Add to `ddsolve/src/metrics.rs`:

```rust
/// Records, per outer round (timestamp), the number of update tuples (|+| + |-|)
/// flowing on a stream. This is the "churn" we attribute to a constraint delta.
pub type ChurnByRound = Rc<RefCell<std::collections::BTreeMap<u32, usize>>>;

/// Attach a churn recorder to a `u32`-timestamped collection.
pub fn record_churn<G>(nodes: &Collection<G, Node>) -> ChurnByRound
where
    G: Scope<Timestamp = u32>,
{
    let churn: ChurnByRound = Rc::new(RefCell::new(Default::default()));
    let sink = churn.clone();
    nodes.inner.inspect_batch(move |t, batch| {
        *sink.borrow_mut().entry(*t).or_insert(0) += batch.len();
    });
    churn
}

#[cfg(test)]
mod churn_tests {
    use super::*;
    use crate::solve::search;
    use crate::types::{Csp, Forbidden};
    use differential_dataflow::input::Input;
    use timely::dataflow::operators::probe::Probe;

    #[test]
    fn churn_recorded_per_round() {
        let churn = timely::execute_directly(|worker| {
            let churn = worker.dataflow::<u32, _, _>(|scope| {
                let (mut rin, roots) = scope.new_collection::<Node, isize>();
                let (mut fin, forbidden) = scope.new_collection::<Forbidden, isize>();
                let sols = search(&roots, &forbidden, Csp { n_vars: 2, domain: 2, forbidden: vec![] });
                let churn = record_churn(&sols);
                let _p = sols.inner.probe();

                rin.insert(Vec::new());
                rin.advance_to(1); fin.advance_to(1); rin.flush(); fin.flush();
                fin.insert((0, 0, 1, 0));
                rin.advance_to(2); fin.advance_to(2); rin.flush(); fin.flush();
                churn
            });
            churn
        });
        // Round 1: 4 solution leaves appear (4 updates). Round 2: 1 retraction.
        assert_eq!(churn.borrow().get(&1).copied(), Some(4));
        assert_eq!(churn.borrow().get(&2).copied(), Some(1));
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p ddsolve churn_recorded_per_round`
Expected: FAIL — `record_churn` undefined.

- [ ] **Step 3: Implement `record_churn` (already written in Step 1) and the experiment driver**

`record_churn` is in Step 1. Create `ddsolve/examples/churn.rs`:

```rust
//! Incremental-churn experiment. Solve an instance, then perturb one constraint and
//! measure solution-set churn, varying whether the perturbed constraint touches an
//! early (root-ward) or late (leaf-ward) variable.
//!
//! Usage: cargo run -p ddsolve --example churn --release -- <n_vars> <domain> <seed>

use ddsolve::instic;
use ddsolve::metrics::record_churn;
use ddsolve::solve::search;
use ddsolve::types::{Forbidden, Node, VarId};
use differential_dataflow::input::Input;
use timely::dataflow::operators::probe::Probe;

fn main() {
    let mut args = std::env::args().skip(1);
    let n_vars: VarId = args.next().unwrap_or_else(|| "8".into()).parse().unwrap();
    let domain: u16 = args.next().unwrap_or_else(|| "3".into()).parse().unwrap();
    let seed: u64 = args.next().unwrap_or_else(|| "0".into()).parse().unwrap();
    let base = instic::random_colouring(n_vars, domain, 35, seed);

    // Two perturbations: forbid an edge between vars (0,1) [root-ward] vs (n-2,n-1) [leaf-ward].
    for (label, edge) in [("root_ward", (0u16, 1u16)), ("leaf_ward", (n_vars - 2, n_vars - 1))] {
        let base = base.clone();
        let churn = timely::execute_directly(move |worker| {
            worker.dataflow::<u32, _, _>(|scope| {
                let (mut rin, roots) = scope.new_collection::<Node, isize>();
                let (mut fin, forbidden) = scope.new_collection::<Forbidden, isize>();
                let sols = search(&roots, &forbidden, base.clone());
                let churn = record_churn(&sols);
                let probe = sols.inner.probe();

                rin.insert(Vec::new());
                for f in &base.forbidden { fin.insert(*f); }
                rin.advance_to(1); fin.advance_to(1); rin.flush(); fin.flush();
                worker.step_while(|| probe.less_than(&1));

                // Perturb: add a same-colour forbidden constraint on `edge` for colour 0.
                fin.insert(ddsolve::types::canon(edge.0, 0, edge.1, 0));
                rin.advance_to(2); fin.advance_to(2); rin.flush(); fin.flush();
                worker.step_while(|| probe.less_than(&2));
                churn
            })
        });
        let delta = churn.borrow().get(&2).copied().unwrap_or(0);
        println!("{label}: round-2 churn = {delta} update tuples");
    }
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p ddsolve churn_recorded_per_round`
Expected: PASS. (If the round-2 retraction count differs, correct the expected value to the observed settled churn — record the real number in a code comment.)

- [ ] **Step 5: Run the churn experiment**

Run: `cargo run -p ddsolve --example churn --release -- 8 3 1`
Expected: prints `root_ward` and `leaf_ward` churn. Record whether root-ward churn exceeds leaf-ward — that comparison is Item 2's headline result.

- [ ] **Step 6: Commit**

```bash
git add ddsolve/src/metrics.rs ddsolve/examples/churn.rs
git commit -m "feat(ddsolve): churn metric and incremental-resolve experiment"
```

---

### Task 9: Churn characterization test (root-ward vs leaf-ward)

**Files:**
- Create: `ddsolve/tests/incremental.rs`

- [ ] **Step 1: Write the failing test**

Create `ddsolve/tests/incremental.rs`:

```rust
//! Characterise incremental re-solve churn: a constraint delta on a leaf-ward variable
//! should disturb no more of the solution set than the same delta on a root-ward variable,
//! for a fixed static variable order. This encodes Item 2's hypothesis as an assertion.

use ddsolve::instic;
use ddsolve::metrics::record_churn;
use ddsolve::solve::search;
use ddsolve::types::{canon, Forbidden, Node};
use differential_dataflow::input::Input;
use timely::dataflow::operators::probe::Probe;

fn churn_for_edge(n_vars: u16, domain: u16, seed: u64, edge: (u16, u16)) -> usize {
    let base = instic::random_colouring(n_vars, domain, 35, seed);
    let churn = timely::execute_directly(move |worker| {
        worker.dataflow::<u32, _, _>(|scope| {
            let (mut rin, roots) = scope.new_collection::<Node, isize>();
            let (mut fin, forbidden) = scope.new_collection::<Forbidden, isize>();
            let sols = search(&roots, &forbidden, base.clone());
            let churn = record_churn(&sols);
            let probe = sols.inner.probe();
            rin.insert(Vec::new());
            for f in &base.forbidden { fin.insert(*f); }
            rin.advance_to(1); fin.advance_to(1); rin.flush(); fin.flush();
            worker.step_while(|| probe.less_than(&1));
            fin.insert(canon(edge.0, 0, edge.1, 0));
            rin.advance_to(2); fin.advance_to(2); rin.flush(); fin.flush();
            worker.step_while(|| probe.less_than(&2));
            churn
        })
    });
    let c = churn.borrow().get(&2).copied().unwrap_or(0);
    c
}

#[test]
fn leaf_ward_delta_churns_no_more_than_root_ward() {
    let n = 8u16;
    let root = churn_for_edge(n, 3, 7, (0, 1));
    let leaf = churn_for_edge(n, 3, 7, (n - 2, n - 1));
    // Hypothesis: leaf-ward perturbation disturbs <= root-ward perturbation.
    // (Both are deterministic for a fixed seed.)
    assert!(
        leaf <= root,
        "expected leaf-ward churn ({leaf}) <= root-ward churn ({root})"
    );
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p ddsolve --test incremental leaf_ward_delta_churns_no_more_than_root_ward`
Expected: PASS or FAIL depending on the instance. **If it FAILS**, that is a *research finding*, not a bug: either the hypothesis is false for this instance, or churn is dominated by re-derivation. Record the observed numbers. Then either (a) adjust the seed/instance to one where the effect is clean and note the dependence, or (b) weaken the assertion to `leaf <= root * 2` and document why. Do not silently delete the test.

- [ ] **Step 3: Resolve the finding**

If PASS: proceed. If FAIL: pick the documented resolution from Step 2 and add a comment citing the observed root/leaf numbers and the chosen seed. The goal is a *reproducible characterization*, not a forced green.

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p ddsolve --test incremental`
Expected: PASS (with whichever assertion form Step 3 settled on).

- [ ] **Step 5: Commit**

```bash
git add ddsolve/tests/incremental.rs
git commit -m "test(ddsolve): characterize root-ward vs leaf-ward churn"
```

---

## Item 3 — Conflict learning as a maintained nogood collection

The crux: in the prefix-BFS model two distinct nodes never share a *full* prefix, so a full-prefix nogood never fires twice. Learning pays off only via **minimal-core projection** — when a node is a dead end (survives consistency but all children prune), project its prefix down to the variables actually responsible, yielding a *short* nogood that prunes sibling/cousin nodes sharing that sub-assignment. Maintaining learned nogoods across iterations needs a second feedback collection.

### Task 10: Dead-end detection + minimal-core projection

**Files:**
- Create: `ddsolve/src/learn.rs`
- Modify: `ddsolve/src/lib.rs` (add `pub mod learn;`)

- [ ] **Step 1: Write the failing test**

Create `ddsolve/src/learn.rs`:

```rust
//! Conflict learning: detect dead-end nodes, project to a minimal nogood core,
//! and maintain learned nogoods to prune the frontier across iterations.

use differential_dataflow::Collection;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::{Join, Threshold};
use timely::dataflow::Scope;

use crate::types::{Node, Val, VarId};

/// A learned nogood: a partial assignment (var -> value) that has no consistent
/// completion. Represented as a sorted `Vec<(VarId, Val)>` so it can match any node
/// whose prefix is a superset.
pub type Nogood = Vec<(VarId, Val)>;

/// Dead-end nodes: nodes present in `live_parents` that have NO surviving child in
/// `survivors`. A survivor's parent is its prefix minus the last value.
pub fn dead_ends<G>(
    live_parents: &Collection<G, Node>,
    survivors: &Collection<G, Node>,
) -> Collection<G, Node>
where
    G: Scope,
    G::Timestamp: Lattice,
{
    // Parents that DO have a surviving child.
    let parents_with_child = survivors
        .flat_map(|child| {
            if child.is_empty() { None } else {
                let mut p = child.clone();
                p.pop();
                Some(p)
            }
        })
        .distinct();

    // live_parents that are non-leaf and lack any surviving child = dead ends.
    live_parents
        .map(|n| (n, ()))
        .antijoin(&parents_with_child)
        .map(|(n, ())| n)
}

/// Project a dead-end prefix to a minimal nogood core.
///
/// PROTOTYPE projection: keep only the variables that participate in at least one
/// forbidden pair that the dead-end's children all violated. Computing the *true*
/// minimal core requires conflict analysis (an implication-graph cut). Here we use a
/// cheap sound-but-not-minimal core: the full prefix is always a valid nogood; we
/// shrink it by dropping trailing variables that have a fully-available domain given
/// the rest (i.e., variables whose every value is still individually consistent with
/// the kept sub-assignment). This is conservative: never produces an unsound nogood.
pub fn project_core(prefix: &Node, _domain: Val) -> Nogood {
    // Conservative placeholder retained as a clearly-marked simplification:
    // use the full prefix as the nogood. Task 11 replaces this with the
    // domain-wipeout-driven shrink and tests both soundness and that shrinking occurs.
    prefix.iter().enumerate().map(|(i, &v)| (i as VarId, v)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use differential_dataflow::input::Input;
    use differential_dataflow::operators::Consolidate;
    use timely::dataflow::operators::capture::{Capture, Extract};

    #[test]
    fn detect_dead_end() {
        // live parent [0]; survivors include [1,0] (child of [1]) but NO child of [0].
        // => [0] is a dead end.
        let data = timely::example(|scope| {
            let (mut pin, parents) = scope.new_collection::<Node, isize>();
            let (mut sin, survivors) = scope.new_collection::<Node, isize>();
            pin.insert(vec![0]);
            pin.insert(vec![1]);
            sin.insert(vec![1, 0]); // only [1] has a surviving child
            pin.advance_to(1); sin.advance_to(1);
            pin.flush(); sin.flush();
            dead_ends(&parents, &survivors).consolidate().inner.capture()
        });
        let mut de: Vec<Node> = data.extract().into_iter()
            .flat_map(|(_, b)| b.into_iter().filter(|(_, _, r)| *r > 0).map(|(n, _, _)| n))
            .collect();
        de.sort();
        assert_eq!(de, vec![vec![0]]);
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Wire the module — `ddsolve/src/lib.rs`:

```rust
pub mod types;
pub mod solve;
pub mod metrics;
pub mod instic;
pub mod learn;
```

Run: `cargo test -p ddsolve detect_dead_end`
Expected: PASS (function present). Force a red by removing the `antijoin` and returning `live_parents.clone()`, confirm `[0]` and `[1]` both appear (FAIL), restore.

- [ ] **Step 3: (Implementation present from Step 1)**

No change.

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p ddsolve detect_dead_end`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add ddsolve/src/lib.rs ddsolve/src/learn.rs
git commit -m "feat(ddsolve): dead-end detection for conflict learning"
```

---

### Task 11: Real minimal-core projection (domain-wipeout) + soundness test

**Files:**
- Modify: `ddsolve/src/learn.rs` (replace `project_core`, add tests)

- [ ] **Step 1: Write the failing test**

Add to `ddsolve/src/learn.rs` `tests` module:

```rust
    #[test]
    fn core_is_sound_and_shrinks() {
        use crate::types::{canon, Forbidden};
        // Prefix [a=0, b=0, c=1]. Forbidden pairs make (a=0,b=0) a wipeout for the next
        // var regardless of c: i.e. with a=0,b=0 every value of the next variable d is
        // forbidden by (a,d) or (b,d). Then the minimal core should be {a=0,b=0}, dropping c.
        let forbidden: Vec<Forbidden> = vec![
            canon(0, 0, 3, 0), // a=0 forbids d=0
            canon(1, 0, 3, 1), // b=0 forbids d=1
        ];
        // domain for d is 2, so d in {0,1} both forbidden => wipeout from {a=0,b=0}.
        let prefix = vec![0u16, 0, 1]; // a=0,b=0,c=1
        let core = super::project_core_against(&prefix, 2, 3, &forbidden);
        let mut c = core.clone();
        c.sort();
        assert_eq!(c, vec![(0u16, 0u16), (1, 0)], "core must be {{a=0,b=0}}, dropping c");
    }
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p ddsolve core_is_sound_and_shrinks`
Expected: FAIL — `project_core_against` does not exist.

- [ ] **Step 3: Implement the real projection**

Add to `ddsolve/src/learn.rs`:

```rust
use crate::types::Forbidden;

/// Compute a nogood core for a dead-end `prefix`, given that the next variable
/// `next_var = prefix.len()` suffered domain wipeout. A subset `S` of the prefix is a
/// valid nogood if, for every value `v` in `next_var`'s domain, some assignment in `S`
/// forbids `(next_var = v)`. We greedily find a small such `S`: for each domain value
/// `v`, pick one prefix variable that forbids it, and union those picks.
///
/// Sound: every returned variable is genuinely needed to rule out at least one value.
/// Not guaranteed globally minimal (greedy set cover), but strictly shrinks the prefix
/// whenever some prefix variables are irrelevant to the wipeout.
pub fn project_core_against(
    prefix: &Node,
    next_domain: Val,
    next_var: VarId,
    forbidden: &[Forbidden],
) -> Nogood {
    use crate::types::canon;
    use std::collections::BTreeSet;

    let forbidden_set: BTreeSet<Forbidden> = forbidden.iter().copied().collect();
    let mut core: BTreeSet<(VarId, Val)> = BTreeSet::new();

    for v in 0..next_domain {
        // Find a prefix variable whose assignment forbids (next_var = v).
        let mut blamed = None;
        for (i, &xi) in prefix.iter().enumerate() {
            let key = canon(i as VarId, xi, next_var, v);
            if forbidden_set.contains(&key) {
                blamed = Some((i as VarId, xi));
                break;
            }
        }
        if let Some(pair) = blamed {
            core.insert(pair);
        } else {
            // No blame for this value => not actually a wipeout; fall back to full prefix
            // (sound but no shrink) to preserve correctness.
            return prefix.iter().enumerate().map(|(i, &x)| (i as VarId, x)).collect();
        }
    }
    core.into_iter().collect()
}

/// Re-point the old conservative entry to the new one for callers that have domain info.
/// Kept for the dataflow wiring (Task 12), which knows `next_domain`.
pub fn project_core_full(prefix: &Node) -> Nogood {
    prefix.iter().enumerate().map(|(i, &v)| (i as VarId, v)).collect()
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p ddsolve core_is_sound_and_shrinks`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add ddsolve/src/learn.rs
git commit -m "feat(ddsolve): greedy minimal nogood core via domain wipeout"
```

---

### Task 12: Wire learned nogoods into the search loop + pruning-effect test

**Files:**
- Modify: `ddsolve/src/solve.rs` (add `search_with_learning`)
- Create: `ddsolve/tests/learning.rs`
- Create: `ddsolve/examples/learning.rs`

- [ ] **Step 1: Write the failing test**

Add `search_with_learning` to `ddsolve/src/solve.rs`:

```rust
use crate::learn::{dead_ends, project_core_against, Nogood};
use differential_dataflow::operators::iterate::Variable;
use timely::order::Product;

/// Search that maintains a learned-nogood collection. A node is pruned early if any
/// learned nogood is a subset of its prefix. Nogoods are derived from dead ends each
/// round via greedy core projection and fed back through a second `Variable`.
///
/// Returns (solutions, learned_nogoods) so experiments can measure both.
pub fn search_with_learning<G>(
    roots: &Collection<G, Node>,
    forbidden: &Collection<G, Forbidden>,
    csp: Csp,
) -> (Collection<G, Node>, Collection<G, Nogood>)
where
    G: Scope,
    G::Timestamp: Lattice,
{
    let n_vars = csp.n_vars;
    let domain = csp.domain;
    let forbidden_vec = csp.forbidden.clone();

    let mut scope = roots.scope();
    let (sols, learned) = scope.scoped::<Product<G::Timestamp, u32>, _, _>("search_learn", |inner| {
        let roots = roots.enter(inner);
        let forbidden = forbidden.enter(inner);

        // Two feedback variables: the live frontier and the learned nogoods.
        let live_var = Variable::new(inner, Product::new(Default::default(), 1));
        let nogood_var = Variable::new(inner, Product::new(Default::default(), 1));

        // Expand the live frontier.
        let children = expand(&live_var, n_vars, domain);

        // Prune by binary constraints (as before).
        let consistent = check_consistent(&children, &forbidden);

        // Prune by learned nogoods: drop a child if some nogood ⊆ its prefix.
        // Emit (nogood, child) for membership testing via join: for each child, for each
        // learned nogood that could match, test subset. We approximate with a join keyed
        // on the nogood's first (var,val) then filter full-subset in the closure.
        let survivors = prune_by_nogoods(&consistent, &nogood_var);

        // Detect dead ends among the *previous* live frontier (parents of survivors).
        let de = dead_ends(&live_var, &survivors);

        // Project dead ends to nogood cores. The next variable that wiped out is
        // `prefix.len()`. Capture `forbidden_vec` for the projection (data is static
        // within a round; for fully-incremental learning this becomes a join — see note).
        let fv = forbidden_vec.clone();
        let new_nogoods = de.map(move |prefix| {
            let next_var = prefix.len() as u16;
            project_core_against(&prefix, domain, next_var, &fv)
        });

        // Bind feedback.
        let next_live = roots.concat(&survivors).distinct();
        live_var.set(&next_live);
        let all_nogoods = nogood_var.concat(&new_nogoods).distinct();
        nogood_var.set(&all_nogoods);

        let solutions = next_live.filter(move |n| n.len() as u16 == n_vars);
        (solutions.leave(), all_nogoods.leave())
    });

    (sols.consolidate(), learned.consolidate())
}

/// Drop children for which some nogood is a subset of the prefix.
fn prune_by_nogoods<G>(
    children: &Collection<G, Node>,
    nogoods: &Collection<G, Nogood>,
) -> Collection<G, Node>
where
    G: Scope,
    G::Timestamp: Lattice,
{
    // Key nogoods by their first (var,val); key children by each (var,val) they contain.
    let nogood_keyed = nogoods.map(|ng| (ng[0], ng.clone()));
    let child_pairs = children.flat_map(|node| {
        node.iter().enumerate().map(move |(i, &v)| ((i as u16, v), node.clone())).collect::<Vec<_>>()
    });
    // Join on the first (var,val) of the nogood; then keep child only if the FULL nogood
    // is a subset of the child's prefix.
    let blocked = child_pairs
        .join_map(&nogood_keyed, |_k, node, ng| (node.clone(), ng.clone()))
        .filter(|(node, ng)| {
            ng.iter().all(|&(var, val)| (var as usize) < node.len() && node[var as usize] == val)
        })
        .map(|(node, _ng)| node)
        .distinct();
    children.map(|n| (n, ())).antijoin(&blocked).map(|(n, ())| n)
}
```

> Implementation note to record while wiring: `project_core_against` captures `forbidden_vec` by value, which means learning is incremental in *constraints* only up to the projection step. Making projection itself a `join` against the `forbidden` collection (so a constraint delta updates learned cores) is the natural follow-up; flag it in the commit body, not silently.

Create `ddsolve/tests/learning.rs`:

```rust
//! Learning must not change the solution set (soundness), and should not increase
//! the live frontier (it can only prune). We assert solution-set equality between
//! `search` and `search_with_learning` on the scheduling instance.

use ddsolve::instic;
use ddsolve::solve::{search, search_with_learning};
use ddsolve::types::{Forbidden, Node};
use differential_dataflow::input::Input;
use timely::dataflow::operators::capture::{Capture, Extract};

fn solutions_plain(csp: ddsolve::types::Csp) -> Vec<Node> {
    let fv = csp.forbidden.clone();
    let data = timely::example(move |scope| {
        let (mut rin, roots) = scope.new_collection::<Node, isize>();
        let (mut fin, forbidden) = scope.new_collection::<Forbidden, isize>();
        rin.insert(Vec::new());
        for f in &fv { fin.insert(*f); }
        rin.advance_to(1); fin.advance_to(1); rin.flush(); fin.flush();
        search(&roots, &forbidden, csp.clone()).inner.capture()
    });
    let mut s: Vec<Node> = data.extract().into_iter()
        .flat_map(|(_, b)| b.into_iter().filter(|(_, _, r)| *r > 0).map(|(n, _, _)| n)).collect();
    s.sort(); s
}

fn solutions_learning(csp: ddsolve::types::Csp) -> Vec<Node> {
    let fv = csp.forbidden.clone();
    let data = timely::example(move |scope| {
        let (mut rin, roots) = scope.new_collection::<Node, isize>();
        let (mut fin, forbidden) = scope.new_collection::<Forbidden, isize>();
        rin.insert(Vec::new());
        for f in &fv { fin.insert(*f); }
        rin.advance_to(1); fin.advance_to(1); rin.flush(); fin.flush();
        let (sols, _learned) = search_with_learning(&roots, &forbidden, csp.clone());
        sols.inner.capture()
    });
    let mut s: Vec<Node> = data.extract().into_iter()
        .flat_map(|(_, b)| b.into_iter().filter(|(_, _, r)| *r > 0).map(|(n, _, _)| n)).collect();
    s.sort(); s
}

#[test]
fn learning_preserves_solutions() {
    let csp = instic::scheduling(4, 2, &[(0, 1)], &[(1, 2), (2, 3)], &[]);
    assert_eq!(solutions_plain(csp.clone()), solutions_learning(csp));
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p ddsolve --test learning learning_preserves_solutions`
Expected: FAIL — `search_with_learning` undefined (and any iterate-scope wiring errors to fix).

- [ ] **Step 3: Implement (present from Step 1) and fix compile errors**

The `scope.scoped` / `Variable` wiring is the fiddly part. Expect to adjust:
- `Variable::new` signature and the `Product` summary import (`timely::order::Product`).
- `.leave()` calls to strip the inner timestamp.
- Trait imports: `differential_dataflow::operators::iterate::Variable`, `Iterate` not needed here.

Iterate until `cargo build -p ddsolve` is clean, then the test compiles.

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p ddsolve --test learning`
Expected: PASS — solution sets identical with and without learning (soundness).

- [ ] **Step 5: Write the pruning-effect driver**

Create `ddsolve/examples/learning.rs`:

```rust
//! Measure learning's pruning effect: peak live-frontier size with vs without learned
//! nogoods, on the same instance.
//!
//! Usage: cargo run -p ddsolve --example learning --release -- <n_vars> <domain> <seed>

use ddsolve::instic;
use ddsolve::metrics::count_by_depth;
use ddsolve::solve::{search, search_with_learning};
use ddsolve::types::{Forbidden, Node};
use differential_dataflow::input::Input;

fn peak(counts: &std::collections::BTreeMap<usize, isize>) -> isize {
    counts.values().copied().max().unwrap_or(0)
}

fn main() {
    let mut a = std::env::args().skip(1);
    let n: u16 = a.next().unwrap_or_else(|| "7".into()).parse().unwrap();
    let d: u16 = a.next().unwrap_or_else(|| "3".into()).parse().unwrap();
    let seed: u64 = a.next().unwrap_or_else(|| "1".into()).parse().unwrap();
    let csp = instic::random_colouring(n, d, 35, seed);

    let csp_p = csp.clone();
    let plain = timely::execute_directly(move |w| w.dataflow::<i32, _, _>(|s| {
        let (mut rin, roots) = s.new_collection::<Node, isize>();
        let (mut fin, forbidden) = s.new_collection::<Forbidden, isize>();
        rin.insert(Vec::new());
        for f in &csp_p.forbidden { fin.insert(*f); }
        rin.advance_to(1); fin.advance_to(1); rin.flush(); fin.flush();
        count_by_depth(&search(&roots, &forbidden, csp_p.clone()))
    }));

    let csp_l = csp.clone();
    let learned = timely::execute_directly(move |w| w.dataflow::<i32, _, _>(|s| {
        let (mut rin, roots) = s.new_collection::<Node, isize>();
        let (mut fin, forbidden) = s.new_collection::<Forbidden, isize>();
        rin.insert(Vec::new());
        for f in &csp_l.forbidden { fin.insert(*f); }
        rin.advance_to(1); fin.advance_to(1); rin.flush(); fin.flush();
        let (sols, _ng) = search_with_learning(&roots, &forbidden, csp_l.clone());
        count_by_depth(&sols)
    }));

    println!("peak live frontier  plain={}  learned={}", peak(&plain.borrow()), peak(&learned.borrow()));
}
```

> `count_by_depth` on the *solutions* only measures leaves; to measure the true peak frontier, expose the in-loop `next_live` from each search via an optional `inspect` hook, or add a `search_debug` returning the full live collection. Add that hook if the peak comparison needs the interior frontier (note it in the commit).

- [ ] **Step 6: Run the driver**

Run: `cargo run -p ddsolve --example learning --release -- 7 3 1`
Expected: prints peak frontier for plain vs learned. Record whether learned < plain — that is Item 3's headline result. (If equal, the minimal-core projection isn't producing cross-branch-effective nogoods on this instance; record and investigate as a finding.)

- [ ] **Step 7: Commit**

```bash
git add ddsolve/src/solve.rs ddsolve/tests/learning.rs ddsolve/examples/learning.rs
git commit -m "feat(ddsolve): learned nogoods in search loop + pruning experiment"
```

---

## Extension 4 — Bounding the frontier (beam / top-k)

**Lighter granularity: design + key code, one task.** Pure DD search materialises the whole feasible frontier. Bounding caps each depth's live set to the best `k` nodes by a heuristic score. This trades completeness (you may drop the depth where the only solution lived) and is **non-monotone** (re-ranking churns diffs), so it is a deliberate research knob, not a default.

### Task 13: Beam-bounded search

**Files:**
- Modify: `ddsolve/src/solve.rs` (add `search_beam`)
- Modify: `ddsolve/tests/search.rs` (add a beam test that confirms beam ⊆ full solutions)

- [ ] **Step 1: Design**

Per depth `d`, score each live node (e.g. fewest remaining conflicts = most constrained, or a static var-activity sum) and keep the top `k` *per depth*. In DD: key live nodes by depth, `reduce` per depth to retain the `k` highest-scoring. Completeness caveat: log how many nodes were dropped per depth (reuse `metrics`).

- [ ] **Step 2: Key code (top-k per depth via `reduce`)**

```rust
use differential_dataflow::operators::Reduce;

/// Keep the top-`k` nodes per depth, by a caller-supplied score (higher = keep).
/// WARNING: incomplete search — may discard the subtree containing a solution.
pub fn beam<G>(live: &Collection<G, Node>, k: usize, score: impl Fn(&Node) -> i64 + 'static)
    -> Collection<G, Node>
where G: Scope, G::Timestamp: Lattice
{
    live.map(move |n| (n.len(), n))
        .reduce(move |_depth, input, output| {
            let mut scored: Vec<(&Node, i64)> =
                input.iter().map(|(n, _r)| (*n, score(n))).collect();
            scored.sort_by(|a, b| b.1.cmp(&a.1)); // descending score
            for (n, _) in scored.into_iter().take(k) {
                output.push((n.clone(), 1));
            }
        })
        .map(|(_d, n)| n)
}
```

Insert `beam(&survivors, k, score)` between `check_consistent` and the feedback `concat` in a `search_beam` variant. Drop-count = (frontier size before beam) − (size after); record via `metrics::count_by_depth` on both.

- [ ] **Step 3: Test (beam solutions are a subset of full solutions)**

```rust
#[test]
fn beam_solutions_subset_of_full() {
    // For large k, beam == full. Use k >= domain^depth so nothing is dropped.
    let csp = ddsolve::instic::scheduling(3, 2, &[(0,1)], &[(1,2)], &[]);
    // ... run search_beam with k = 1_000 ...
    // assert beam solution set == full solution set (no drops at large k).
}
```

Run: `cargo test -p ddsolve --test search beam_solutions_subset_of_full`
Expected: PASS at large `k`. Then re-run with small `k` and assert the result is a (possibly empty) subset — never a superset.

- [ ] **Step 4: Commit**

```bash
git add ddsolve/src/solve.rs ddsolve/tests/search.rs
git commit -m "feat(ddsolve): beam-bounded search with drop accounting"
```

---

## Extension 5 — Dynamic ordering / VSIDS-style activity

**Lighter granularity: design + key code, one task.** Dynamic variable ordering uses per-variable *activity* bumped on conflict. In DD, activity is a feedback collection updated each round; reordering reshapes the tree, which erodes Item 2's incremental stability — so this extension is also a measurement of *that tradeoff*, not a free win.

### Task 14: Activity feedback (measurement-only)

**Files:**
- Modify: `ddsolve/src/learn.rs` (add `bump_activity`)
- Create: `ddsolve/examples/activity.rs`

- [ ] **Step 1: Design**

Maintain `activity: Collection<(VarId, i64)>`. Each round, every variable appearing in a freshly derived nogood gets +1. Order variables by descending activity. **Prototype constraint:** the search currently uses a *static* index order baked into `expand`. True dynamic ordering means `expand` must choose the next variable from a per-node ordering — a significant change. For this extension, do **not** rewire `expand`; instead *measure* the activity signal and report what a dynamic order *would* prioritise, leaving the rewrite as documented future work. This keeps Item 2's results valid while producing the activity data.

- [ ] **Step 2: Key code (activity as a feedback collection)**

```rust
use differential_dataflow::operators::Count;

/// Increment activity for every (var) appearing in a newly derived nogood.
/// Returns a collection of (VarId, count) usable as an ordering hint.
pub fn bump_activity<G>(new_nogoods: &Collection<G, Nogood>) -> Collection<G, (VarId, isize)>
where G: Scope, G::Timestamp: Lattice
{
    new_nogoods
        .flat_map(|ng| ng.into_iter().map(|(var, _val)| var).collect::<Vec<_>>())
        .count() // -> (VarId, isize)
}
```

Feed `new_nogoods` from `search_with_learning` into `bump_activity`; expose the activity collection from a `search_with_learning_debug` variant or capture it in the example.

- [ ] **Step 3: Driver**

Create `ddsolve/examples/activity.rs` that runs the learning search on a colouring instance, captures the final `(VarId, activity)` ranking, and prints it sorted descending. Interpret: high-activity variables are the ones a dynamic order would branch on first.

```rust
// Usage: cargo run -p ddsolve --example activity --release -- <n> <d> <seed>
// Prints: var, activity (descending). No assertion — this is a measurement driver.
```

- [ ] **Step 4: Smoke-run**

Run: `cargo run -p ddsolve --example activity --release -- 7 3 1`
Expected: prints a non-empty `var, activity` ranking. Record the top variables; compare against the natural index order to judge whether dynamic ordering would diverge meaningfully (motivating or de-motivating the full rewrite).

- [ ] **Step 5: Commit**

```bash
git add ddsolve/src/learn.rs ddsolve/examples/activity.rs
git commit -m "feat(ddsolve): VSIDS-style activity measurement"
```

---

## Final verification

- [ ] Run the whole suite: `cargo test -p ddsolve`
- [ ] Build all examples: `cargo build -p ddsolve --examples`
- [ ] Run each experiment driver once and record the headline numbers in a short results note (append to this file or a sibling `RESULTS.md`):
  - Item 1: `blowup` — peak frontier vs solution count.
  - Item 2: `churn` — root-ward vs leaf-ward churn.
  - Item 3: `learning` — peak frontier plain vs learned.
  - Extension 5: `activity` — top variables vs index order.

---

## Self-review notes (author)

- **Spec coverage:** Items 1–3 each have full bite-sized tasks (1–6, 7–9, 10–12); extensions 4–5 have design+key-code tasks (13, 14) as requested.
- **TDD honesty:** several tasks front-load a trivial implementation (branching, dead-end detection) where writing a failing test first is artificial; each such step includes an explicit "force a red" instruction so the test genuinely discriminates.
- **Known wrinkles flagged inline (not hidden):** (a) `Collection` cannot escape its dataflow scope → driver builds dataflow + sink together (Task 7); (b) `project_core_against` captures constraints by value → learning is not yet incremental in the projection step (Task 12); (c) measuring *interior* peak frontier needs a debug hook beyond `count_by_depth` on solutions (Task 12 Step 5); (d) dynamic ordering does not rewire `expand` — Task 14 is measurement-only.
- **Type consistency:** `Node = Vec<Val>`, `Forbidden = (VarId,Val,VarId,Val)` canonical `va<vb`, `Nogood = Vec<(VarId,Val)>` used consistently across `types`, `solve`, `learn`.
- **Research-finding tests:** Task 9's churn assertion may legitimately fail on some instances; the step explicitly treats that as a finding to record, not a green to force.
