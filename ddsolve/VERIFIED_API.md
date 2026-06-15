# Verified differential-dataflow v0.24 API (for ddsolve)

The implementation plan was written against an older DD API (`Collection<G: Scope, D, R>`,
single-arg `iterate(|inner| …)`, `&other` join args). **v0.24 differs.** Translate all
plan code to the verified idioms below. When in doubt, copy the shapes here — they compile.

## Collection type

`Collection<'scope, T: Timestamp, C>` is generic over a scope lifetime, a timestamp `T`,
and a container `C`. The data/diff-parameterized alias is what we use everywhere:

```rust
use differential_dataflow::VecCollection; // = Collection<'scope, T, Vec<(D, T, R)>>, R defaults to isize
```

`VecCollection<'a, T, D>` means a collection of `D` with `isize` diffs in a scope with
timestamp `T`.

## Generic operator helper functions — the canonical signature

Model every helper on the `bfs` example (`differential-dataflow/examples/bfs.rs`):

```rust
use differential_dataflow::VecCollection;
use differential_dataflow::lattice::Lattice;
use timely::progress::Timestamp;

pub fn my_op<'a, T>(input: &VecCollection<'a, T, MyData>) -> VecCollection<'a, T, MyOut>
where
    T: Timestamp + Lattice + Ord,
{
    // ...
}
```

- Parameterize by `<'a, T>`, NOT by `<G: Scope>`.
- Bound: `T: Timestamp + Lattice + Ord` (add `+ std::hash::Hash` if a join/antijoin needs it — it usually compiles without, the inner data bounds carry it).
- Take inputs by `&VecCollection<'a, T, _>` and `.clone()` inside before consuming operators.

## Operators consume `self` (clone to reuse)

`map`, `flat_map`, `filter`, `join_map`, `semijoin`, `antijoin`, `concat`, `consolidate`,
`distinct`, `reduce`, `iterate` all take `self` by value. To use a collection twice, `.clone()`
it (cheap — clones a stream handle).

## Import rules

- `map`, `flat_map`, `filter`, `concat`, `consolidate`, `join_map`, `semijoin`, `antijoin`
  are **inherent methods** on `Collection` — NO import needed.
- `distinct` is **inherent** on `Collection` in v0.24 — no import needed (verified Task 3).
- `reduce`, `count`, `threshold`, `iterate`, `enter`/`leave` are **trait methods** — bring them in with:
  ```rust
  use differential_dataflow::operators::*;
  ```
  (or the specific trait, e.g. `use differential_dataflow::operators::iterate::Iterate;`)
- `consolidate` is inherent — do NOT write `use differential_dataflow::operators::Consolidate;` (it does not exist; that import fails to compile).
- Input + test helpers:
  ```rust
  use differential_dataflow::input::Input;                     // new_collection, InputSession
  use timely::dataflow::operators::capture::{Capture, Extract}; // .inner.capture() / .extract()
  ```

## join_map / semijoin / antijoin — args BY VALUE

Verified signatures (collection.rs ~1150–1210). `self` is `Collection<(K, V), R>`:

```rust
left.join_map(right, |k, v1, v2| out)   // right: Collection<(K, V2), R2> BY VALUE -> Collection<D, R*R2>
kv.semijoin(keys)                        // keys:  Collection<K, R2>      BY VALUE -> keeps (k,v) where k in keys
kv.antijoin(keys)                        // keys:  Collection<K, R2>      BY VALUE -> keeps (k,v) where k NOT in keys
```

NOTE: the plan often writes `.antijoin(&conflicting)` / `.join_map(&other, …)` — drop the `&`,
pass by value, and `.clone()` the argument collection if it is reused elsewhere.

## reduce

Inherent shape from bfs: `coll.reduce(|key, input, output| { … })` where `input: &[(&V, R)]`
and you `output.push((val, diff))`. Example: `.reduce(|_, s, t| t.push((*s[0].0, 1)))`.

## iterate — TWO-arg closure, `.enter(scope)` for outer collections

The plan shows single-arg `iterate(|inner| …)`. **v0.24 is two-arg** `iterate(|scope, inner| …)`:

```rust
let result = roots.clone().iterate(|scope, inner| {
    let roots = roots.enter(scope);      // bring outer collections into the nested scope
    let edges = edges.enter(scope);
    inner.join_map(edges, |_k, l, d| (*d, l + 1))
         .concat(roots)
         .reduce(|_, s, t| t.push((*s[0].0, 1)))
});
```

- `inner` is the live collection in the nested scope (timestamp `Product<T, u64>`).
- `.enter(scope)` lifts an outer collection in; the operator returns the next-iteration collection.
- Iterate runs to fixed point and leaves the scope automatically (returns `VecCollection<'a, T, …>`).

## Variable (for mutual recursion / multiple feedback edges, Task 12)

```rust
use differential_dataflow::operators::iterate::Variable;
use timely::order::Product;

scope.scoped::<Product<T, u64>, _, _>("name", |inner| {
    let (var, coll) = Variable::new(inner, Product::new(Default::default(), 1));
    // ... build `next` from `coll` ...
    var.set(&next);            // or var.set(next) — check arity; bind the feedback
    coll.leave()               // strip the inner timestamp on the way out
})
```
`Variable::new(scope, step)` returns `(Variable, Collection)`. Confirm exact `set` arg
(by value vs ref) against `differential-dataflow/src/operators/iterate.rs` when wiring Task 12.

## Tests — CRITICAL: do NOT use InputSession inside `timely::example`

`timely::example(func)` = `execute_directly(|w| w.dataflow(func))`. It builds the dataflow
once and runs to completion. An `InputSession` from `scope.new_collection()` created *inside*
that closure is never driven or closed by this harness, so the frontier never empties and
**the test hangs forever** (observed: `expand_one_level` ran >60s). The plan's tests all use
`new_collection` + `insert`/`advance_to`/`flush` inside `timely::example` — that pattern is
WRONG and hangs. Translate every such test to one of these two proven patterns:

### Pattern A — static data via `to_stream` (use for single-shot tests: Tasks 2,3,4,6,10,11,13)

Auto-closing; matches `differential-dataflow/tests/reduce.rs`. Timestamp is `u64` (example gives `Scope<u64>`).

```rust
use differential_dataflow::AsCollection;
use timely::dataflow::operators::{Capture, ToStream};
use timely::dataflow::operators::capture::Extract;

let data = timely::example(|scope| {
    // each tuple is (data, time, diff)
    let roots = vec![(Vec::<Val>::new(), 0, 1isize)].into_iter().to_stream(scope).as_collection();
    let forbidden = vec![((0u16,1u16,1u16,1u16), 0, 1isize)].into_iter().to_stream(scope).as_collection();
    my_op(&roots, &forbidden).consolidate().inner.capture()
});
let survivors: Vec<_> = data.extract().into_iter()
    .flat_map(|(_, b)| b.into_iter().filter(|(_,_,r)| *r > 0).map(|(d,_,_)| d)).collect();
```

### Pattern B — driven worker for multi-round delta tests (Tasks 7,8,9,12 incremental)

Matches `differential-dataflow/tests/bfs.rs` `bfs_differential`. Drive inputs across rounds,
`worker.step()` to a probe, and **drop the inputs** so the run terminates.

```rust
timely::execute_directly(|worker| {
    let (mut input_a, mut input_b, probe) = worker.dataflow::<u32, _, _>(|scope| {
        let (ha, ca) = scope.new_collection::<Node, isize>();
        let (hb, cb) = scope.new_collection::<Forbidden, isize>();
        let out = my_op(&ca, &cb);
        let probe = out.inner.probe();           // use timely ProbeHandle
        (ha, hb, probe)
    });
    input_a.insert(Vec::new()); input_a.advance_to(1); input_a.flush();
    input_b.advance_to(1); input_b.flush();
    worker.step_while(|| probe.less_than(input_a.time()));
    // ... more rounds ...
    // inputs dropped here -> dataflow closes
});
```
Collect results inside an `inspect_batch` writing to an `Arc<Mutex<...>>` (NOT `Rc` — the
value is returned out of `execute_directly`, which requires `Send`). `InputSession` and the
probe handle CAN be returned out of `worker.dataflow`; only a `Collection` cannot.

GOTCHA: `stream.probe()` returns a TUPLE `(Handle<T>, Stream)`, not just the handle. Use
`let probe = sols.inner.probe().0;` (or `probe_with(&handle)`). `less_than` is a method on
`Handle<T>`: `probe.less_than(input.time())`. `execute_directly(func) -> T` auto-runs the
dataflow to completion (`while worker.has_dataflows() { step_or_park }`) after `func` returns,
so dropping the inputs at the end of `func` lets it terminate.

## If something still won't compile

Read the real signature in `differential-dataflow/src/collection.rs` (inherent ops),
`src/operators/{reduce,threshold,count,iterate,join}.rs` (trait ops). Do NOT guess import
paths. If stuck after a genuine effort, report BLOCKED with the exact `rustc` error.
