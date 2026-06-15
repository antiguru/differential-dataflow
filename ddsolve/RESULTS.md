# ddsolve — research results

Backtracking CSP search expressed as differential dataflow. See
`docs/superpowers/plans/2026-06-14-backtracking-csp-on-dd.md` for the design and
`ddsolve/VERIFIED_API.md` for the DD v0.24 idioms used.

Core idea: the search tree is reified as a collection of assignment prefixes. Branching
is `flat_map`, consistency pruning is `join` + `antijoin`, recursion is `iterate`.
"Backtracking" is a negative diff — a pruned node simply leaves the collection.

All numbers below are reproducible via the example drivers (run with `--release`).

## Item 1 — frontier blow-up (`--example blowup`)

`blowup 6 3 40 1` (random colouring, 6 vars, domain 3, 40% edges):

```
depth:        0  1  2   3   4   5    6
net live:     1  3  9  27  36  72  144
peak_frontier=144  solutions=144
```

Pure DD materialises the entire feasible frontier. Growth is the exponential `3^depth`
(1,3,9,27…) tempered by constraint pruning (36/72/144 vs unconstrained 81/243/729).

## Item 2 — incremental re-solve churn (`--example churn`)

`churn 8 3 1` (8 vars, domain 3): round-2 churn from adding one forbidden pair, by
where the perturbed edge sits in the static variable order:

```
root_ward edge (0,1):  round-2 churn = 96
leaf_ward edge (6,7):  round-2 churn = 64
```

Confirms the hypothesis: a constraint delta on an early (root-ward) variable disturbs
more of the solution set than the same delta on a late (leaf-ward) variable. The
`leaf <= root` relation is asserted in `tests/incremental.rs`.

## Item 3 — conflict learning (`--example learning`)

Learning is sound (solution set unchanged — `tests/learning.rs`) and prunes the frontier
where domain wipeouts occur. Domain 3 is usually too loose (0 nogoods); domain 2 shows
the effect (`learning <n> 2 <seed>`):

```
n=8 d=2 seed=1:  peak live  plain=16  learned=4   nogoods=24   (4x reduction)
n=8 d=2 seed=2:  peak live  plain=4   learned=1   nogoods=11
n=8 d=2 seed=3:  peak live  plain=4   learned=1   nogoods=5
n=8 d=2 seed=4:  peak live  plain=4   learned=1   nogoods=13
n=8 d=2 seed=5:  peak live  plain=4   learned=4   nogoods=2
```

Cross-branch nogoods (minimal cores projected from dead ends, greedy set cover) cut the
peak live frontier up to 4x. Key correctness subtlety: dead-end detection must exclude
full-depth solution leaves, else solutions are misread as nogoods and pruned.

## Incremental latency benchmark (`--example bench`)

The headline measurement: initialise with a base instance (untimed), then feed a stream
of single-constraint updates (each toggles one forbidden pair on/off) and record the
per-update latency — wall-clock from feeding the delta until the maintained solution
collection catches up. Emits a latency CCDF (`P[latency > x]`) so the tail is visible,
for plain incremental search vs the learning variant, on an identical update stream.

`bench 10 3 500 1` (10 vars, domain 3, 500 updates):

```
                  p50      p90      p99      max
plain            326 us   893 us   3.3 ms   27 ms
learning         3.1 ms   8.8 ms    20 ms   58 ms
```

Findings:
- Plain incremental maintenance is **sub-millisecond at the median** — a single
  constraint delta is absorbed by DD's incremental machinery without re-solving.
- The distribution is **heavy-tailed** (p50 → max ≈ 80×): most updates are cheap, but a
  delta that flips a large subtree's feasibility forces a big diff. This is the central
  cost characteristic of incremental search.
- In-loop **learning costs ~10× latency** (mutual-recursion feedback + the nogood subset
  join run every update), trading per-update speed for frontier reduction. Worthwhile only
  when the frontier blow-up it prevents dominates.

Scale note: domain 4 / 12 vars / 1000 updates (the requested "medium") runs the plain
variant fine (p50 8 ms, p99 163 ms, max 2.4 s) but the learning variant is impractically
slow there (the full frontier materialises at every update). Use domain ≤ 3 for the
side-by-side learning comparison; raise scale for plain-only runs.

## Parallel speedup (`--example parscale`)

Same update-stream benchmark run across 1/2/4/8 timely workers (`Config::process(W)`).
All workers advance the dataflow frontier each round; only worker 0 feeds data and times.

`parscale 12 4 200 1` (12 vars, domain 4, 200 updates):

```
workers   total    p50      p99      speedup
   1      24.8 s   100 ms   416 ms   1.00x
   2      15.3 s    65 ms   210 ms   1.62x
   4       8.9 s    39 ms   123 ms   2.77x
   8       8.0 s    37 ms    91 ms   3.08x
```

Findings:
- Real speedup — ~2.8x on 4 workers; p99 latency drops 4.5x (416 ms → 91 ms).
- Sub-linear and diminishing past 4 workers (3.08x at 8). Expected: per-update
  incremental work is fine-grained, and DD's data exchange plus the per-update `iterate`
  fixpoint barrier limit how much parallelism each small delta exposes. The largest wins
  are on the expensive (tail) updates, which carry enough work to amortise exchange.

## Open-loop / offered-load throughput (`--example openloop`)

The closed-loop benchmarks (bench/parscale/tput) feed a fixed batch, block until it
completes, then feed the next — so offered load is coupled to latency and throughput is
pinned at 1/latency (coordinated omission). The open-loop benchmark fixes a *virtual*
arrival schedule (update `i` arrives at `i/rate`, independent of system speed); after each
batch completes it feeds everything that virtually arrived during processing as the next,
dynamically sized batch. Latency = completion − virtual arrival (a backlog shows as growing
latency). Single worker.

`openloop 10 3 2 1` (domain 3, 10 vars, 2 s per rate):

```
offered_rps  achieved_rps  avg_batch  p50_ms  p99_ms  max_ms
    100            99         1.0      12.47   20.96   37.43
    500           499         1.0       2.76   14.10   25.04
   1000           999         1.4       1.76   14.78   21.32
   2000          1994         2.6       5.74   18.15   20.65
   5000          4988        20.8      10.60   23.59   31.49
  10000          9988        65.8      11.38   27.82   32.13
  20000         19903       126.2      11.56   27.22   30.42
  50000         49698       287.3      10.72   28.75   32.17
```

Findings:
- Achieved throughput **tracks the offered rate up to 50k updates/s** with p99 latency flat
  (~28 ms) — the system is not saturated anywhere in the swept range; capacity is higher.
- The mechanism is **dynamic batching under load**: as offered load rises, backlog accrues
  during each round so the next batch grows (avg batch 1 → 287), amortizing the per-round
  `iterate`/scheduling overhead. This is the pipelining the closed-loop benchmarks cannot
  exercise.
- Contrast with closed-loop: `bench` on the same 10/3 instance reports p50 326 us, implying
  ~3k updates/s. Open-loop sustains **≥50k/s (~16x)** — the closed-loop figure was coordinated
  omission, pinned at 1/latency. To find the true saturation point, push the offered rate
  past 50k/s until achieved throughput plateaus below offered and latency diverges.

## Multi-worker open loop + finding the knee (`--example openloop_par`)

Open loop across W timely workers, with the **outer timely timestamp set to wall-clock time**
(elapsed microseconds, strictly increasing) — every worker derives the same logical time from
a shared monotonic source after the init barrier, advances its input to it each round, and
feeds its own shard. A `max_batch` knob caps arrivals drained per round.

**Uncapped (`openloop_par 10 3 8 1 0`, 8 s/rate)** — no knee anywhere up to 1M updates/s,
and latency is stable over the whole run (growth ≈ 1.0, i.e. no drift / backlog buildup):

```
W   offered    achieved  avg_batch  p50_ms  p99_ms  growth
1   1,000,000   998,815    6037      11.6    28.8    0.97
4   1,000,000   999,609    1186       7.2    17.0    0.98
8   1,000,000   975,435    1124      13.6    24.4    0.96
```

(Verified over an 8 s window — 5x longer than the initial probe — to rule out a slowly
growing backlog; latency stays flat. Only rare isolated max-latency spikes, e.g. W8@1M hits
213 ms once, are scheduling outliers, not systematic growth.)

**Scaling offered load to the inflection (`openloop_par 10 3 1.5 1 0`, rates 1M–50M, 18-core host):**

Each worker is a multi-producer: it strides only its own shard indices (`i += peers`), so
input generation is divided across workers rather than every worker scanning the whole range.
(With the original full-range scan, every worker did O(batch) index work; at multi-million
batches that replicated cost crushed high worker counts — W16@50M managed only 8.7M/s. Striding
fixed it: W16@50M → 26.7M/s, a 3x recovery, and peak capacity is now flat across W rather than
decreasing.)

```
W    offered    achieved   p50_ms   p99_ms  growth   note
1    10M         9.93M      15.5     36.7    1.06
1    20M        19.74M      33.8     85.2    1.49
1    50M        25.52M    1126     1711     2.39     knee (achieved << offered, p50 x70)
4    10M         8.20M       9.7     22.3    1.01     knee onset
4    20M        14.16M      14.7     29.9    0.90
4    50M        27.69M     584     1067     2.72
8    20M        13.73M      39.0    688      0.89     knee
16   10M         9.62M      40.0     82.2    1.07
16   50M        26.73M     930     1352     1.25
```

Findings:
- A real inflection appears at high load: sustainable capacity (achieved ≈ offered, latency
  flat) is ~20–25M updates/s. Past it, achieved plateaus well below offered and p50 jumps
  ~50–70x (tens of ms → ~1 s).
- **Peak capacity is ~flat across worker counts (~25–28M/s), not increasing.** More workers do
  NOT raise throughput here: the deltas self-cancel (≤375 net constraints), so there is almost
  no parallel per-round work to spread, while cross-worker exchange overhead scales with W. At
  low/mid load W4 has the best latency (7–15 ms); high worker counts only add latency (W16 is
  25 ms even at 1M). DD parallelism needs genuinely parallel per-round work — a self-cancelling
  micro-delta stream is its worst case.
- Caveat: at 50M the avg batch is millions, so even the strided per-worker generate loop is a
  real cost; the very top rows are partly driver-bound. But the 20M→50M achieved plateau plus
  latency blow-up is a genuine system inflection.

**Capped (`openloop_par 10 3 1.5 1 8`, max_batch=8)** — capacity collapses, latency diverges:

```
W   offered     achieved  avg_batch  p50_ms  p99_ms   growth
1   10k .. 1M    ~2500/s     8.0      ~700    ~1400     ~2.9
4   10k .. 1M    ~2700/s     2.0      ~720    ~1450     ~3.0
8   10k .. 1M    ~1560/s     1.0      ~740    ~1450     ~2.9
```

Findings:
- **Capacity is governed by batch size (amortization), not per-update cost.** Uncapped, DD
  collapses the whole accumulated backlog into one fixpoint per round, so capacity exceeds
  1M updates/s with flat ~28 ms p99. Capping the batch to 8 collapses capacity to ~2.5k/s and
  p99 diverges to ~1.4 s (growth ~3 = backlog building each round) — the textbook saturation
  knee. **The knee location is the batch cap, a knob, not a hardware ceiling.**
- A second reason the uncapped run never saturates at this scale: only ~375 distinct
  constraints exist (10 vars, dom 3), and batched toggles over a 64-pair pool self-cancel via
  consolidation, so net work per round is bounded regardless of offered rate.
- **Parallelism helps only with large batches.** Uncapped: 4 workers roughly halve latency
  (p50 12→7 ms, p99 28→16 ms); 8 workers are worse than 4 (oversubscription / exchange
  overhead) — ~4 effective cores. Capped to 8: 4 workers ≈ 1 worker and **8 workers are the
  slowest** (~1.5k/s) — tiny per-round batches cannot amortize cross-worker exchange. This is
  the deeper explanation for parscale's diminishing returns: one-at-a-time deltas are the
  small-batch regime where exchange dominates.

## RETRACTED: the open-loop "capacity" numbers are warmup/full-drain artifacts

⚠️ **The open-loop `achieved_rps` figures above (and the earlier 25M/s) do NOT measure
capacity.** A detailed re-analysis (2026-06-15) found three compounding methodology bugs, since
fixed (warmup exclusion added). The honest meaningful-work capacity is **≈4k updates/s, peaking
at W=4**, matching the original closed-loop `bench` (~3k/s) — the "millions/s" was fiction. The
experiments above are kept only as a record of the flawed measurements; the DEFINITIVE table
below supersedes them.

### Bug 1 — `achieved` cannot expose a ceiling in full-drain mode
With `max_batch` unbounded, every round drains *all* arrivals, so by construction
`achieved → offered` as the window grows. It is not a capacity metric; it is offered load minus
the uncounted tail. Proof — the same `openloop_par 12 4 <dur> 1 0 1000`, W=4, at two windows:

```
offered   achieved(1.5s)  ratio    achieved(8s)  ratio
10k       3975            0.40     9909          0.99
50k       20806           0.42     49871         1.00
100k      37949           0.38     97671         0.98
500k      252757          0.51     493843        0.99
1M        411067          0.41     987723        0.99
2M        21              0.00     1999767       1.00
```

The 1.5 s "achieved ≈ 0.4 × offered" was a *constant fraction of offered* (the giveaway that it
tracks offered, not a ceiling). At 8 s it is ≈ offered at every rate — even W=1 drains 1.82M/s.
So "W=16 = 1.87M/s ceiling / W=1 drowns" was purely the 1.5 s window truncating during warmup.

### Bug 2 — warmup contamination dominates (NOW FIXED)
The initial full-frontier fixpoint + DD arrangement build costs *seconds* (dom-4/12-var). For
low worker counts this single fixpoint ≈ the whole measurement window, so it pollutes both
`achieved` (truncated tail) and the latency percentiles (the slow early samples). The latency
medians even *fall* as offered rises (W=4 p50 347 ms→8 ms from 10k→1M) — not because the system
sped up, but because steady-state samples bury the fixed warmup cost in the tail. **Fixed:**
`openloop_par` now takes a 7th arg `warmup_s` (default 10) — the warmup window runs normally but
is excluded; at the boundary it snapshots `prev_target`/elapsed and counts only the measured
window. Output gains a `ratio` (achieved/offered) and a `valid` gate.

### Bug 3 — full-drain re-enters cancellation even with a "large" pool
The large-pool fix only escapes self-cancellation if the per-round batch stays *below* the pool.
In full-drain the batch grows to ~1M while the pool is ≤992, so each round toggles every pair
~1000× and still collapses to ≤992 net diffs. Escaping cancellation requires `max_batch ≪ pool`.

### DEFINITIVE capacity — warmup-excluded, capped below pool, 60 s window
`openloop_par 12 4 60 1 200 20000 10` (12 vars, dom 4, max_batch=200, pool clamped 992, 10 s
warmup excluded, 60 s measured; 18-core host). Saturated at every rate (offered ≫ capacity), so
`achieved` IS the capacity; invariant across offered rate confirms a real ceiling. All `valid=yes`.

```
W   achieved_rps (50k / 500k / 2M offered)   ~cap    p50_ms   growth
1   1846 / 1453 / 1643                        ~1650   ~39000   ~2.2
4   4201 / 4019 / 3878                        ~4000   ~39000   ~2.2
8   3699 / 3642 / 3740                        ~3700   ~38000   ~2.2
16  3862 / 3696 / 3394                        ~3650   ~38000   ~2.3
```

Findings (definitive):
- **Real meaningful-work capacity ≈ 4k updates/s, peak at W=4**, then a slight *decline* to
  W=8/16. ~2.4× from W=1, nothing past W=4 — the same ~4-effective-cores ceiling as `parscale`.
- **Reconciles with the closed-loop `bench` (~3k/s)** — the closed-loop figure was right; the
  open-loop "millions/s" was the cancellation+full-drain+warmup artifact.
- **W=1 capacity ~1650/s** (invariant across offered rate). The earlier 8 s "270/s" was warmup;
  excluding it raises and stabilises the number. W=1 is now measurable.
- p50 ~38–39 s and growth ~2.2 just reflect the enormous backlog (offered 50k vs 4k capacity);
  under saturation latency necessarily diverges — the trustworthy output is `achieved`.

### Full-drain validity check — warmup-excluded, 60 s (`openloop_par 12 4 60 1 0 1000 10`)
Confirms the gate and the contrast. Full drain (batch ≫ pool ⇒ cancellation ⇒ near-trivial work)
keeps up everywhere (`ratio≈1.00`, `valid=yes`), with clean warmup-excluded latency:

```
W   ratio (50k/500k/2M)   p50_ms@2M   p99_ms@2M
1   1.00 / 0.97 / 0.99    938         5364
4   1.00 / 0.99 / 1.00    2.5         9.8
8   1.00 / 0.98 / 0.99    6.4         14.7
16  1.00 / 0.99 / 0.99    16.8        33.6
```

This drains 2M/s only because the work self-cancels (≤992 net diffs/round); it is NOT 2M/s of
real solving. The capped table above is the honest capacity. Note W=1 full-drain p99 ~5 s vs
W≥4 ~10–35 ms: parallelism still cuts tail latency even in the trivial regime.

Methodology now sound. To trust a row: (1) warmup excluded (built in); (2) gate on `valid`
(full-drain ⇒ `ratio≈1`; capped capacity ⇒ `ratio<1`, saturated); (3) for capacity use
`max_batch ≪ pool` AND a window of many rounds (60 s here).

## Feed distribution — is single-worker feeding the bottleneck? (`--example tput`)

`parscale` feeds all updates on worker 0. Does that serialize and cause the diminishing
returns? Tested directly: a batched-throughput benchmark comparing "w0" (worker 0 feeds the
whole batch) vs "all" (each worker feeds its own shard) at identical total work.

`tput 12 4 40 16 1` (weak scaling — W*batch toggles per round):

```
workers  feed  updates  time_s   throughput
   1     w0      640     79.09       8 upd/s
   1     all     640     80.17       8
   2     w0     1280     66.44      19
   2     all    1280     66.49      19
   4     w0     2560     45.34      56
   4     all    2560     45.46      56
   8     w0     5120     40.10     128
   8     all    5120     39.97     128
```

Conclusion: **feed distribution makes no difference** (<0.5% across every W). Single-worker
feeding is NOT the bottleneck — each update is one tiny tuple, exchanged to its home worker
in a single cheap message, and the heavy join/reduce/iterate work is sharded across workers
regardless of who fed the input.

The diminishing returns in `parscale` (strong scaling) come from the per-update `iterate`
fixpoint barrier: depths are sequential (critical path ≈ n_vars rounds, each an all-to-all
exchange), and a single-tuple delta exposes little intra-depth work to parallelise, so
exchange/coordination overhead dominates past ~4 workers. (Note: the `tput` numbers above
are WEAK scaling — batch grows with W — so larger batches amortise the per-round barrier;
read them as feed-mode comparison, not strong-scaling speedup.)

## Memory scaling (`--example memscale`)

A counting global allocator tracks peak heap while `search_live` runs; swept over problem
size. `memscale 3 30 1 13` (domain 3, 30% edge density):

```
n_vars  frontier_nodes  solutions  peak_KB  bytes_per_node
   6          292          144        560      1965
   7          733          324       1043      1457
   8         1561          576       2096      1375
   9         4522          864       8501      1925
  10         3487          864       5825      1711
  11         2881          360       4017      1428
  12         5512          528       8906      1655
  13         2557          240       3292      1318
```

Findings:
- **Peak memory ∝ frontier size, not `n_vars`.** Bytes-per-node is roughly constant
  (1.3–2.0 KB), so `peak ≈ frontier_nodes × ~1.5 KB`. The frontier (the materialised
  search tree, all depths) is the single driver of memory.
- **Frontier size is non-monotone in `n_vars` at fixed edge density**: more variables at
  30% density means more edges per node, hence more pruning — so the frontier peaks at
  n=12 (5512 nodes) here, not n=13 (2557). Worst case (sparse constraints) the frontier is
  exponential `domain^depth`; constraint pruning tempers it.
- **~1.5 KB/live-node** is far above the raw node (`Vec<u16>` of ≤13 values ≈ 26 bytes).
  The overhead is DD's arrangement/trace indexing — `iterate` keeps arranged traces of the
  frontier. So memory, not time, is the binding constraint of full-frontier materialisation,
  and it is what beam bounding (Extension 4) or in-loop learning (Item 3) exist to contain.

## Extension 4 — beam bounding (`search_beam`)

Top-k per depth via `reduce`. Incomplete by design: equals full search at large `k`
(tested), a subset at small `k` (`tests/search.rs::beam_solutions_subset_of_full`).

## Extension 5 — VSIDS-style activity (`--example activity`)

`activity 8 2 1`: per-variable nogood-occurrence counts (measurement only — the search
keeps a static order):

```
var:       1   0   2   3   4   5
activity: 22  20  20  14   8   6
dynamic order would prioritise: [1, 0, 2, 3, 4, 5]   (vars 6,7: no activity)
```

The activity signal exists and diverges from the index order, motivating (but not
implementing) dynamic variable ordering.

## Known limitations (prototype scope)

- Uniform domain, binary extensional constraints only. Aggregate capacity ("≤K tasks per
  node") is modelled only for K=1 via pairwise `exclusive`; the aggregate encoding (a
  per-node `reduce`) is future work.
- Static variable order (deliberate — Item 2's incrementality depends on stable tree shape).
- Minimal-core projection is greedy (sound, not globally minimal). Projection captures the
  constraint set by value, so learning is not yet incremental in the projection step.
- Dynamic ordering (Extension 5) is measured, not wired into `expand`.
