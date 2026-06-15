# Benchmarking methodology (ddsolve)

How the ddsolve experiments measure an *incrementally maintained* CSP solver, and the
pitfalls found along the way. Numbers live in `RESULTS.md`; this file is the "how/why".

## What is being measured

`search`/`search_live` maintain the full set of solutions (and the full live search frontier)
of a finite-domain CSP as a differential-dataflow collection. A workload is a stream of
**constraint updates** (each toggles one forbidden value-pair on/off). We measure how cheaply
the maintained output absorbs each update.

## Driver patterns (and the v0.24 API facts behind them)

- `timely::example` / `execute_directly` build the dataflow once and run to completion. They
  require the returned value be `Send` (use `Arc<Mutex<_>>`, not `Rc`).
- **Never feed an `InputSession` inside `timely::example`** — it is not driven/closed there and
  the dataflow hangs. For single-shot correctness tests use static `to_stream` data (auto-closes).
- For timed/driven runs use `worker.dataflow` returning the `InputSession`s + probe (a
  `Collection` cannot escape its scope; input handles and `stream.probe().0` can). Drive rounds
  with `advance_to` / `flush` / `worker.step_while(|| probe.less_than(t))`, then drop inputs.
- See `VERIFIED_API.md` for the full v0.24 signature notes.

## Closed loop vs open loop (the central methodology lesson)

- **Closed loop** (`bench`, `parscale`, `tput`): feed a batch, block until it completes, feed the
  next. Offered load is coupled to latency, so measured throughput is pinned at 1/latency
  (coordinated omission). Useful for per-update latency CCDFs, NOT for capacity.
- **Open loop / offered load** (`openloop`, `openloop_par`): fix a *virtual* arrival schedule —
  update `i` arrives at wall time `i/rate`, independent of system speed. After each batch
  completes, feed everything that virtually arrived during processing as the next (dynamically
  sized) batch. Latency = completion − virtual arrival, so a backlog shows up as growing latency.
  This is the only way to find true capacity and exercise pipelining.

## Key methodology decisions

- **Outer timely timestamp = wall-clock time** (elapsed microseconds, strictly increasing). Event
  time is real time; the probe frontier is real time. All workers derive it from a shared
  monotonic source after the init barrier.
- **Multi-worker open loop**: every worker derives the same logical time and feeds its own shard
  (`i % peers == index`). Multi-producer: each worker **strides** its shard (`i += peers`) — do
  NOT have every worker scan the whole `[prev,target)` range (that replicates O(batch) work on
  every worker and crushes high worker counts).
- **Saturation signal**: in open loop with full-drain batching, achieved ≈ offered *always* (we
  feed all arrivals by end of run), so saturation does NOT show as a throughput drop. It shows as
  **unbounded latency growth** — measure the growth ratio (2nd-half p50 / 1st-half p50); ~1 below
  capacity, ≫1 past the knee. Also watch achieved < offered at extreme rates.
- **`max_batch` knob**: caps arrivals drained per round. With a finite cap a backlog builds once
  offered > capacity → a clean classic knee. The knee location IS the cap (a knob, not hardware).
- **Run long enough**: a slow backlog can take seconds to reveal. Verify latency is flat over a
  window several× the initial probe (8 s used). The longer run also caught a real overflow bug
  (`prev_target + usize::MAX`; fixed with `saturating_add`).

## Pitfalls discovered (read before trusting a number)

1. **Coordinated omission** — closed-loop throughput is 1/latency, ~16× below real capacity.
2. **Toggle cancellation / consolidation** — toggling a *small* pool of pairs means a large batch
   self-cancels to a few net diffs; DD then does ~no work and apparent capacity is unbounded
   (25M+ updates/s at 0.04 µs/element = a tell that nothing real is happening). Fix: keep the
   per-round batch **below** the constraint pool so toggles don't revisit/cancel — this needs
   `max_batch ≪ pool_size`, NOT merely a big pool (in full-drain the batch grows past any pool;
   see pitfall 8).
7. **Warmup contamination** — the first full-frontier fixpoint + DD arrangement build costs seconds;
   for low worker counts it ≈ the whole window, polluting `achieved` (truncated tail) and latency
   percentiles (slow early samples make medians *fall* as offered rises). **Fixed:** `warmup_s`
   arg excludes that window (snapshot at boundary, count only the measured region).
8. **`achieved` is not a ceiling in full-drain** — with unbounded `max_batch`, `achieved → offered`
   by construction (we drain everything eventually). A ceiling only shows via latency divergence or
   in capped mode. Reading `achieved` as throughput produced the false "millions/s" result; the
   real capacity (capped probe) is ~3k/s, matching closed-loop `bench`. Validity gate: full-drain
   rows must show `achieved≈offered` or the window was too short (warmup bias).
3. **Replicated driver scan** — every worker scanning the full batch range looks like a worker-0
   bottleneck and inverts scaling; stride per worker instead.
4. **Driver-bound top end** — at multi-million batches the generate/toggle loop itself costs real
   time; the very highest-rate rows are partly measuring the driver, not the dataflow.
5. **Feed distribution is a red herring** for work placement — DD exchanges by key regardless of
   which worker fed the tuple (proven: w0-feeds-all ≡ all-feed-shard, `tput`). It matters only for
   the *generation* cost (pitfall 3).
6. **Parallelism needs parallel work** — a self-cancelling micro-delta stream has ~no parallel
   per-round work, so more workers only add exchange overhead; peak capacity stays flat (best case)
   or drops. Genuine speedup needs a large-net, non-cancelling workload on a substantial frontier.

## The experiment drivers

| example | measures |
|---|---|
| `blowup` | frontier size per depth (materialisation cost) |
| `memscale` | peak heap vs problem size (counting allocator) |
| `bench` | closed-loop per-update latency CCDF (plain vs learning) |
| `parscale` | closed-loop strong-scaling speedup across workers |
| `tput` | feed-distribution isolation (w0 vs all) |
| `openloop` | single-worker offered-load throughput + CCDF |
| `openloop_par` | multi-worker offered load; knee finder; `max_batch` + `pool_size` knobs |
| `churn` / `learning` / `activity` | Item 2/3 research drivers (not perf) |

## Meaningful-work run — RETRACTED then FIXED

`openloop_par 12 4 1.5 1 0 1000` was first read as "W=16 sustains 1.87M/s, parallelism pays."
**Wrong** (see RESULTS.md § RETRACTED): in full-drain `achieved ≈ offered` by construction (not a
ceiling), the 1.5 s numbers were the window truncating during warmup, and a big pool still
self-cancels once the batch exceeds it. **Fix applied:** `openloop_par` gained a `warmup_s` arg
(excluded window) + `ratio`/`valid` columns. Definitive run
`openloop_par 12 4 60 1 200 20000 10` (capped below pool, 10 s warmup, 60 s measured): real
meaningful-work capacity **≈4k updates/s, peaking at W=4** then declining (W=1 ~1650/s), saturated
and invariant across offered rate — reconciles with closed-loop `bench`. Full-drain validity run
(`... 0 1000 10`) shows `ratio≈1` everywhere (`valid=yes`), confirming the gate. Numbers in
RESULTS.md § DEFINITIVE.

## Methodology — warmup exclusion (the fix)

The driver now runs a `warmup_s` window normally but EXCLUDES it: at the boundary it snapshots
`prev_target`/elapsed and counts only the measured window for `achieved` and latency. This removes
the first full-frontier fixpoint + arrangement build (seconds) that previously dominated low-W
rows. Each row also reports `ratio = achieved/offered` and a `valid` gate — full-drain trustworthy
iff `ratio≈1`; capped row reports capacity iff `ratio<1` (saturated). Run windows of many rounds
(60 s) so the measured region is steady state, not a transient.
