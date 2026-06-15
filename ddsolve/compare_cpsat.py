#!/usr/bin/env python3
"""OR-Tools CP-SAT reaction baseline for the ddsolve head-to-head.

Reads an instance JSON written by `examples/compare.rs` (n, d, base forbidden pairs, change
stream, and ddsolve's measured per-change reaction latencies). Applies the SAME change stream
(toggle each pair in/out of an active set) and, after every change, measures CP-SAT's reaction
time. Two reaction modes, both rebuild-and-solve from scratch (CP-SAT keeps no cross-solve
state):

  * find_one : solve for ONE feasible assignment (the realistic scheduler reaction).
  * count    : count ALL solutions (the SAME task ddsolve performs — apples-to-apples).

Prints a one-line summary; with --emit also rewrites the JSON adding cpsat latencies.
"""
import json
import sys
import time
import argparse
from ortools.sat.python import cp_model


def build(n, d, forb):
    m = cp_model.CpModel()
    xs = [m.new_int_var(0, d - 1, f"x{i}") for i in range(n)]
    for (a, xa, b, xb) in forb:
        # forbid the simultaneous assignment x_a == xa AND x_b == xb
        m.add_forbidden_assignments([xs[a], xs[b]], [(xa, xb)])
    return m, xs


def react_find_one(n, d, forb):
    m, _ = build(n, d, forb)
    s = cp_model.CpSolver()
    s.parameters.num_search_workers = 1
    s.parameters.max_time_in_seconds = 10.0
    t0 = time.perf_counter()
    s.solve(m)
    return (time.perf_counter() - t0) * 1e6  # us


class _Counter(cp_model.CpSolverSolutionCallback):
    def __init__(self):
        super().__init__()
        self.n = 0

    def on_solution_callback(self):
        self.n += 1


def react_count(n, d, forb):
    m, _ = build(n, d, forb)
    s = cp_model.CpSolver()
    s.parameters.num_search_workers = 1
    s.parameters.enumerate_all_solutions = True
    s.parameters.max_time_in_seconds = 10.0
    cb = _Counter()
    t0 = time.perf_counter()
    s.solve(m, cb)
    return (time.perf_counter() - t0) * 1e6  # us


def pct(xs, p):
    if not xs:
        return 0.0
    s = sorted(xs)
    i = round((p / 100.0) * (len(s) - 1))
    return s[min(i, len(s) - 1)]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("json")
    ap.add_argument("--mode", choices=["find_one", "count"], default="find_one")
    ap.add_argument("--emit", action="store_true")
    args = ap.parse_args()

    data = json.load(open(args.json))
    n, d = data["n"], data["d"]
    base = [tuple(p) for p in data["base"]]
    changes = [tuple(p) for p in data["changes"]]
    react = react_find_one if args.mode == "find_one" else react_count

    active = set()
    lat = []
    for pair in changes:
        if pair in active:
            active.discard(pair)
        else:
            active.add(pair)
        forb = base + list(active)
        lat.append(react(n, d, forb))

    dd = data["ddsolve_us"]
    dd_p50, dd_p99 = pct(dd, 50), pct(dd, 99)
    cs_p50, cs_p99 = pct(lat, 50), pct(lat, 99)
    ratio = cs_p50 / dd_p50 if dd_p50 else float("nan")
    # CSV: n, d, base, changes, dd_p50_us, dd_p99_us, cpsat_p50_us, cpsat_p99_us, speedup(cpsat/dd)
    print(
        f"{n},{d},{len(base)},{len(changes)},"
        f"{dd_p50:.1f},{dd_p99:.1f},{cs_p50:.1f},{cs_p99:.1f},{ratio:.2f}"
    )
    if args.emit:
        data[f"cpsat_{args.mode}_us"] = lat
        json.dump(data, open(args.json, "w"))


if __name__ == "__main__":
    main()
