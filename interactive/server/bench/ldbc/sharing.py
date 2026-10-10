#!/usr/bin/env python3
"""Static plan statistics (E0) and the cross-query sharing ceiling (E1).

Every level compiles the suite's logical plans to DDP and counts operators with
`examples/plan_stats.rs`, which runs the server's parse, lower and optimize
pipeline. No server is started. Levels:

- `separate`: one program per query, as `suite.py` installs them (E0).
- `combined`: all queries in one program, so DDIR's dedup sees every query.
- `context`: as `combined`, with one shared `Context`, so the helpers
  queries.py already factors (`edges`, `threads`, ...) are shared.
- `hashcons`: as `context`, with structurally equal plan nodes merged.
- `canonical`: as `hashcons`, after a directional normal form: conjuncts split,
  sorted and pushed through select, join, distinct, union, semi, anti and group
  keys, and adjacent selects composed.
- `spj`: as `canonical`, with each select-project-join block rebuilt in a
  canonical join order.
- `canonical-inplace`, `spj-inplace`: the same, with conjuncts merged and
  sorted but left where the plan put them.

Each rewritten plan is evaluated on the fixture and must return the original
plan's rows. The rewritten levels are also counted per query, so a change in
plan shape is not mistaken for sharing.

A normal form commits to one shape per block, which can dissolve sharing the
plan already had, so the e-graph proxy (`egraph_proxy`) leaves plans as written
and instead groups their arranged inputs by a name- and join-order-insensitive
signature. Inputs that differ by identity but share a signature are sharing an
e-graph could find without changing anything else.

The counts are static. The fixture only checks answers, so arrangements are not
weighted by rows.
"""
import argparse
from collections import Counter
from hashlib import sha1
import json
from pathlib import Path
import subprocess
import tempfile

from snb import witness
from snb.data import SCHEMA
from snb.parameters import parameters, requests
from snb.queries import Context, QUERIES
from snb.rel import Compiler, E, R, col, evaluate, source_shape

CATALOGUE = [f'{family}{i}' for family, end in (('is', 8), ('ic', 15), ('bi', 21)) for i in range(1, end)]
SPJ = ('join', 'select', 'where')


class MultiCompiler(Compiler):
    """`Compiler(imports=True)` with one positional input per `request.<query>` source."""
    def __init__(self):
        super().__init__(imports=True)
        self.requests = {}

    def emit(self, r):
        if r.op == 'source' and r.args[0].startswith('request.') and id(r) not in self.names:
            name = 'r' + str(len(self.names))
            self.names[id(r)] = name
            self.inputs.append((r.args[0], r.args[1]))
            index = self.requests.setdefault(r.args[0], len(self.requests))
            self.lines.append(f'-- {name}: ({", ".join(r.fields)})\n'
                              f'let {name} = input {index} : ({source_shape(r.args[1])} ; ());')
            return name
        return super().emit(r)


def plans(shared_context):
    context = Context()
    result = {}
    for name in CATALOGUE:
        fn, schema, _ = QUERIES[name]
        result[name] = fn(context if shared_context else Context(), R.source('request.' + name, schema))
    return result


def rebuild(r, args):
    return r if all(a is b for a, b in zip(args, r.args)) else R(r.op, r.fields, *args)


def memoized(transform):
    """Apply `transform(node, recurse)` bottom-up over a plan DAG, once per node."""
    def run(roots):
        memo = {}
        def recurse(r):
            if id(r) not in memo:
                memo[id(r)] = (r, transform(r, recurse))
            return memo[id(r)][1]
        return {name: recurse(r) for name, r in roots.items()}
    return run


def value_key(x):
    if isinstance(x, R): raise TypeError('plan node inside an expression')
    if isinstance(x, E): return ('E', x.op, tuple(value_key(a) for a in x.args))
    if isinstance(x, dict): return ('D', tuple((value_key(k), value_key(v)) for k, v in x.items()))
    if isinstance(x, (tuple, list)): return ('T', tuple(value_key(a) for a in x))
    return ('V', type(x).__name__, x)


def hashcons(roots):
    table = {}
    def transform(r, recurse):
        args = tuple(recurse(a) if isinstance(a, R) else a for a in r.args)
        key = (r.op, r.fields, tuple(('R', id(a)) if isinstance(a, R) else value_key(a) for a in args))
        return table.setdefault(key, rebuild(r, args))
    return memoized(transform)(roots)


def columns(e):
    if e.op == 'column': return {e.args[0]}
    return set().union(*(columns(a) for a in e.args if isinstance(a, E)))


def subst(e, env):
    if e.op == 'column': return env[e.args[0]]
    return E(e.op, tuple(subst(a, env) if isinstance(a, E) else a for a in e.args))


def conjuncts(p):
    return conjuncts(p.args[0]) + conjuncts(p.args[1]) if p.op == '&&' else [p]


def conjoin(cs):
    unique = {repr(value_key(c)): c for c in cs}
    ordered = [unique[k] for k in sorted(unique)]
    pred = ordered[0]
    for c in ordered[1:]:
        pred = E('&&', (pred, c))
    return pred


def where(x, cs):
    return R('where', x.fields, x, conjoin(cs)) if cs else x


def push(x, cs):
    """`x` filtered by the conjuncts `cs`, each placed as low as it can go."""
    if not cs: return x
    op, args = x.op, x.args
    if op == 'where':
        return push(args[0], cs + conjuncts(args[1]))
    if op == 'select':
        return R('select', x.fields, push(args[0], [subst(c, args[1]) for c in cs]), args[1])
    if op == 'distinct':
        return R('distinct', x.fields, push(args[0], cs))
    if op == 'union':
        return R('union', x.fields, push(args[0], cs), push(args[1], cs))
    if op in ('semi', 'anti'):
        return R(op, x.fields, push(args[0], cs), *args[1:])
    if op == 'join':
        left, right, on, renamed = args
        back = {new: old for old, new in renamed.items()}
        lefts = [c for c in cs if columns(c) <= set(left.fields)]
        rights = [c for c in cs if not columns(c) <= set(left.fields) and columns(c) <= set(back)]
        rest = [c for c in cs if not columns(c) <= set(left.fields) and not columns(c) <= set(back)]
        rights = [subst(c, {n: col(o) for n, o in back.items()}) for c in rights]
        return where(R('join', x.fields, push(left, lefts), push(right, rights), on, renamed), rest)
    if op == 'group':
        source, keys = args[0], set(args[1])
        down = [c for c in cs if columns(c) <= keys]
        return where(R('group', x.fields, push(source, down), *args[1:]), [c for c in cs if not columns(c) <= keys])
    return where(x, cs)


def canonical_node(r, recurse, pushdown):
    args = tuple(recurse(a) if isinstance(a, R) else a for a in r.args)
    if r.op == 'where' and pushdown:
        return push(args[0], conjuncts(args[1]))
    if r.op == 'where':
        source, cs = args[0], conjuncts(args[1])
        if source.op == 'where':
            source, cs = source.args[0], cs + conjuncts(source.args[1])
        return where(source, cs)
    if r.op == 'select':
        source, pairs = args
        if source.op == 'select':
            pairs = {n: subst(e, source.args[1]) for n, e in pairs.items()}
            source = source.args[0]
        if list(pairs) == list(source.fields) and all(e.op == 'column' and e.args[0] == n for n, e in pairs.items()):
            return source
        return R('select', r.fields, source, pairs)
    return rebuild(r, args)


def canonical(roots, pushdown):
    return memoized(lambda r, recurse: canonical_node(r, recurse, pushdown))(roots)


def signature(r, memo={}):
    if id(r) not in memo:
        parts = [r.op, r.fields] + [signature(a) if isinstance(a, R) else value_key(a) for a in r.args]
        memo[id(r)] = (r, sha1(repr(parts).encode()).hexdigest())
    return memo[id(r)][1]


class Fallback(Exception):
    """A block this normal form does not cover (an equijoin on a computed key)."""


def spj_node(r, recurse, stats, pushdown):
    if r.op not in SPJ:
        return rebuild(r, tuple(recurse(a) if isinstance(a, R) else a for a in r.args))
    atoms, eqs, preds = [], [], []
    def flatten(x):
        if x.op not in SPJ:
            atom = recurse(x)
            atoms.append(atom)
            return {c: col(('@', len(atoms) - 1, c)) for c in atom.fields}
        if x.op == 'select':
            env = flatten(x.args[0])
            return {n: subst(e, env) for n, e in x.args[1].items()}
        if x.op == 'where':
            env = flatten(x.args[0])
            preds.extend(conjuncts(subst(x.args[1], env)))
            return env
        left, right, on, renamed = x.args
        lenv, renv = flatten(left), flatten(right)
        for lk, rk in on.items():
            if lenv[lk].op != 'column' or renv[rk].op != 'column':
                raise Fallback
            eqs.append((lenv[lk].args[0], renv[rk].args[0]))
        return {**lenv, **{new: renv[old] for old, new in renamed.items()}}
    try:
        out = flatten(r)
    except Fallback:
        stats['fallback'] += 1
        return canonical_node(r, recurse, pushdown)
    stats['blocks'] += 1
    atom_of = lambda e: {c[1] for c in columns(e)}
    local = {i: sorted(repr(value_key(subst(p, {c: col(c[2]) for c in columns(p)}))) for p in preds if atom_of(p) == {i})
             for i in range(len(atoms))}
    label = lambda i: (signature(atoms[i]), local[i])
    order, place = [], {}
    while len(order) < len(atoms):
        rest = [i for i in range(len(atoms)) if i not in place]
        def link(i):
            return sorted((place[a[1]], a[2], b[2]) for a, b in eqs + [(b, a) for a, b in eqs]
                          if b[1] == i and a[1] in place)
        connected = [i for i in rest if link(i)] or rest
        chosen = min(connected, key=lambda i: (label(i), link(i), i))
        place[chosen] = len(order)
        order.append(chosen)
    names = {}
    applied = set()
    plan = None
    for k, i in enumerate(order):
        atom = atoms[i]
        mine = [p for p in preds if atom_of(p) == {i}] if pushdown else []
        atom = where(atom, [subst(p, {c: col(c[2]) for c in columns(p)}) for p in mine])
        applied.update(id(p) for p in mine)
        prefix = f'_{k}_'
        if plan is None:
            plan = atom
            names.update({('@', i, c): c for c in atom.fields})
        else:
            on, extra = {}, []
            for a, b in eqs + [(b, a) for a, b in eqs]:
                if b[1] == i and a[1] in place and place[a[1]] < k:
                    if names[a] in on: extra.append((a, b))
                    else: on[names[a]] = b[2]
            plan = plan.join(atom, on, prefix)
            names.update({('@', i, c): prefix + c for c in atom.fields})
            for a, b in extra:
                preds.append(E('==', (col(a), col(b))))
        placed = set(order[:k + 1])
        done = pushdown or k == len(order) - 1
        ready = [p for p in preds if id(p) not in applied and atom_of(p) <= placed] if done else []
        applied.update(id(p) for p in ready)
        plan = where(plan, [subst(p, {c: col(names[c]) for c in columns(p)}) for p in ready])
    for a, b in eqs:
        if place[a[1]] == place[b[1]]:
            plan = where(plan, [E('==', (col(names[a]), col(names[b])))])
    return R('select', r.fields, plan, {n: subst(e, {c: col(names[c]) for c in columns(e)}) for n, e in out.items()})


def spj(roots, stats, pushdown):
    return canonical(memoized(lambda r, recurse: spj_node(r, recurse, stats, pushdown))(roots), pushdown)


def digest(x):
    return sha1(repr(x).encode()).hexdigest()[:16]


def positional(e, fields):
    """`e` with each column named by its position in `fields`, so names do not matter."""
    if e.op == 'column': return ('col', fields.index(e.args[0]))
    return (e.op, tuple(positional(a, fields) if isinstance(a, E) else value_key(a) for a in e.args))


class Signatures:
    """Name- and join-order-insensitive descriptions of plan nodes.

    `describe(r)` returns `(base, out)`: `base` identifies the rows a node's
    select-project-join block ranges over (its atoms in a canonical order, the
    equalities and the conjuncts), and `out[f]` is field `f` as an expression
    over those atoms. Two nodes whose outputs agree field for field compute the
    same relation. Atoms that tie on every label are ordered by their position,
    so some equivalent blocks get different signatures: merges found are real,
    the count of them is a lower bound.
    """
    def __init__(self):
        self.memo = {}

    def relation(self, r):
        base, out = self.describe(r)
        return digest((base, tuple(out[f] for f in r.fields)))

    def describe(self, r):
        return self.full(r)[:2]

    def relaxed(self, r):
        """`r`'s block with its single-atom conjuncts dropped: what an arrangement serving
        `r` and its differently filtered or projected siblings would range over."""
        return self.full(r)[2]

    def full(self, r):
        if id(r) not in self.memo:
            self.memo[id(r)] = (r, self.spj(r) if r.op in SPJ else (*self.atom(r), None))
        base, out, loose = self.memo[id(r)][1]
        return base, out, base if loose is None else loose

    def atom(self, r):
        parts = [r.op]
        source = next((a for a in r.args if isinstance(a, R)), None)
        for a in r.args:
            if isinstance(a, R): parts.append(self.relation(a))
            elif isinstance(a, E) and source is not None and r.op != 'closure': parts.append(positional(a, list(source.fields)))
            elif isinstance(a, dict) and r.op == 'group':
                parts.append(tuple((kind, positional(e, list(source.fields))) for kind, e in a.values()))
            elif isinstance(a, tuple) and r.op in ('group', 'rank') and all(isinstance(x, str) for x in a):
                parts.append(tuple(source.fields.index(x) for x in a))
            elif isinstance(a, tuple) and r.op == 'rank':
                parts.append(tuple(positional(e, list(source.fields)) for e in a))
            else: parts.append(value_key(a))
        return digest(parts), {f: ('col', i) for i, f in enumerate(r.fields)}

    def spj(self, r):
        atoms, eqs, preds = [], [], []
        def flatten(x):
            if x.op not in SPJ:
                atoms.append(x)
                return {c: col(('@', len(atoms) - 1, c)) for c in x.fields}
            if x.op == 'select':
                env = flatten(x.args[0])
                return {n: subst(e, env) for n, e in x.args[1].items()}
            if x.op == 'where':
                env = flatten(x.args[0])
                preds.extend(conjuncts(subst(x.args[1], env)))
                return env
            left, right, on, renamed = x.args
            lenv, renv = flatten(left), flatten(right)
            for lk, rk in on.items():
                preds.append(E('==', (lenv[lk], renv[rk])))
            return {**lenv, **{new: renv[old] for old, new in renamed.items()}}
        out = flatten(r)
        labels = [self.relation(a) for a in atoms]
        atom_of = lambda e: {c[1] for c in columns(e)}
        local = [sorted(repr(positional(subst(p, {c: col(c[2]) for c in columns(p)}), list(atoms[i].fields)))
                        for p in preds if atom_of(p) == {i}) for i in range(len(atoms))]
        place = {}
        def rename(e):
            return subst(e, {c: col((place[c[1]], atoms[c[1]].fields.index(c[2]))) for c in columns(e)})
        while len(place) < len(atoms):
            rest = [i for i in range(len(atoms)) if i not in place]
            def link(i):
                return sorted(repr(value_key(subst(p, {c: col(('new', c[2]) if c[1] == i else (place[c[1]], c[2]))
                                                       for c in columns(p)})))
                              for p in preds if i in atom_of(p) and atom_of(p) - {i} and atom_of(p) - {i} <= set(place))
            connected = [i for i in rest if link(i)] or rest
            place[min(connected, key=lambda i: (labels[i], local[i], link(i), i))] = len(place)
        order = sorted(place, key=place.get)
        base = digest(([labels[i] for i in order], sorted(repr(value_key(rename(p))) for p in preds)))
        loose = digest(([labels[i] for i in order], sorted(repr(value_key(rename(p))) for p in preds if len(atom_of(p)) > 1)))
        return base, {f: repr(value_key(rename(e))) for f, e in out.items()}, loose


def arranged(roots):
    """Each arranged input `(node, key fields, kind)` as `Compiler.emit` arranges it."""
    seen, points = set(), []
    def visit(r):
        if id(r) in seen: return
        seen.add(id(r))
        args = r.args
        if r.op in ('join', 'semi', 'anti'):
            points.append((args[0], tuple(args[2]), 'key'))
            points.append((args[1], tuple(args[2].values()), 'key' if r.op == 'join' else 'distinct'))
        elif r.op == 'group': points.append((args[0], args[1], 'group'))
        elif r.op == 'rank': points.append((args[0], args[3], 'rank'))
        elif r.op == 'distinct': points.append((args[0], args[0].fields, 'distinct'))
        elif r.op == 'closure': points.append((args[1], tuple(args[2].values()), 'key'))
        for a in args:
            if isinstance(a, R): visit(a)
    for r in roots.values(): visit(r)
    return points


def egraph_proxy(roots):
    """Arranged inputs counted by identity, then by signature (see `Signatures`)."""
    sigs = Signatures()
    points = arranged(roots)
    by_identity = {(id(r), key, kind): r for r, key, kind in points}
    groups, relaxed = {}, {}
    for (_, key, kind), r in by_identity.items():
        base, out = sigs.describe(r)
        keys = tuple(out[k] for k in key)
        groups.setdefault(digest((base, keys, tuple(out[f] for f in r.fields), kind)), []).append(r)
        relaxed.setdefault(digest((sigs.relaxed(r), keys, kind)), []).append(r)
    merged = [g for g in groups.values() if len(g) > 1]
    return dict(by_identity=len(by_identity), by_signature=len(groups), merged_groups=len(merged),
                merged_members=sum(len(g) for g in merged), by_relaxed=len(relaxed))


def fixture():
    graph = witness.graph()
    tables = {name: [tuple(tuple(v.encode()) if kind == 'bytes' else v for v, kind in zip(row, schema.values()))
                     for row in graph[name]] for name, schema in SCHEMA.items()}
    for name in CATALOGUE:
        _, schema, _ = QUERIES[name]
        params = parameters(graph, name)
        params.update(witness.params(name))
        rows = requests(name, schema, params, 1)
        tables['request.' + name] = [tuple(tuple(v.encode()) if kind == 'bytes' else v for v, kind in zip(row, schema.values()))
                                     for row in rows]
    return tables


def answers(roots, data):
    cache = {}
    return {name: Counter(repr(tuple(row[n] for n in r.fields)) for row in evaluate(r, data, cache))
            for name, r in roots.items()}


def program(roots):
    compiler = MultiCompiler()
    return compiler.compile_many({name + '.answer': r for name, r in roots.items()})


def stats(binary, paths):
    out = subprocess.run([str(binary), *map(str, paths)], capture_output=True, text=True, check=True).stdout
    return [json.loads(line) for line in out.splitlines()]


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--plan-stats', type=Path, required=True, help='built examples/plan_stats binary')
    parser.add_argument('--output', type=Path, help='new directory for the DDP programs and report.json')
    args = parser.parse_args()
    if args.output:
        args.output.mkdir(parents=True)
    output = args.output or Path(tempfile.mkdtemp(prefix='ddir-sharing-'))

    data = fixture()
    fresh, shared = plans(False), plans(True)
    expected = answers(fresh, data)
    spj_stats = Counter()
    levels = {'separate': fresh, 'combined': fresh, 'context': shared}
    levels['hashcons'] = hashcons(shared)
    levels['canonical'] = hashcons(canonical(shared, pushdown=True))
    levels['spj'] = hashcons(spj(shared, spj_stats, pushdown=True))
    levels['canonical-inplace'] = hashcons(canonical(shared, pushdown=False))
    levels['spj-inplace'] = hashcons(spj(shared, Counter(), pushdown=False))
    for level in ('hashcons', 'canonical', 'spj', 'canonical-inplace', 'spj-inplace'):
        got = answers(levels[level], data)
        wrong = [n for n in CATALOGUE if got[n] != expected[n]]
        assert not wrong, f'{level}: answers differ from the original plans for {wrong}'
    empty = [n for n in CATALOGUE if not expected[n]]

    proxy = {level: egraph_proxy(levels[level]) for level in ('hashcons', 'canonical-inplace')}
    report = dict(egraph_proxy=proxy, fixture_rows=sum(len(v) for k, v in data.items() if not k.startswith('request.')),
                  empty_answers=empty, spj=dict(spj_stats), levels={})
    for level, roots in levels.items():
        directory = output / level
        directory.mkdir()
        if level in ('separate', 'canonical', 'spj', 'canonical-inplace', 'spj-inplace'):
            paths = []
            for name in CATALOGUE:
                path = directory / f'{name}.ddp'
                path.write_text(program({name: roots[name]}))
                paths.append(path)
            per_query = {name: s for name, s in zip(CATALOGUE, stats(args.plan_stats, paths))}
            report['levels'][level + ' (per query)'] = dict(
                arrangements=sum(s['optimized']['arrangements'] for s in per_query.values()),
                ops=sum(s['optimized']['ops'] for s in per_query.values()),
                per_query={n: dict(arrangements=s['optimized']['arrangements'], ops=s['optimized']['ops'],
                                   lowered_ops=s['lowered']['ops'], iterating=s['optimized']['iterating'],
                                   plan_us=s['parse_us'] + s['lower_us'] + s['optimize_us'])
                           for n, s in per_query.items()})
        if level != 'separate':
            path = directory / 'all.ddp'
            path.write_text(program(roots))
            [s] = stats(args.plan_stats, [path])
            report['levels'][level] = dict(arrangements=s['optimized']['arrangements'], ops=s['optimized']['ops'],
                                           iterating=s['optimized']['iterating'],
                                           plan_us=s['parse_us'] + s['lower_us'] + s['optimize_us'])
    (output / 'report.json').write_text(json.dumps(report, indent=2) + '\n')

    print(f'{len(CATALOGUE)} queries, answers checked on the fixture ({report["fixture_rows"]} rows); '
          f'empty answers: {", ".join(empty) or "none"}')
    print(f'spj blocks rebuilt: {spj_stats["blocks"]}, left canonical: {spj_stats["fallback"]}')
    print(f'{"level":<31}{"arrangements":>14}{"ops":>8}{"iterating":>11}{"plan ms":>9}')
    for level, s in report['levels'].items():
        iterating = s.get('iterating', sum(q['iterating'] for q in s.get('per_query', {}).values()))
        plan_us = s.get('plan_us', sum(q['plan_us'] for q in s.get('per_query', {}).values()))
        print(f'{level:<31}{s["arrangements"]:>14}{s["ops"]:>8}{iterating:>11}{plan_us / 1000:>9.1f}')
    for level, p in proxy.items():
        print(f'e-graph proxy over {level}: {p["by_identity"]} arranged inputs by identity, '
              f'{p["by_signature"]} by signature ({p["merged_groups"]} groups merge {p["merged_members"]} inputs), '
              f'{p["by_relaxed"]} ignoring single-atom filters and non-key fields')
    print(f'Report: {output / "report.json"}')


if __name__ == '__main__':
    main()
