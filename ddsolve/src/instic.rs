//! Instance builders: turn problems into a `Csp` (forbidden-pair encoding).

use crate::types::{canon, Csp, Val, VarId};

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
    forbidden.sort();
    forbidden.dedup();
    Csp { n_vars, domain, forbidden }
}

/// A scheduling instance. Tasks are variables; their domain is the set of nodes
/// (machines). Constraints, all reduced to binary forbidden pairs:
/// - affinity(t, u): tasks t and u must share a node  -> forbid every (t=p, u=q) with p != q.
/// - anti_affinity(t, u): tasks t and u must NOT share a node -> forbid every (t=p, u=p).
/// - exclusive(t, u, p): tasks t and u cannot both sit on node p -> forbid (t=p, u=p) for that p.
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
                if p != q {
                    forbidden.push(canon(t, p, u, q));
                }
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
    forbidden.sort();
    forbidden.dedup();
    Csp { n_vars: n_tasks, domain: n_nodes, forbidden }
}
