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
