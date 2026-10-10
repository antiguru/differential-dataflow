//! Count a program's scope-tree operators by kind, before and after `optimize`,
//! and time the planning pipeline the server runs (`server/src/cmd.rs::prepare`).
//!
//! Usage: `plan_stats <program.ddp>...`. Prints one JSON object per program.
//!
//! `arrangements` is `Arrange` plus `Reduce` nodes: each `Reduce` maintains an
//! arrangement of its output, and `collapse_arranges` removes the `Arrange` that
//! would otherwise re-arrange it.

use std::time::Instant;

use interactive::scope_ir::{Item, Node, Program, Scope};

#[derive(Default)]
struct Counts { linear: usize, concat: usize, arrange: usize, join: usize, reduce: usize, inspect: usize, scopes: usize, iterating: usize }

impl Counts {
    fn of(program: &Program) -> Self {
        let mut counts = Counts::default();
        counts.scope(&program.root);
        counts
    }
    fn scope(&mut self, scope: &Scope) {
        self.scopes += 1;
        if !scope.vars.is_empty() { self.iterating += 1; }
        for item in &scope.items {
            match item {
                Item::Sub(child) => self.scope(child),
                Item::Op(Node::Linear { .. }) => self.linear += 1,
                Item::Op(Node::Concat(_)) => self.concat += 1,
                Item::Op(Node::Arrange(_)) => self.arrange += 1,
                Item::Op(Node::Join { .. }) => self.join += 1,
                Item::Op(Node::Reduce { .. }) => self.reduce += 1,
                Item::Op(Node::Inspect { .. }) => self.inspect += 1,
            }
        }
    }
    fn json(&self) -> String {
        format!(
            "{{\"ops\": {}, \"arrangements\": {}, \"linear\": {}, \"concat\": {}, \"arrange\": {}, \"join\": {}, \"reduce\": {}, \"inspect\": {}, \"scopes\": {}, \"iterating\": {}}}",
            self.linear + self.concat + self.arrange + self.join + self.reduce + self.inspect,
            self.arrange + self.reduce,
            self.linear, self.concat, self.arrange, self.join, self.reduce, self.inspect, self.scopes, self.iterating,
        )
    }
}

fn main() {
    let paths: Vec<String> = std::env::args().skip(1).collect();
    assert!(!paths.is_empty(), "usage: plan_stats <program.ddp>...");
    fn timed<T>(f: impl FnOnce() -> T) -> (T, u128) {
        let start = Instant::now();
        let result = f();
        (result, start.elapsed().as_micros())
    }
    for path in paths {
        let source = interactive::load_program(&path);
        let (parsed, parse_us) = timed(|| interactive::parse::pipe::parse(&source));
        let (mut program, lower_us) = timed(|| interactive::lower::lower_tree(parsed));
        let before = Counts::of(&program);
        let ((), optimize_us) = timed(|| program.optimize());
        let after = Counts::of(&program);
        println!(
            "{{\"program\": {:?}, \"parse_us\": {}, \"lower_us\": {}, \"optimize_us\": {}, \"lowered\": {}, \"optimized\": {}}}",
            path, parse_us, lower_us, optimize_us, before.json(), after.json(),
        );
    }
}
