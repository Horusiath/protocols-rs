use serde::{Serialize, Deserialize};
use crate::PID;
use crate::crdt::commutative::event::Versioned;
use crate::crdt::convergent::Materialize;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Counter(i64);

impl Default for Counter {
    fn default() -> Self {
        Counter(0)
    }
}

impl<'m> Materialize<'m> for Counter {
    type Value = i64;

    fn value(&'m self) -> Self::Value {
        self.0
    }
}