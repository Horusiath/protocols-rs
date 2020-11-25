use serde::{Serialize, Deserialize};
use crate::PID;
use crate::crdt::commutative::event::Versioned;
use serde::export::PhantomData;
use crate::crdt::convergent::Materialize;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LWWRegister<T>(Option<Versioned<T>>);

impl<T> Default for LWWRegister<T>{
    fn default() -> Self {
        LWWRegister(None)
    }
}

impl<'m, T: 'm> Materialize<'m> for LWWRegister<T> {
    type Value = Option<&'m T>;

    fn value(&'m self) -> Self::Value {
        self.0.as_ref().map(|v| &v.value)
    }
}