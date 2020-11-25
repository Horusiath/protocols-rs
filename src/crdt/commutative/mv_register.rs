use serde::{Serialize, Deserialize};
use crate::PID;
use crate::crdt::commutative::event::Versioned;
use serde::export::PhantomData;
use smallvec::alloc::collections::BTreeSet;
use crate::crdt::convergent::Materialize;
use smallvec::SmallVec;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MVRegister<T>(BTreeSet<Versioned<T>>);

impl<T> Default for MVRegister<T>{
    fn default() -> Self {
        MVRegister(BTreeSet::default())
    }
}

impl<'m, T: 'm> Materialize<'m> for MVRegister<T> {
    type Value = SmallVec<[&'m T; 2]>;

    fn value(&'m self) -> Self::Value {
        self.0.iter().map(|v| &v.value).collect()
    }
}