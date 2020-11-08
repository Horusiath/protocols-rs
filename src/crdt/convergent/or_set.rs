use crate::crdt::convergent::kernel::{Kernel};
use serde::{Serialize, Deserialize};
use crate::crdt::convergent::{Materialize, Convergent, DeltaConvergent, kernel};
use crate::vtime::{ReplicaId, Dot};
use smallvec::SmallVec;
use smallvec::alloc::collections::{BTreeSet, BTreeMap};
use std::rc::Rc;

#[derive(Clone, Debug, Serialize)]
pub struct ORSet<T: Ord>(Kernel<T>);

impl<T: Ord> ORSet<T> {
    pub fn insert(&mut self, id: ReplicaId, value: T) {
        self.0.insert(id, Rc::new(value));
    }

    pub fn remove(&mut self, value: &T) {
        self.0.remove(value);
    }

    pub fn is_empty(&self) -> bool { self.0.is_empty() }

    pub fn len(&self) -> usize { self.0.len() }
}

impl<T: Ord> Default for ORSet<T> {
    fn default() -> Self {
        ORSet(Kernel::default())
    }
}

impl<'mat, T: Ord> Materialize for &'mat ORSet<T> where T: Ord {
    type Value = BTreeSet<&'mat T>;

    fn value(&self) -> Self::Value {
        let kernel = &self.0;
        kernel.value().collect()
    }
}

impl<T: Ord> Convergent for ORSet<T> {
    fn merge(&mut self, other: &Self) -> bool {
        self.0.merge(&other.0)
    }
}

impl<T: Ord> DeltaConvergent for ORSet<T> {
    type Delta = Delta<T>;

    fn delta(&mut self) -> Option<Self::Delta> {
        self.0.delta()
    }

    fn merge_delta(&mut self, other: &Self::Delta) -> bool {
        self.0.merge_delta(other)
    }
}

pub type Delta<T> = kernel::Delta<T>;

#[cfg(test)]
mod test {
    use crate::crdt::convergent::or_set::ORSet;
    use crate::crdt::convergent::{Materialize, Convergent, DeltaConvergent};
    use crate::vtime::ReplicaId;
    use smallvec::alloc::collections::BTreeSet;

    const A: ReplicaId = 1;
    const B: ReplicaId = 2;
    const C: ReplicaId = 3;


    #[test]
    fn orset_identity() {
        let a: ORSet<u32> = ORSet::default();
        assert!(a.is_empty());
        assert_eq!((&a).value(), BTreeSet::new());
    }

    #[test]
    fn orset_idempotency() {
        let mut a = ORSet::default();
        a.insert(A, "hello");

        let b = a.clone();

        let mut expected = BTreeSet::new();
        expected.insert(&"hello");
        assert_eq!((&a).value(), expected);
        assert!(!a.merge(&b));
        assert_eq!((&a).value(), expected);
    }

    #[test]
    fn orset_associativity() {
        let mut a = ORSet::default();
        a.insert(A, "A");
        let mut b = ORSet::default();
        b.insert(B, "B");
        let mut c = ORSet::default();
        c.insert(C, "C");

        let mut a2 = a.clone();
        let mut b2 = b.clone();
        let c2 = c.clone();

        let mut expected = BTreeSet::new();
        expected.insert(&"A");
        expected.insert(&"B");
        expected.insert(&"C");

        // (a + b) + c
        assert!(a.merge(&b));
        assert!(a.merge(&c));
        assert_eq!((&a).value(), expected);

        // a + (b + c)
        assert!(b2.merge(&c2));
        assert!(a2.merge(&b2));
        assert_eq!((&a2).value(), expected);

        assert!(!a.merge(&a2));
    }

    #[test]
    fn orset_commutativity() {
        let mut a = ORSet::default();
        a.insert(A, "A");
        let mut b = ORSet::default();
        b.insert(B, "B");

        let mut a2 = a.clone();
        let mut b2 = b.clone();

        let mut expected = BTreeSet::new();
        expected.insert(&"A");
        expected.insert(&"B");

        // a + b
        assert!(a.merge(&b));
        assert_eq!((&a).value(), expected);

        // b + a
        assert!(b2.merge(&a2));
        assert_eq!((&b2).value(), expected);

        assert!(!a.merge(&b2));
    }

    #[test]
    fn orset_add_wins() {
        let mut a = ORSet::default();
        a.insert(A, "A");
        let mut b = ORSet::default();
        b.insert(B, "B");

        assert!(a.merge(&b));

        a.insert(A, "B");
        b.remove(&"B");

        let mut expected = BTreeSet::new();
        expected.insert(&"A");
        expected.insert(&"A");

        assert!(a.merge(&b));
        assert_eq!((&a).value(), expected);
    }
}