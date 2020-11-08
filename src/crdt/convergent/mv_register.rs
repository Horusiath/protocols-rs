use crate::vtime::ReplicaId;
use crate::crdt::convergent::{Convergent, Materialize, DeltaConvergent, kernel};
use crate::crdt::convergent::kernel::{Kernel, Value};
use serde::{Serialize, Deserialize};
use std::rc::Rc;

/// Multi-value register.
#[derive(Clone, Debug, Serialize)]
pub struct MVRegister<T: Ord>(Kernel<T>);

impl<T: Ord> MVRegister<T> {

    pub fn is_empty(&self) -> bool { self.0.is_empty() }

    pub fn assign(&mut self, id: ReplicaId, value: T) {
        self.0.clear();
        self.0.insert(id, Rc::new(value));
    }
}

impl<T: Ord> Default for MVRegister<T> {
    fn default() -> Self {
        MVRegister(Kernel::default())
    }
}

impl<'mat, T: Ord> Materialize for &'mat MVRegister<T> {
    type Value = Value<'mat, T>;

    fn value(&self) -> Self::Value {
        let kernel = &self.0;
        kernel.value()
    }
}

impl<T: Ord> Convergent for MVRegister<T> {
    fn merge(&mut self, other: &Self) -> bool {
        self.0.merge(&other.0)
    }
}

impl<T: Ord> DeltaConvergent for MVRegister<T> {
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
    use crate::crdt::convergent::mv_register::MVRegister;
    use crate::crdt::convergent::{Materialize, Convergent, DeltaConvergent};
    use crate::vtime::ReplicaId;

    const A: ReplicaId = 1;
    const B: ReplicaId = 2;
    const C: ReplicaId = 3;

    #[test]
    fn mv_register_identity() {
        let a: MVRegister<u32> = MVRegister::default();
        let expected: Vec<&u32> = Vec::new();
        assert!(a.is_empty());
        assert_eq!((&a).value().collect::<Vec<&u32>>(), expected);
    }

    #[test]
    fn mv_register_idempotency() {
        let mut a = MVRegister::default();
        a.assign(A, "hello");

        let b = a.clone();

        assert_eq!((&a).value().collect::<Vec<&&str>>(), vec![&"hello"]);
        assert!(!a.merge(&b));
        assert_eq!((&a).value().collect::<Vec<&&str>>(), vec![&"hello"]);
    }

    #[test]
    fn mv_register_associativity() {
        let mut a = MVRegister::default();
        a.assign(A, "A");
        let mut b = MVRegister::default();
        b.assign(B, "B");
        let mut c = MVRegister::default();
        c.assign(C, "C");

        let mut a2 = a.clone();
        let mut b2 = b.clone();
        let c2 = c.clone();

        // (a + b) + c
        assert!(a.merge(&b));
        assert!(a.merge(&c));
        assert_eq!((&a).value().collect::<Vec<&&str>>(), vec![&"A", &"B", &"C"]);

        // a + (b + c)
        assert!(b2.merge(&c2));
        assert!(a2.merge(&b2));
        assert_eq!((&a2).value().collect::<Vec<&&str>>(), vec![&"A", &"B", &"C"]);

        assert!(!a.merge(&a2));
    }

    #[test]
    fn mv_register_commutativity() {
        let mut a = MVRegister::default();
        a.assign(A, "A");
        let mut b = MVRegister::default();
        b.assign(B, "B");

        let mut a2 = a.clone();
        let mut b2 = b.clone();

        // a + b
        assert!(a.merge(&b));
        assert_eq!((&a).value().collect::<Vec<&&str>>(), vec![&"A", &"B"]);

        // b + a
        assert!(b2.merge(&a2));
        assert_eq!((&b2).value().collect::<Vec<&&str>>(), vec![&"A", &"B"]);

        assert!(!a.merge(&b2));
    }

    #[test]
    fn mv_register_assign_override() {
        let mut a = MVRegister::default();
        a.assign(A, "A");
        let mut b = MVRegister::default();
        b.assign(B, "B");

        assert!(a.merge(&b));
        assert_eq!((&a).value().collect::<Vec<&&str>>(), vec![&"A", &"B"]);

        a.assign(A, "C");
        assert_eq!((&a).value().collect::<Vec<&&str>>(), vec![&"C"]);

        assert!(b.merge(&a));
        assert_eq!((&a).value().collect::<Vec<&&str>>(), vec![&"C"]);
    }

    #[test]
    fn mv_register_delta() {
        let mut a = MVRegister::default();
        a.assign(A, "A1");
        let mut b = MVRegister::default();

        assert!(b.merge_delta(&a.delta().expect("delta: A")));
        a.assign(A, "A2");
        assert!(b.merge_delta(&a.delta().expect("delta: A (second)")));

        assert_eq!((&a).value().collect::<Vec<&&str>>(), vec![&"A2"]);
        assert_eq!((&b).value().collect::<Vec<&&str>>(), vec![&"A2"]);
    }
}