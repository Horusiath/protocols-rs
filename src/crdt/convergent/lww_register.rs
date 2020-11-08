use crate::vtime::ReplicaId;
use crate::crdt::convergent::{Convergent, Materialize, DeltaConvergent};
use serde::{Serialize, Deserialize};
use crate::hlc::HybridTime;
use serde::export::PhantomData;
use crate::Clock;
use std::cmp::Ordering;

/// Last write wins register.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LWWRegister<T, C>(Option<Delta<T>>, PhantomData<C>);

impl<T> LWWRegister<T, HybridTime> {

    pub fn with_hybrid_clock() -> Self {
        LWWRegister(None, PhantomData::default())
    }
}

impl<T, C> LWWRegister<T, C> where C: Clock {

    pub fn is_empty(&self) -> bool { self.0.is_none() }

    pub fn assign(&mut self, id: ReplicaId, value: T) {
        let now = HybridTime::now();
        if let Some(e) = self.0.as_mut() {
            match e.timestamp.cmp(&now) {
                Ordering::Greater => {}
                Ordering::Equal => {
                    if e.replica_id < id {
                        e.value = value;
                        e.replica_id = id;
                    }
                }
                Ordering::Less => {
                    e.timestamp = now;
                    e.replica_id = id;
                    e.value = value;
                }
            }
        } else {
            self.0.replace(Delta {
                value,
                timestamp: now,
                replica_id: id,
            });
        }
    }
}

impl<T, C> Default for LWWRegister<T, C> {
    fn default() -> Self {
        LWWRegister(None, PhantomData::default())
    }
}

impl<T: Clone, C> Convergent for LWWRegister<T, C> {
    fn merge(&mut self, other: &Self) -> bool {
        if let Some(v2) = other.0.as_ref() {
            self.merge_delta(v2)
        } else {
            false
        }
    }
}

impl<T: Clone, C> DeltaConvergent for LWWRegister<T, C> {
    type Delta = Delta<T>;

    fn delta(&mut self) -> Option<Self::Delta> {
        self.0.clone()
    }

    fn merge_delta(&mut self, other: &Self::Delta) -> bool {
        if let Some(this) = self.0.as_mut() {
            this.merge(&other)
        } else {
            self.0.replace(other.clone());
            true
        }
    }
}

impl<'mat, T, C> Materialize for &'mat LWWRegister<T, C> {
    type Value = Option<&'mat T>;

    fn value(&self) -> Self::Value {
        self.0.as_ref().map(|v| & v.value)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Delta<T> {
    value: T,
    timestamp: HybridTime,
    replica_id: ReplicaId,
}

impl<T: Clone> Convergent for Delta<T> {
    fn merge(&mut self, other: &Self) -> bool {
        match self.timestamp.cmp(&other.timestamp) {
            Ordering::Greater => false,
            Ordering::Less => {
                self.value = other.value.clone();
                self.timestamp = other.timestamp;
                self.replica_id = other.replica_id;
                true
            },
            Ordering::Equal => {
                if self.replica_id > other.replica_id {
                    self.value = other.value.clone();
                    self.timestamp = other.timestamp;
                    self.replica_id = other.replica_id;
                    true
                } else {
                    false
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use crate::crdt::convergent::{Materialize, Convergent, DeltaConvergent};
    use crate::vtime::ReplicaId;
    use crate::hlc::HybridTime;
    use crate::crdt::convergent::lww_register::LWWRegister;

    const A: ReplicaId = 1;
    const B: ReplicaId = 2;
    const C: ReplicaId = 3;

    impl Convergent for &str {
        fn merge(&mut self, other: &Self) -> bool {
            if (self == other) {
                false
            } else {
                panic!("merging different values")
            }
        }
    }

    #[test]
    fn lww_register_identity() {
        let a: LWWRegister<u32, HybridTime> = LWWRegister::with_hybrid_clock();
        assert!(a.is_empty());
        assert_eq!((&a).value(), None);
    }

    #[test]
    fn lww_register_idempotency() {
        let mut a = LWWRegister::with_hybrid_clock();
        a.assign(A, "hello");

        let b = a.clone();

        assert_eq!((&a).value(), Some(&"hello"));
        assert!(!a.merge(&b));
        assert_eq!((&a).value(), Some(&"hello"));
    }

    #[test]
    fn lww_register_associativity() {
        let mut a = LWWRegister::with_hybrid_clock();
        a.assign(A, "A");
        let mut b = LWWRegister::with_hybrid_clock();
        b.assign(B, "B");
        let mut c = LWWRegister::with_hybrid_clock();
        c.assign(C, "C");

        let mut a2 = a.clone();
        let mut b2 = b.clone();
        let c2 = c.clone();

        // (a + b) + c
        assert!(a.merge(&b));
        assert!(a.merge(&c));
        assert_eq!((&a).value(), Some(&"C"));

        // a + (b + c)
        assert!(b2.merge(&c2));
        assert!(a2.merge(&b2));
        assert_eq!((&a2).value(), Some(&"C"));

        assert!(!a.merge(&a2));
    }

    #[test]
    fn lww_register_commutativity() {
        let mut a = LWWRegister::with_hybrid_clock();
        a.assign(A, "A");
        let mut b = LWWRegister::with_hybrid_clock();
        b.assign(B, "B");

        let mut a2 = a.clone();
        let mut b2 = b.clone();

        // a + b
        assert!(a.merge(&b));
        assert_eq!((&a).value(), Some(&"B"));

        // b + a
        assert!(!b2.merge(&a2));
        assert_eq!((&b2).value(), Some(&"B"));

        assert!(!a.merge(&b2));
    }
}