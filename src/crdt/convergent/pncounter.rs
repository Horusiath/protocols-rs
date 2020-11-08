use crate::crdt::convergent::gcounter;
use crate::crdt::convergent::gcounter::{GCounter};
use crate::crdt::convergent::{DeltaConvergent, Convergent, Materialize};
use crate::vtime::ReplicaId;
use serde::{Serialize,Deserialize};

/// A positive-negative counter. It's a distributed eventually consistent counter, which value can
/// be concurrently incremented or decremented over multiple replicas.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PNCounter {
    inc: GCounter,
    dec: GCounter,
}

impl PNCounter {
    pub fn add(&mut self, id: ReplicaId, value: i64) {
        if value > 0 {
            self.inc.add(id, value as u64)
        } else if value < 0 {
            self.dec.add(id, (-value) as u64)
        }
    }

    /// Returns partial counter value at given replica `id`.
    pub fn get(&self, id: &ReplicaId) -> i64 {
        let inc = self.inc.get(id);
        let dec =  self.dec.get(id);
        (inc as i64) - (dec as i64)
    }

    pub fn is_empty(&self) -> bool { self.inc.is_empty() && self.dec.is_empty() }
}

impl Default for PNCounter {
    fn default() -> Self {
        PNCounter {
            inc: GCounter::default(),
            dec: GCounter::default(),
        }
    }
}

impl Convergent for PNCounter {
    fn merge(&mut self, other: &Self) -> bool {
        let inc_changed = self.inc.merge(&other.inc);
        let dec_changed = self.dec.merge(&other.dec);
        inc_changed || dec_changed
    }
}

impl DeltaConvergent for PNCounter {
    type Delta = Delta;

    fn delta(&mut self) -> Option<Self::Delta> {
        match (self.inc.delta(), self.dec.delta()) {
            (None, None) => None,
            (Some(inc), Some(dec)) => Some(Delta(inc, dec)),
            (Some(inc), None) => Some(Delta(inc, gcounter::Delta::default())),
            (None, Some(dec)) => Some(Delta(gcounter::Delta::default(), dec)),
        }
    }

    fn merge_delta(&mut self, other: &Self::Delta) -> bool {
        let inc_changed = self.inc.merge_delta(&other.0);
        let dec_changed = self.dec.merge_delta(&other.1);
        inc_changed || dec_changed
    }
}

impl Materialize for PNCounter {
    type Value = i64;

    fn value(&self) -> Self::Value {
        (self.inc.value() as i64) - (self.dec.value() as i64)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Delta(gcounter::Delta, gcounter::Delta);

impl Default for Delta {
    fn default() -> Self {
        Delta(gcounter::Delta::default(), gcounter::Delta::default())
    }
}

impl Convergent for Delta {
    fn merge(&mut self, other: &Self) -> bool {
        let inc_changed = self.0.merge(&other.0);
        let dec_changed = self.1.merge(&other.1);
        inc_changed || dec_changed
    }
}

#[cfg(test)]
mod test {
    use crate::crdt::convergent::{Materialize, Convergent, DeltaConvergent};
    use crate::vtime::ReplicaId;
    use crate::crdt::convergent::pncounter::PNCounter;

    const A: ReplicaId = 1;
    const B: ReplicaId = 2;
    const C: ReplicaId = 3;

    #[test]
    fn pncounter_identity() {
        let a = PNCounter::default();
        assert_eq!(a.value(), 0);
        assert!(a.is_empty());
    }

    #[test]
    fn pncounter_idempotency() {
        let mut a = PNCounter::default();
        a.add(A, 2);
        a.add(B, -1);

        let b = a.clone();

        assert_eq!(a.value(), 1);
        assert!(!a.merge(&b));
        assert_eq!(a.value(), 1);
    }

    #[test]
    fn pncounter_associativity() {
        let mut a = PNCounter::default();
        a.add(A, 5);
        let mut b = PNCounter::default();
        b.add(B, 1);
        let mut c = PNCounter::default();
        c.add(C, -3);

        let mut a2 = a.clone();
        let mut b2 = b.clone();
        let c2 = c.clone();

        // (a + b) + c
        assert!(a.merge(&b));
        assert!(a.merge(&c));
        assert_eq!(a.value(), 3);

        // a + (b + c)
        assert!(b2.merge(&c2));
        assert!(a2.merge(&b2));
        assert_eq!(a2.value(), 3);

        assert!(!a.merge(&a2));
    }

    #[test]
    fn pncounter_commutativity() {
        let mut a = PNCounter::default();
        a.add(A, -2);
        let mut b = PNCounter::default();
        b.add(B, 1);

        let mut a2 = a.clone();
        let mut b2 = b.clone();

        // a + b
        assert!(a.merge(&b));
        assert_eq!(a.value(), -1);

        // b + a
        assert!(b2.merge(&a2));
        assert_eq!(b2.value(), -1);

        assert!(!a.merge(&b2));
    }

    #[test]
    fn pncounter_delta() {
        let mut a = PNCounter::default();
        let mut b = PNCounter::default();

        a.add(A, 2);

        let delta = a.delta().expect("a: delta");
        assert!(b.merge_delta(&delta));
        assert_eq!(a.value(), b.value());

        // after obtaining delta, inner delta should be empty
        assert!(a.delta().is_none());

        a.add(A, -1);
        let delta = a.delta().expect("a: delta");
        assert!(b.merge_delta(&delta));
        assert_eq!(a.value(), b.value());
    }
}