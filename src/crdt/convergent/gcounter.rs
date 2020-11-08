use crate::vtime::{ReplicaId, VTime};
use crate::crdt::convergent::{Convergent, DeltaConvergent, Materialize};
use serde::{Serialize,Deserialize};

/// A grow-only counter. It's a distributed, eventually consistent counter, that can be incremented
/// concurrently on many replicas. It doesn't support decrement operations (see: `PNCounter`).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GCounter(Delta, #[serde(skip_serializing, skip_deserializing)]Option<Delta>);

impl GCounter {

    /// Increments current counter by a given delta.
    pub fn add(&mut self, id: ReplicaId, delta: u64) {
        let dot = self.0.0.inc_by(id, delta);

        let mut d = self.1.take().unwrap_or_else(|| Delta::default());
        d.0.set(dot);
        self.1 = Some(d);
    }

    /// Returns partial counter value at given replica `id`.
    pub fn get(&self, id: &ReplicaId) -> u64 {
        self.0.0.get(&id)
    }

    /// Checks if current counter contains any values.
    pub fn is_empty(&self) -> bool { self.0.0.is_empty() }
}

impl Default for GCounter {
    fn default() -> Self {
        GCounter(Delta::default(), None)
    }
}

impl Convergent for GCounter {
    fn merge(&mut self, other: &Self) -> bool {
        let changed = self.0.merge(&other.0);
        let delta_changed = self.1.merge(&other.1);
        changed || delta_changed
    }
}

impl DeltaConvergent for GCounter {
    type Delta = Delta;

    fn delta(&mut self) -> Option<Self::Delta> {
        self.1.take()
    }

    fn merge_delta(&mut self, other: &Self::Delta) -> bool {
        self.0.merge(other)
    }
}

impl Materialize for GCounter {
    type Value = u64;

    fn value(&self) -> Self::Value {
        self.0.0.iter().map(|(_,v)| *v).sum()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Delta(VTime);

impl Default for Delta {
    fn default() -> Self {
        Delta(VTime::default())
    }
}

impl Convergent for Delta {
    fn merge(&mut self, other: &Self) -> bool {
        self.0.merge(&other.0)
    }
}


#[cfg(test)]
mod test {
    use crate::crdt::convergent::bcounter::BCounter;
    use crate::crdt::convergent::{Materialize, Convergent, DeltaConvergent};
    use crate::vtime::ReplicaId;
    use crate::crdt::convergent::gcounter::GCounter;

    const A: ReplicaId = 1;
    const B: ReplicaId = 2;
    const C: ReplicaId = 3;

    #[test]
    fn gcounter_identity() {
        let a = GCounter::default();
        assert_eq!(a.value(), 0);
        assert!(a.is_empty());
    }

    #[test]
    fn gcounter_idempotency() {
        let mut a = GCounter::default();
        a.add(A, 2);
        a.add(B, 1);

        let b = a.clone();

        assert_eq!(a.value(), 3);
        assert!(!a.merge(&b));
        assert_eq!(a.value(), 3);
    }

    #[test]
    fn gcounter_associativity() {
        let mut a = GCounter::default();
        a.add(A, 2);
        let mut b = GCounter::default();
        b.add(B, 1);
        let mut c = GCounter::default();
        c.add(C, 3);

        let mut a2 = a.clone();
        let mut b2 = b.clone();
        let c2 = c.clone();

        // (a + b) + c
        assert!(a.merge(&b));
        assert!(a.merge(&c));
        assert_eq!(a.value(), 6);

        // a + (b + c)
        assert!(b2.merge(&c2));
        assert!(a2.merge(&b2));
        assert_eq!(a2.value(), 6);

        assert!(!a.merge(&a2));
    }

    #[test]
    fn gcounter_commutativity() {
        let mut a = GCounter::default();
        a.add(A, 2);
        let mut b = GCounter::default();
        b.add(B, 1);

        let mut a2 = a.clone();
        let mut b2 = b.clone();

        // a + b
        assert!(a.merge(&b));
        assert_eq!(a.value(), 3);

        // b + a
        assert!(b2.merge(&a2));
        assert_eq!(b2.value(), 3);

        assert!(!a.merge(&b2));
    }

    #[test]
    fn gcounter_delta() {
        let mut a = GCounter::default();
        let mut b = GCounter::default();

        a.add(A, 2);

        let delta = a.delta().expect("a: delta");
        assert!(b.merge_delta(&delta));
        assert_eq!(a.value(), b.value());

        // after obtaining delta, inner delta should be empty
        assert!(a.delta().is_none());

        a.add(A, 1);
        let delta = a.delta().expect("a: delta");
        assert!(b.merge_delta(&delta));
        assert_eq!(a.value(), b.value());
    }
}