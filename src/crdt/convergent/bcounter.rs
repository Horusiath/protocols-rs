use crate::PID;
use crate::crdt::convergent::{Convergent, DeltaConvergent, Materialize};
use serde::{Serialize,Deserialize};
use smallvec::alloc::collections::BTreeMap;
use crate::crdt::convergent::gcounter::GCounter;
use crate::crdt::convergent::pncounter::PNCounter;
use crate::crdt::convergent::pncounter;

/// A bounded counter, that can be increased or decreased, but which total value can never drop
/// below 0. Since it's possible to run into situation, where it's not possible to decrement
/// counter's value safely, the corresponding `add` operation returns result, which indicates
/// possible failure.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BCounter {
    counter: PNCounter,
    transfers: BTreeMap<(PID, PID), u64>,

    #[serde(skip_serializing, skip_deserializing)]
    transfers_delta: Option<BTreeMap<(PID, PID), u64>>,
}

impl BCounter {

    /// Increments a counter by a given value. This value can be negative, but in this case an
    /// operation may fail if there's possible risk, that in result of decrement, a counter value
    /// would turn negative.
    pub fn add(&mut self, id: PID, delta: i64) -> crate::Result<()> {
        if delta > 0 {
            Ok(self.counter.add(id, delta))
        } else if delta < 0 {
            let available = self.quota(&id);
            if available >= (-delta as u64) {
                Ok(self.counter.add(id, delta))
            } else {
                Err(anyhow::anyhow!("Cannot subtract {} from replica ({}): maximum available quota of {} for that replica has been surpassed.", delta, id, available))
            }
        } else {
            Ok(())
        }
    }

    /// Transfer the quota from one PID to another. In that case a `recipient` node will be able to
    /// be decremented by possibly higher number without failure, at ta cost of reducing that
    /// decrement capability on the `sender` node.
    pub fn transfer(&mut self, sender: PID, recipient: PID, quota: u64) -> crate::Result<()> {
        let available = self.quota(&sender);
        if quota < available {
            let e = self.transfers.entry((sender, recipient)).or_default();
            *e = *e + quota;

            // update delta as well
            let mut delta = self.transfers_delta.take().unwrap_or_default();
            let e = delta.entry((sender, recipient)).or_default();
            *e = *e + quota;
            self.transfers_delta = Some(delta);

            Ok(())
        } else {
            Err(anyhow::anyhow!("Cannot transfer {} from replica ({}) to ({}): maximum available quota of {} for that replica has been surpassed.", quota, sender, recipient, available))
        }
    }

    /// Get current quota at a given node. Quota describes the maximum available decrement number,
    /// that can be safely performed on that node when doing [add] operation. It's possible to
    /// transfer quota from one node to another using [transfer] function.
    pub fn quota(&self, id: &PID) -> u64 {
        self.transfers.iter().fold(self.counter.get(id) as u64, |acc, ((src, dst), v)| {
            if src == id { acc - v }
            else if dst == id { acc + v }
            else { acc }
        })
    }
}

impl Default for BCounter {
    fn default() -> Self {
        BCounter {
            counter: PNCounter::default(),
            transfers: BTreeMap::new(),
            transfers_delta: None,
        }
    }
}

impl Convergent for BCounter {
    fn merge(&mut self, other: &Self) -> bool {
        let counter_changed = self.counter.merge(&other.counter);
        let mut transfers_changed = false;
        for (key, value) in other.transfers.iter() {
            let e = self.transfers.entry(key.clone()).or_default();
            if *e < *value {
                *e = *value;
                transfers_changed = true;
            }
        }
        counter_changed || transfers_changed
    }
}

impl DeltaConvergent for BCounter {
    type Delta = Delta;

    fn delta(&mut self) -> Option<Self::Delta> {
        let counter = self.counter.delta();
        let transfers = self.transfers_delta.take();
        if counter.is_none() && transfers.is_none() {
            None
        } else {
            Some(Delta { counter, transfers })
        }
    }

    fn merge_delta(&mut self, other: &Self::Delta) -> bool {
        let mut changed = other.counter.as_ref()
            .map(|c| self.counter.merge_delta(c))
            .unwrap_or(false);

        if let Some(transfers) = &other.transfers {
            for (k, v) in transfers {
                let e = self.transfers.entry(*k).or_default();
                if v > e {
                    *e = *v;
                    changed = true;
                }
            }
        }

        changed
    }
}

impl<'m> Materialize<'m> for BCounter {
    type Value = u64;

    fn value(&'m self) -> Self::Value {
        self.counter.value() as u64
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Delta {
    counter: Option<pncounter::Delta>,
    transfers: Option<BTreeMap<(PID, PID), u64>>,
}

impl Default for Delta {
    fn default() -> Self {
        Delta { counter: None, transfers: None }
    }
}

impl Convergent for Delta {
    fn merge(&mut self, other: &Self) -> bool {
        let counter_changed = self.counter.merge(&other.counter);
        if let Some(t2) = other.transfers.as_ref() {
            if let Some(mut t1) = self.transfers.take() {
                let mut transfers_changed = false;
                for (&k, &v) in t2.iter() {
                    let e = t1.entry(k).or_default();
                    if v > *e {
                        *e = v;
                        transfers_changed = true;
                    }
                }
                self.transfers = Some(t1);
                counter_changed || transfers_changed
            } else {
                self.transfers = Some(t2.clone());
                true
            }
        } else {
            counter_changed
        }
    }
}


#[cfg(test)]
mod test {
    use crate::crdt::convergent::bcounter::BCounter;
    use crate::crdt::convergent::{Materialize, Convergent, DeltaConvergent};
    use crate::PID;

    const A: PID = 1;
    const B: PID = 2;
    const C: PID = 3;

    #[test]
    fn bcounter_identity() {
        let a = BCounter::default();
        assert_eq!(a.value(), 0);
    }

    #[test]
    fn bcounter_idempotency() {
        let mut a = BCounter::default();
        assert!(a.add(A, 2).is_ok());
        assert!(a.add(A, -1).is_ok()); // we use local quota

        let b = a.clone();

        assert_eq!(a.value(), 1);
        assert!(!a.merge(&b));
        assert_eq!(a.value(), 1);
    }

    #[test]
    fn bcounter_associativity() {
        let mut a = BCounter::default();
        assert!(a.add(A, 5).is_ok());
        let mut b = BCounter::default();
        assert!(b.add(B, 1).is_ok());
        let mut c = BCounter::default();
        assert!(c.add(C, 3).is_ok());

        let mut a2 = a.clone();
        let mut b2 = b.clone();
        let c2 = c.clone();

        // (a + b) + c
        assert!(a.merge(&b));
        assert!(a.merge(&c));
        assert_eq!(a.value(), 9);

        // a + (b + c)
        assert!(b2.merge(&c2));
        assert!(a2.merge(&b2));
        assert_eq!(a2.value(), 9);

        assert!(!a.merge(&a2));
    }

    #[test]
    fn bcounter_commutativity() {
        let mut a = BCounter::default();
        assert!(a.add(A, 2).is_ok());
        let mut b = BCounter::default();
        assert!(b.add(B, 1).is_ok());

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
    fn bcounter_transfer() {
        let mut a = BCounter::default();
        assert!(a.add(A, 5).is_ok());
        let mut b = BCounter::default();
        assert!(b.add(B, 1).is_ok());
        assert!(b.add(B, -3).is_err()); // not enough quota (1)

        // B doesn't have enough quota, so we'll try to transfer some of it from A
        assert!(a.transfer(A, B, 6).is_err()); // not enough quota(5)
        assert!(a.transfer(A, B, 2).is_ok());

        // we made a successful transfer, but that should not touch actual value
        assert_eq!(a.value(), 5);
        assert_eq!(b.value(), 1);

        // merge to make B aware of transferred quota
        assert!(b.merge(&a));
        assert_eq!(b.value(), 6); // we only tranferred quota, value is unchanged

        // now we should be free to -3
        assert!(b.add(B, -3).is_ok());
        assert_eq!(b.value(), 3);
    }

    #[test]
    fn bcounter_non_negative() {
        let mut a = BCounter::default();
        assert!(a.add(A, 3).is_ok());
        let mut b = BCounter::default();
        assert!(b.add(B, 2).is_ok());

        let da = a.delta().expect("bcounter delta: a");
        assert!(b.merge_delta(&da));

        assert!(a.add(A, -3).is_ok());
        assert!(b.add(A, -5).is_err()); // we cannot count below 0

        let db = b.delta().expect("bcounter delta: b");
        assert!(a.merge_delta(&db));

        assert_eq!(a.value(), 2);
    }
}