use crate::vtime::{VTime, Dot, ReplicaId};
use smallvec::alloc::collections::BTreeSet;
use crate::crdt::convergent::Convergent;
use serde::{Serialize, Deserialize};

/// A dotted version vector, that can be used to represent not only operations in a continuous
/// logical timeline, but also to represent detached events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DottedVersion(VTime, BTreeSet<Dot>);

impl DottedVersion {
    fn compress(&mut self) {
        let vtime = self.1.iter().fold(&mut self.0, |acc, dot| {
            let id = dot.id();
            let v = acc.get(&id);
            if dot.seq_nr() == v + 1 {
                acc.inc(id);
                acc
            } else {
                acc
            }
        });
        self.1.drain_filter(|d| vtime.contains(d));
        self.0 = vtime.clone();
    }

    /// Returns true, if a given `Dot` has been observed by a current dotter version vector.
    pub fn contains(&self, dot: &Dot) -> bool {
        self.0.contains(dot) || self.1.contains(dot)
    }

    pub fn inc_by(&mut self, key: ReplicaId, delta: u64) -> Dot {
        // we don't need to update dot cloud (self.1) as this function should only be called for
        // key that represent current replica and that entry is always up-to-date and should never
        // contain detached dots
        self.0.inc_by(key, delta)
    }

    #[inline]
    pub fn inc(&mut self, key: ReplicaId) -> Dot { self.inc_by(key, 1) }
}

impl Default for DottedVersion {
    fn default() -> Self {
        DottedVersion(VTime::default(), BTreeSet::new())
    }
}

impl Convergent for DottedVersion {
    fn merge(&mut self, other: &Self) -> bool {
        let vec_changed = self.0.merge(&other.0);
        let mut cloud_changed = false;
        for dot in other.1.iter() {
            cloud_changed = self.1.insert(*dot) || cloud_changed;
        }
        if cloud_changed {
            self.compress();
        }
        vec_changed || cloud_changed
    }
}