use crate::vtime::VTime;
use smallvec::alloc::collections::BTreeMap;
use crate::crdt::convergent::Convergent;
use crate::PID;
use serde::{Serialize, Deserialize};

/// Matrix clock.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MTime(BTreeMap<PID, VTime>);

impl MTime {

    pub fn get(&self, id: &PID) -> Option<&VTime> {
        self.0.get(id)
    }

    pub fn get_mut(&mut self, id: &PID) -> Option<&mut VTime> {
        self.0.get_mut(id)
    }

    pub fn replace(&mut self, id: PID, time: VTime) -> Option<VTime> {
        self.0.insert(id, time)
    }

    pub fn merge_vtime(&mut self, id: PID, time: &VTime) -> bool {
        let e = self.0.entry(id).or_default();
        e.merge(time)
    }

    pub fn min(&self) -> VTime {
        self.0.iter().fold(VTime::default(), |acc, (_, time)| {
            acc.min(time)
        })
    }

    pub fn max(&self) -> VTime {
        self.0.iter().fold(VTime::default(), |acc, (_, time)| {
            acc.max(time)
        })
    }
}

impl Default for MTime {
    fn default() -> Self {
        MTime(BTreeMap::new())
    }
}

impl Convergent for MTime {
    fn merge(&mut self, other: &Self) -> bool {
        let mut changed = false;
        for (&id, time) in other.0.iter() {
            let e = self.0.entry(id).or_default();
            changed = e.merge(time) || changed;
        }
        changed
    }
}

pub type Iter<'a> = smallvec::alloc::collections::btree_map::Iter<'a, u32, u64>;