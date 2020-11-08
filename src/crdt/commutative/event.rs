use crate::hlc::HybridTime;
use crate::vtime::{VTime, ReplicaId};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Versioned<T> {
    pub origin: ReplicaId,
    pub sys_ts: HybridTime,
    pub vec_ts: VTime,
    pub data: T
}

impl<T> Versioned<T> {
    pub fn new(origin: ReplicaId, sys_ts: HybridTime, vec_ts: VTime, data: T) -> Self {
        Versioned {
            origin,
            sys_ts,
            vec_ts,
            data
        }
    }
}

impl<T> PartialOrd for Versioned<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> PartialEq for Versioned<T> {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other) == Some(Ordering::Equal)
    }
}

impl<T> Ord for Versioned<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.vec_ts.partial_cmp(&other.vec_ts) {
            None => {
                match self.sys_ts.cmp(&other.sys_ts) {
                    Ordering::Equal => {
                        self.origin.cmp(&other.origin)
                    },
                    ord => ord,
                }
            },
            Some(ord) => ord,
        }
    }
}

impl<T> Eq for Versioned<T> {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event<T> {
    pub origin: ReplicaId,
    pub origin_seq_nr: u64,
    pub local_seq_nr: u64,
    pub sys_ts: HybridTime,
    pub vec_ts: VTime,
    pub data: T
}

impl<T> Event<T> {
    pub fn new(origin: ReplicaId, origin_seq_nr: u64, local_seq_nr: u64, sys_ts: HybridTime, vec_ts: VTime, data: T) -> Self {
        Event {
            origin,
            origin_seq_nr,
            local_seq_nr,
            sys_ts,
            vec_ts,
            data
        }
    }
}

impl<T> Into<Versioned<T>> for Event<T> {
    fn into(self) -> Versioned<T> {
        Versioned::new(self.origin, self.sys_ts, self.vec_ts, self.data)
    }
}

#[cfg(test)]
mod test {

}