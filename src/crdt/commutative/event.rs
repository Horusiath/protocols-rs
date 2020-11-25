use serde::{Serialize, Deserialize};
use crate::PID;
use crate::hlc::HybridTime;
use crate::vtime::VTime;
use std::cmp::Ordering;
use std::convert::TryInto;
use serde::de::DeserializeOwned;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    origin: PID,
    origin_seq_nr: u64,
    local_seq_nr: u64,
    sys_time: HybridTime,
    vec_time: VTime,
    payload: Vec<u8>
}

impl Event {
    pub fn new(origin: PID, origin_seq_nr: u64, local_seq_nr: u64, sys_time: HybridTime, vec_time: VTime, payload: Vec<u8>) -> Self {
        Event {
            origin,
            origin_seq_nr,
            local_seq_nr,
            sys_time,
            vec_time,
            payload
        }
    }
}

impl<T: DeserializeOwned> TryInto<Versioned<T>> for Event {
    type Error = serde_cbor::Error;

    fn try_into(self) -> Result<Versioned<T>, Self::Error> {
        let value : T = serde_cbor::from_slice(self.payload.as_slice())?;
        Ok(Versioned {
            origin: self.origin,
            sys_time: self.sys_time,
            vec_time: self.vec_time,
            value
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Versioned<T> {
    pub origin: PID,
    pub sys_time: HybridTime,
    pub vec_time: VTime,
    pub value: T
}

impl<T> Versioned<T> {
    pub fn new(origin: PID, sys_time: HybridTime, vec_time: VTime, value: T) -> Self {
        Versioned {
            origin,
            sys_time,
            vec_time,
            value,
        }
    }
}

impl<T> Ord for Versioned<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.vec_time.partial_cmp(&other.vec_time) {
            None => {
                match self.sys_time.cmp(&other.sys_time) {
                    Ordering::Equal => {
                        self.origin.cmp(&other.origin)
                    },
                    ord => ord
                }
            },
            Some(ord) => ord
        }

    }
}

impl<T> Eq for Versioned<T> { }

impl<T> PartialOrd for Versioned<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> PartialEq for Versioned<T> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}