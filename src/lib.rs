#![feature(btree_drain_filter)]
#![feature(smart_ptr_as_ref)]

use std::time::SystemTime;

pub mod raft;
pub mod crdt;
pub mod vtime;
pub mod transport;
pub mod mtime;
pub mod hlc;
pub mod dotted_version;
pub mod paxos;

pub type Result<T> = anyhow::Result<T>;

pub trait Clock {
    fn now() -> Self;
}

impl Clock for SystemTime {
    fn now() -> Self {
        SystemTime::now()
    }
}