pub mod event;
pub mod counter;
pub mod lww_register;
pub mod mv_register;
pub mod log;

use smallvec::SmallVec;
use crate::vtime::VTime;
use crate::Result;
use crate::crdt::commutative::event::Versioned;

pub trait Commutative {
    type Operation;

    fn redundant(&self, v: Versioned<Self::Operation>) -> bool;

    fn apply(&mut self, v: Versioned<Self::Operation>) -> bool;

    fn prune(&mut self, timestamp: VTime) -> bool { false }
}