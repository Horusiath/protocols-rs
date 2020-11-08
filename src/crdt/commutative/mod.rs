pub mod event;
pub mod replicator;
pub mod counter;
pub mod mv_register;
pub mod lww_register;
pub mod or_set;
pub mod rga;

pub trait Commutative {
}