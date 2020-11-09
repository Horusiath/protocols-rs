use serde::{Serialize, Deserialize};
use crate::Result;
use std::collections::HashSet;
use crate::raft::PeerId;

pub enum State {
    Leader(LeaderState),
    Follower(FollowerState),
    Candidate(CandidateState),
}

pub struct LeaderState {
}

impl LeaderState {

}

pub struct FollowerState {

}

impl FollowerState {

}

pub struct CandidateState {

}

impl CandidateState {

}