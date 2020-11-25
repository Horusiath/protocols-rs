Various implementations of different distributed protocols in Rust:

Conflict Free Replicated Data Types:

1. Delta-state:
    - [x] Bounded Counter
    - [x] Grown-only Counter
    - [x] Positive/Negative Counter
    - [x] Last Write Wins Register
    - [x] Mutli-Value Register
    - [x] (Add-Wins) Observed-Remove Set
    - [x] (Add-Wins) Observed-Remove Map
2. Pure-operation based:
    - [ ] Reliable Causal Broadcast protocol
    - [ ] Counter
    - [ ] Last Write Wins Register
    - [ ] Multi Value Register
    - [ ] Observed Remove Set
3. Membership protocols:
    - [ ] Fireflies (byzantine-resistant membership)
    - [ ] Rapid (strongly-consistent)
    - [ ] HyParView (weakly-consistent)
    - [ ] Serf (self-adapting SWIM variant)
4. Paxos implementation:
    - [ ] Compare-And-Swap Paxos
    - [ ] Matchmaker Paxos
5. [ ] Raft implementation