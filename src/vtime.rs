use std::collections::BTreeMap;
use serde::{Serialize, Deserialize};
use std::cmp::Ordering;
use crate::crdt::convergent::Convergent;
use std::iter::{Peekable, FusedIterator, FromIterator};

pub type ReplicaId = u32;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct Dot(ReplicaId, u64);

/// Represents a logical timestamp of a single operation. It consists of two values: `id` which is
/// a logical identifier of a replica, and monotonically increasing `seq_nr`, consistent
/// within the scope of that replica. These values, combined, can be used to uniquely represent
/// events in distributed systems across different actors, even in a face of concurrent operations.
impl Dot {
    /// Replica identifer of a creator of current Dot.
    pub fn id(&self) -> ReplicaId { self.0 }

    /// A sequence number, which is monotonically increasing in a scope of a current replica `id`.
    pub fn seq_nr(&self) -> u64 { self.1 }
}

/// Vector clock. Vector clocks can be used to represent causal time dependencies. Two instances of
/// vector clocks can be partially compared - in that case None variant represents a concurrent
/// events (that happened on two different actors without knowing about each other).
///
/// Vector clocks are also convergent - meaning they provide `merge` operation that is commutative,
/// associative and idempotent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VTime(BTreeMap<ReplicaId, u64>);

impl VTime {

    /// Increments a partial counter of a given replica `id` by a given `delta`, returning `Dot`
    /// representing a new logical timestamp generated this way.
    pub fn inc_by(&mut self, id: ReplicaId, delta: u64) -> Dot {
        if delta > 0 {
            let e = self.0.entry(id).or_default();
            let value = *e + delta;
            *e = value;
            Dot(id, value)
        } else {
            Dot(id, *self.0.get(&id).unwrap_or(&0u64))
        }
    }

    /// Increments a partial counter of a given replica `id` by 1, returning `Dot`
    /// representing a new logical timestamp generated this way.
    #[inline]
    pub fn inc(&mut self, id: ReplicaId) -> Dot { self.inc_by(id, 1) }

    /// Returns a sequence number of a given replica.
    pub fn get(&self, id: &ReplicaId) -> u64 { *self.0.get(id).unwrap_or(&0u64) }

    /// Puts a given `Dot` inside of a vector clock, updating a sequence number of a corresponding
    /// replica id in that clock if it was more recent. Returns true if current vector clock has
    /// been successfully updated.
    pub fn set(&mut self, dot: Dot) -> bool {
        let e = self.0.entry(dot.id()).or_default();
        if dot.seq_nr() > *e {
            *e = dot.seq_nr();
            true
        } else {
            false
        }
    }

    /// Iterates over replica id's and their sequence numbers stored inside of a current vector
    /// clock.
    pub fn iter(&self) -> Iter<'_> { self.0.iter() }

    /// Checks if current vector clock stores any value.
    pub fn is_empty(&self) -> bool { self.0.is_empty() }

    /// Checks if a given `Dot` has been already observed by current vector clock.
    pub fn contains(&self, dot: &Dot) -> bool {
        self.get(&dot.0) >= dot.1
    }

    /// Zips two vector clocks together, returning an iterator of tuples or replica id of both
    /// clocks and corresponding sequence numbers of each of vector clocks.
    pub fn zip<'a>(&'a self, other: &'a Self) -> Zip<'a> {
        let left= self.iter().peekable();
        let right = other.iter().peekable();
        Zip(left, right)
    }

    /// Returns a new vector clock instance, that is a minimum of sequence numbers stored by
    /// two given clocks.
    pub fn min(&self, other: &Self) -> Self {
        self.zip(other).map(|(&id, &l, &r)| (id, l.min(r))).collect()
    }

    /// Returns a new vector clock instance, that is a maximum of sequence numbers stored by
    /// two given clocks.
    pub fn max(&self, other: &Self) -> Self {
        self.zip(other).map(|(&id, &l, &r)| (id, l.max(r))).collect()
    }
}

impl Default for VTime {
    fn default() -> Self {
        VTime(BTreeMap::new())
    }
}

impl Convergent for VTime {
    fn merge(&mut self, other: &Self) -> bool {
        let mut changed = false;
        for (k, v) in other.0.iter() {
            let e = self.0.entry(*k).or_default();
            if *e < *v {
                *e = *v;
                changed = true;
            }
        }
        changed
    }
}

impl PartialOrd for VTime {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let mut a = self.0.iter();
        let mut b = other.0.iter();
        let mut result = Some(Ordering::Equal);
        let mut e1 = a.next();
        let mut e2 = b.next();
        while result != None {
            match (e1, e2) {
                (None, None) => break,
                (None, Some(_)) => {
                    match result {
                        Some(Ordering::Greater) => result = None,
                        Some(Ordering::Equal) => result = Some(Ordering::Less),
                        _ => {},
                    }
                    break;
                },
                (Some(_), None) => {
                    match result {
                        Some(Ordering::Less) => result = None,
                        Some(Ordering::Equal) => result = Some(Ordering::Greater),
                        _ => {},
                    }
                    break;
                },
                (Some((k1, v1)), Some((k2, v2))) => {
                    match k1.cmp(k2) {
                        Ordering::Equal => {
                            result =
                                match v1.partial_cmp(v2) {
                                    Some(Ordering::Greater) if result == Some(Ordering::Less) => {
                                        None
                                    },
                                    Some(Ordering::Less) if result == Some(Ordering::Greater) => {
                                        None
                                    },
                                    other if result == Some(Ordering::Equal) => other,
                                    _ => result,
                                };

                            e1 = a.next(); // A B
                            e2 = b.next(); // A C
                        },
                        Ordering::Less | Ordering::Greater => {
                            result = None;
                        },
                    }
                }
            }
        }
        result
    }
}

impl FromIterator<(ReplicaId, u64)> for VTime {
    fn from_iter<T: IntoIterator<Item=(ReplicaId, u64)>>(iter: T) -> Self {
        let mut map = BTreeMap::new();
        for (key, value) in iter {
            map.insert(key, value);
        }
        VTime(map)
    }
}

impl PartialEq for VTime {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other) == Some(Ordering::Equal)
    }
}

pub type Iter<'a> =  smallvec::alloc::collections::btree_map::Iter<'a, u32, u64>;

#[derive(Clone, Debug)]
pub struct Zip<'a>(Peekable<Iter<'a>>, Peekable<Iter<'a>>);

impl<'a> Iterator for Zip<'a> {
    type Item = (&'a ReplicaId, &'a u64, &'a u64);

    fn next(&mut self) -> Option<Self::Item> {
        let a = self.0.peek().cloned();
        let b = self.1.peek().cloned();
        match (a, b) {
            (None, None) => None,
            (Some((id1, left)), Some((id2, right))) => {
                match id1.cmp(id2) {
                    Ordering::Equal => {
                        self.0.next();
                        self.1.next();
                        Some((id1, left, right))
                    }
                    Ordering::Less => {
                        self.0.next();
                        Some((id1, left, &0))
                    }
                    Ordering::Greater => {
                        self.1.next();
                        Some((id2, &0, &right))
                    }
                }
            },
            (Some((id, left)), None) => {
                self.0.next();
                Some((id, left, &0))
            },
            (None, Some((id, right))) => {
                self.1.next();
                Some((id, &0, right))
            }
        }
    }
}

impl<'a> FusedIterator for Zip<'a> {}

#[cfg(test)]
mod test {
    use crate::vtime::{VTime, ReplicaId};
    use std::cmp::Ordering;
    use crate::crdt::convergent::Convergent;

    const A: ReplicaId = 1;
    const B: ReplicaId = 2;
    const C: ReplicaId = 3;

    fn vtime(a: u64, b:u64, c: u64) -> VTime {
        let mut ts = VTime::default();
        ts.inc_by(A, a);
        ts.inc_by(B, b);
        ts.inc_by(C, c);
        ts
    }

    #[test]
    fn vtime_partial_cmp() {
        let cases = vec![
            (vtime(0,0,0), vtime(0,0,0), Some(Ordering::Equal)),
            (vtime(1,2,3), vtime(1,2,3), Some(Ordering::Equal)),
            (vtime(1,2,3), vtime(1,2,0), Some(Ordering::Greater)),
            (vtime(1,3,3), vtime(1,2,3), Some(Ordering::Greater)),
            (vtime(1,0,0), vtime(1,2,0), Some(Ordering::Less)),
            (vtime(1,2,2), vtime(1,2,3), Some(Ordering::Less)),
            (vtime(1,2,3), vtime(3,2,1), None),
            (vtime(1,0,1), vtime(1,1,0), None),
        ];

        for (left, right, expected) in cases {
            assert_eq!(left.partial_cmp(&right), expected);
        }
    }

    #[test]
    fn vtime_zip() {
        let cases = vec![
            (vtime(0,0,0), vtime(0,0,0), vec![]),
            (vtime(1,2,3), vtime(1,2,0), vec![(A, 1, 1), (B, 2, 2), (C, 3, 0)]),
            (vtime(1,3,3), vtime(1,2,3), vec![(A, 1, 1), (B, 3, 2), (C, 3, 3)]),
            (vtime(1,0,1), vtime(1,1,0), vec![(A, 1, 1), (B, 0, 1), (C, 1, 0)]),
        ];

        for (left, right, expected) in cases {
            let zipped = left.zip(&right)
                .map(|(&x,&y,&z)| (x,y,z))
                .collect::<Vec<_>>();

            assert_eq!(zipped, expected);
        }
    }

    #[test]
    fn vtime_merge() {

        fn assert_merge(mut left: VTime, right: VTime, expected: VTime, changed: bool) {
            assert_eq!(left.merge(&right), changed);
            assert_eq!(left, expected);
        }

        assert_merge(vtime(0,0,0), vtime(0,0,0), vtime(0,0,0), false);
        assert_merge(vtime(2,2,3), vtime(1,2,0), vtime(2,2,3), false);
        assert_merge(vtime(1,3,3), vtime(1,2,4), vtime(1,3,4), true);
        assert_merge(vtime(1,0,1), vtime(1,1,0), vtime(1,1,1), true);
    }
}