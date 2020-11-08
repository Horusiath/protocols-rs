mod bcounter;
mod mv_register;
mod kernel;
mod or_set;
mod or_map;
mod gcounter;
mod pncounter;
mod lww_register;

/// A convergent trait that can be used to merge data from two instances together. Returns a true,
/// when self has been changed in result of merge operation (there were new updates carried by
/// `other`), or false otherwise.
///
/// `merge` operation is expected to be:
/// - idempotent: `a.merge(a) <=> a`
/// - commutative: `a.merge(b) <=> b.merge(a)`
/// - associative: `a.merge(b).merge(c) <=> a.merge(b.merge(c))`
pub trait Convergent {
    fn merge(&mut self, other: &Self) -> bool;
}

impl<T: Convergent + Clone> Convergent for Option<T> {
    fn merge(&mut self, other: &Self) -> bool {
        if let Some(v2) = other {
            if let Some(v1) = self {
                v1.merge(v2)
            } else {
                self.replace(v2.clone());
                true
            }
        } else {
            false
        }
    }
}

/// It's similar to `Convergent` trait, but allows to merge not only with other instance of the same
/// type but also their deltas - it's a special "carrier" type, that doesn't convey the full data
/// associated with given CRDT, but only part of it that represents the most recent updates.
///
/// Deltas are nested inside of a given CRDT and changed as part of updates. They can be retrieved
/// using `delta` function - in that case the carrier instance is moved from inside of CRDT outside.
pub trait DeltaConvergent {
    type Delta;
    /// Move delta from within the CRDT.
    fn delta(&mut self) -> Option<Self::Delta>;
    fn merge_delta(&mut self, other: &Self::Delta) -> bool;
}

/// Trait used to materialize a CRDT value into a user-facing state, stripped of CRDT-specific
/// metadata.
pub trait Materialize {
    type Value;
    fn value(&self) -> Self::Value;
}