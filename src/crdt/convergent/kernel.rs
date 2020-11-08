use serde::{Serialize, Deserialize};
use crate::vtime::{VTime, Dot, ReplicaId};
use smallvec::alloc::collections::BTreeSet;
use crate::crdt::convergent::{Convergent, Materialize, DeltaConvergent};
use std::collections::BTreeMap;
use smallvec::SmallVec;
use smallvec::alloc::collections::btree_map::{Values, Keys};
use std::iter::FusedIterator;
use crate::dotted_version::DottedVersion;
use std::rc::Rc;
use std::ops::Deref;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Kernel<T: Ord> {
    seen: DottedVersion,
    entries: BTreeMap<Rc<T>, SmallVec<[Dot;1]>>,

    #[serde(skip_serializing, skip_deserializing, default)]
    delta: Option<Delta<T>>,
}

impl<T: Ord> Kernel<T> {
    pub fn insert(&mut self, id: ReplicaId, value: Rc<T>) -> Dot {
        let dot = self.seen.inc(id);
        let e = self.entries.entry(value.clone()).or_default();
        e.push(dot);

        let mut delta = self.delta.take().unwrap_or_default();
        delta.insert(value, dot);
        self.delta = Some(delta);

        dot
    }

    pub fn remove(&mut self, value: &T) {
        let dots = self.entries.remove(value).unwrap_or_default();

        let mut delta = self.delta.take().unwrap_or_default();
        delta.remove(dots);
        self.delta = Some(delta);
    }

    pub fn clear(&mut self) {
        let dots = self.entries.values().flatten().cloned().collect();
        self.entries.clear();

        let mut delta = self.delta.take().unwrap_or_default();
        delta.remove(dots);
        self.delta = Some(delta);
    }

    pub fn len(&self) -> usize { self.entries.len() }

    pub fn is_empty(&self) -> bool { self.entries.is_empty() }


    pub(crate) fn merge_with<F>(&mut self, other: &Self, mut f: F) -> bool where F:FnMut(MergeOp<'_, T>) -> () {
        let mut changed = false;

        // insert all values that were not "seen"
        for (value, other_dots) in other.entries.iter() {
            let e = self.entries.entry(value.clone()).or_default();
            for dot in other_dots {
                if !e.contains(dot) {
                    e.push(*dot);
                    changed = true;

                    f(MergeOp::Updated(value.clone()));
                }
            }
        }

        // remove all values that were seen but are not present in other
        self.entries.drain_filter(|value, dots| {
            if dots.iter().any(|d| other.seen.contains(d)) && !other.entries.contains_key(value) {
                changed = true;
                f(MergeOp::Removed(value));
                true
            } else {
                false
            }
        });

        changed = self.seen.merge(&other.seen) || changed;
        changed
    }

    pub(crate) fn merge_with_delta<F>(&mut self, other: &Delta<T>, mut f: F) -> bool where F:FnMut(MergeOp<'_, T>) -> () {
        let mut changed = false;
        for (value, dots) in other.inserts.iter() {
            let unseen = dots.iter().any(|dot| !self.seen.contains(dot));
            if unseen {
                changed = true;
                let e = self.entries.entry(value.clone()).or_default();
                for dot in dots.iter() {
                    if !e.contains(dot) {
                        e.push(*dot);
                    }
                }

                f(MergeOp::Updated(value.clone()))
            }
        }
        for dot in other.removals.iter() {
            self.entries.drain_filter(|value, dots| {
                let found = dots.iter().any(|d| d == dot);
                if found && dots.len() == 1 {
                    f(MergeOp::Removed(value.deref()));
                    true // if dot to remove is the only dot for that entry, remove entry
                } else if found {
                    // remove that dot from the entry
                    dots.retain(|d| d != dot);
                    false
                } else {
                    false // dot not found, continue
                }
            });
        }
        changed
    }
}

pub(crate) enum MergeOp<'a, T> {
    Updated(Rc<T>),
    Removed(&'a T),
}

impl<T: Ord> Default for Kernel<T> {
    fn default() -> Self {
        Kernel {
            seen: DottedVersion::default(),
            entries: BTreeMap::new(),
            delta: None,
        }
    }
}

impl<T: Ord> Convergent for Kernel<T> {
    fn merge(&mut self, other: &Self) -> bool {
        self.merge_with(other, |_| {})
    }
}

impl<T: Ord> DeltaConvergent for Kernel<T> {
    type Delta = Delta<T>;

    fn delta(&mut self) -> Option<Self::Delta> {
        self.delta.take()
    }

    fn merge_delta(&mut self, other: &Self::Delta) -> bool {
        self.merge_with_delta(other, |_|{})
    }
}


impl<'m, T: Ord + 'm> Materialize<'m> for Kernel<T> {
    type Value = Value<'m, T>;

    fn value(&'m self) -> Self::Value {
        Value(self.entries.keys())
    }
}

#[derive(Clone, Debug)]
pub struct Value<'a, T>(Keys<'a, Rc<T>, SmallVec<[Dot;1]>>);

impl<'a, V> Iterator for Value<'a, V> {
    type Item = &'a V;

    fn next(&mut self) -> Option<&'a V> { self.0.next().map(|rc| rc.deref()) }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }

    fn last(mut self) -> Option<&'a V> {
        self.next_back()
    }
}

impl<'a, V> DoubleEndedIterator for Value<'a, V> {
    fn next_back(&mut self) -> Option<&'a V> {
        self.0.next_back().map(|rc| rc.deref())
    }
}

impl<V> ExactSizeIterator for Value<'_, V> {
    fn len(&self) -> usize {
        self.0.len()
    }
}

impl<V> FusedIterator for Value<'_, V> {}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Delta<T: Ord> {
    inserts: BTreeMap<Rc<T>, SmallVec<[Dot;1]>>,
    removals: SmallVec<[Dot;1]>
}

impl<T: Ord> Delta<T> {
    fn insert(&mut self, value: Rc<T>, dot: Dot) {
        let e = self.inserts.entry(value).or_default();
        if !e.contains(&dot) {
            e.push(dot);
        }
    }

    fn remove(&mut self, dots: SmallVec<[Dot;1]>) {
        for dot in dots {
            if !self.removals.contains(&dot) {
                self.removals.push(dot);
            }
        }
    }

    pub fn has_inserts(&self) -> bool { !self.inserts.is_empty() }

    pub fn has_removals(&self) -> bool { !self.removals.is_empty() }

    pub fn keys(&self) -> DeltaKeys<'_, T> { DeltaKeys(self.inserts.keys()) }
}

pub struct DeltaKeys<'a, T>(std::collections::btree_map::Keys<'a, Rc<T>, SmallVec<[Dot;1]>>);

impl<'a, T> Iterator for DeltaKeys<'a, T> {
    type Item = Rc<T>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().cloned()
    }
}

impl<T: Ord> Default for Delta<T> {
    fn default() -> Self {
        Delta {
            inserts: BTreeMap::new(),
            removals: SmallVec::default(),
        }
    }
}

impl<T: Ord> Convergent for Delta<T> {
    fn merge(&mut self, other: &Self) -> bool {
        let mut inserts_changed = false;
        for (key, value) in other.inserts.iter() {
            if self.inserts.contains_key(key) {
                let e = self.inserts.get_mut(key).expect("Defect: or_set::Delta check didn't found");
                for dot in value.iter() {
                    if !e.contains(dot) {
                        e.push(*dot);
                        inserts_changed = true;
                    }
                }
            } else {
                self.inserts.insert(key.clone(), value.clone());
                inserts_changed = true;
            }
        }

        let mut removals_changed = false;
        for dot in other.removals.iter() {
            if !self.removals.contains(dot) {
                self.removals.push(*dot);
                removals_changed = true;
            }
        }

        inserts_changed || removals_changed
    }
}