use crate::crdt::convergent::kernel::{Kernel, DeltaOp};
use serde::{Serialize, Deserialize};
use crate::crdt::convergent::{Materialize, Convergent, kernel, DeltaConvergent};
use smallvec::alloc::collections::BTreeMap;
use std::cmp::Ordering;
use crate::vtime::ReplicaId;
use std::rc::Rc;
use std::ops::{Deref, DerefMut};
use smallvec::alloc::collections::btree_map::Entry;

#[derive(Debug, Clone, Serialize)]
pub struct ORMap<K: Ord, V> {
    kernel: Kernel<K>,
    entries: BTreeMap<Rc<K>, V>,

    #[serde(skip_serializing, skip_deserializing)]
    delta_inserts: Option<BTreeMap<Rc<K>, V>>,
}

impl<K: Ord, V: Clone> ORMap<K, V> {
    pub fn insert(&mut self, id: ReplicaId, key: K, value: V) {
        let key = Rc::new(key);
        self.kernel.insert(id, key.clone());
        self.entries.insert(key.clone(), value.clone());

        let mut delta = self.delta_inserts.take().unwrap_or_default();
        delta.insert(key, value);
        self.delta_inserts = Some(delta);
    }

    pub fn remove(&mut self, key: &K) {
        self.kernel.remove(key);
        self.entries.remove(key);

        if let Some(delta) = self.delta_inserts.as_mut() {
            delta.remove(key);
        }
    }

    pub fn len(&self) -> usize { self.kernel.len() }
}

impl<K: Ord, V> Default for ORMap<K, V> {
    fn default() -> Self {
        ORMap {
            kernel: Kernel::default(),
            entries: BTreeMap::new(),
            delta_inserts: None,
        }
    }
}

impl<'mat, K: Ord, V> Materialize for &'mat ORMap<K, V> where K: Ord {
    type Value = BTreeMap<&'mat K, &'mat V>;

    fn value(&self) -> Self::Value {
        self.entries.iter()
            .map(|(k,v)| (k.deref(), v))
            .collect()
    }
}

impl<K: Ord, V: Convergent> Convergent for ORMap<K, V> {
    fn merge(&mut self, other: &Self) -> bool {
        self.kernel.merge(&other.kernel)
    }
}

impl<K: Ord, V: DeltaConvergent + Default> DeltaConvergent for ORMap<K, V> {
    type Delta = Delta<K, V::Delta>;

    fn delta(&mut self) -> Option<Self::Delta> {
        if let Some(kernel_delta) = self.kernel.delta() {
            let entries_delta = self.delta_inserts.take()
                .unwrap_or_default()
                .into_iter()
                .filter_map(|(k, mut v)| v.delta().map(|d| (k, d)))
                .collect();
            Some(Delta {
                kernel: kernel_delta,
                entries: entries_delta,
            })
        } else {
            None
        }
    }

    fn merge_delta(&mut self, other: &Self::Delta) -> bool {
        let kernel = &mut self.kernel;
        let entries = &mut self.entries;
        let changed = kernel.merge_with(&other.kernel, |op| {
            match op {
                DeltaOp::Updated(rc) => {
                    let value = other.entries.get(&rc).expect("Defect: ORMap::merge_delta - insert detected but no entry found in delta object");
                    let e = entries.entry(rc.clone()).or_default();
                    e.merge_delta(value);
                },
                DeltaOp::Removed(key) => {
                    entries.remove(key);
                },
            }
        });
        changed
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Delta<K: Ord, D> {
    kernel: kernel::Delta<K>,
    entries: BTreeMap<Rc<K>, D>,
}

impl<K: Ord, D> Default for Delta<K, D> {
    fn default() -> Self {
        Delta {
            kernel: kernel::Delta::default(),
            entries: BTreeMap::new(),
        }
    }
}

impl<K: Ord, D: Convergent + Default> Convergent for Delta<K, D> {
    fn merge(&mut self, other: &Self) -> bool {
        let mut changed = self.kernel.merge(&other.kernel);
        for (key, value) in other.entries.iter() {
            match self.entries.entry(key.clone()) {
                Entry::Vacant(e) => {
                    let mut delta= D::default();
                    changed = delta.merge(value) || changed;
                    e.insert(delta);
                },
                Entry::Occupied(e) => {
                    changed = e.into_mut().merge(&value) || changed;
                }
            }
        }
        changed
    }
}