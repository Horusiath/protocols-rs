use crate::crdt::convergent::kernel::{Kernel, MergeOp};
use serde::{Serialize, Deserialize};
use crate::crdt::convergent::{Materialize, Convergent, kernel, DeltaConvergent};
use smallvec::alloc::collections::BTreeMap;
use std::cmp::Ordering;
use crate::vtime::ReplicaId;
use std::rc::Rc;
use std::ops::{Deref, DerefMut};

type IEntry<'a, K, V> = std::collections::btree_map::Entry<'a, K, V>;

#[derive(Debug, Clone, Serialize)]
pub struct ORMap<K: Ord, V> {
    kernel: Kernel<K>,
    entries: BTreeMap<Rc<K>, V>,
}

impl<K: Ord, V> ORMap<K, V> {

    pub fn entry(&mut self, key: K) -> Entry<'_, K, V> {
        let key = Rc::new(key);
        Entry {
            key,
            handle: self,
        }
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        self.kernel.remove(key);
        self.entries.remove(key)
    }

    pub fn is_empty(&self) -> bool { self.entries.is_empty() }

    pub fn len(&self) -> usize { self.kernel.len() }
}

impl<'m, K: Ord, V: Materialize<'m>> ORMap<K, V> {
    pub fn get(&'m self, key: &K) -> Option<V::Value> {
        self.entries.get(key).map(|v| v.value())
    }
}

#[derive(Debug)]
pub struct Entry<'a, K: Ord, V> {
    key: Rc<K>,
    handle: &'a mut ORMap<K, V>,
}

impl<'a, K: Ord, V> Entry<'a, K, V> {
    pub fn key(&self) -> &K { self.key.deref() }

    pub fn or_insert(self, id: ReplicaId, default: V) -> &'a mut V {
        let key = self.key;
        match self.handle.entries.entry(key.clone()) {
            IEntry::Vacant(e) => {
                self.handle.kernel.insert(id, key);
                e.insert(default)
            },
            IEntry::Occupied(e) => {
                e.into_mut()
            }
        }
    }

    pub fn or_insert_with<F>(self, id: ReplicaId, default: F) -> &'a mut V where F: FnOnce() -> V {
        let key = self.key;
        match self.handle.entries.entry(key.clone()) {
            IEntry::Vacant(e) => {
                self.handle.kernel.insert(id, key);
                e.insert(default())
            },
            IEntry::Occupied(e) => {
                e.into_mut()
            }
        }
    }

    pub fn or_insert_with_key<F>(self, id: ReplicaId, default: F) -> &'a mut V where F: FnOnce(&K) -> V {
        let key = self.key;
        match self.handle.entries.entry(key.clone()) {
            IEntry::Vacant(e) => {
                let result = e.insert(default(&key));
                self.handle.kernel.insert(id, key);
                result
            },
            IEntry::Occupied(e) => {
                e.into_mut()
            }
        }
    }

    pub fn and_modify<F>(self, id: ReplicaId, f: F) -> Self where F: FnOnce(&mut V) -> () {
        let key = self.key;
        let handle = self.handle;
        if let Some(v) = handle.entries.get_mut(&key) {
            handle.kernel.insert(id, key.clone());
            f(v);
            Entry { key, handle }
        } else {
            Entry { key, handle }
        }
    }
}

impl<'a, K: Ord, V: Default> Entry<'a, K, V> {

    pub fn or_default(self, id: ReplicaId) -> &'a mut V {
        let key = self.key;
        match self.handle.entries.entry(key.clone()) {
            IEntry::Vacant(e) => {
                self.handle.kernel.insert(id, key);
                e.insert(V::default())
            },
            IEntry::Occupied(e) => {
                e.into_mut()
            }
        }
    }

}

impl<K: Ord, V> Default for ORMap<K, V> {
    fn default() -> Self {
        ORMap {
            kernel: Kernel::default(),
            entries: BTreeMap::new(),
        }
    }
}

impl<'m, K: Ord + 'm, V: Materialize<'m>> Materialize<'m> for ORMap<K, V> {
    type Value = BTreeMap<&'m K, V::Value>;

    fn value(&'m self) -> Self::Value {
        self.entries.iter()
            .map(|(k,v)| (k.deref(), v.value()))
            .collect()
    }
}

impl<K: Ord, V: Convergent + Default> Convergent for ORMap<K, V> {
    fn merge(&mut self, other: &Self) -> bool {
        let kernel = &mut self.kernel;
        let entries = &mut self.entries;

        kernel.merge_with(&other.kernel, |op| {
            match op {
                MergeOp::Updated(key) => {
                    let v = other.entries.get(&key).expect("Defect: ORMap::merge - updated operation detected but value not found");
                    let e = entries.entry(key).or_default();
                    e.merge(v);
                },
                MergeOp::Removed(key) => {
                    entries.remove(key);
                },
            }
        })
    }
}

impl<K: Ord, V: DeltaConvergent + Default> DeltaConvergent for ORMap<K, V> {
    type Delta = Delta<K, V::Delta>;

    fn delta(&mut self) -> Option<Self::Delta> {
        if let Some(mut kernel_delta) = self.kernel.delta() {
            let entries_delta = kernel_delta.keys()
                .flat_map(|k| self.entries.get_mut(&k).and_then(|v| v.delta().map(|d| (k, d))))
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
        let changed = kernel.merge_with_delta(&other.kernel, |op| {
            match op {
                MergeOp::Updated(rc) => {
                    let value = other.entries.get(&rc).expect("Defect: ORMap::merge_delta - insert detected but no entry found in delta object");
                    let e = entries.entry(rc.clone()).or_default();
                    e.merge_delta(value);
                },
                MergeOp::Removed(key) => {
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
                IEntry::Vacant(e) => {
                    let mut delta= D::default();
                    changed = delta.merge(value) || changed;
                    e.insert(delta);
                },
                IEntry::Occupied(e) => {
                    changed = e.into_mut().merge(&value) || changed;
                }
            }
        }
        changed
    }
}
#[cfg(test)]
mod test {
    use crate::crdt::convergent::{Materialize, Convergent, DeltaConvergent};
    use crate::crdt::convergent::or_map::ORMap;
    use crate::crdt::convergent::lww_register::LWWRegister;
    use crate::hlc::HybridTime;
    use std::collections::{BTreeMap, BTreeSet};
    use crate::vtime::ReplicaId;
    use crate::crdt::convergent::mv_register::MVRegister;
    use crate::crdt::convergent::or_set::ORSet;
    use futures::StreamExt;

    const A: ReplicaId = 1;
    const B: ReplicaId = 2;
    const C: ReplicaId = 3;

    #[test]
    fn ormap_identity() {
        let a : ORMap<&str, LWWRegister<u32, HybridTime>> = ORMap::default();
        assert!(a.is_empty());
        assert_eq!(a.value(), BTreeMap::new())
    }

    #[test]
    fn ormap_idempotency() {
        let mut a = ORMap::default();
        let e = a.entry("key").or_insert(A, LWWRegister::with_hybrid_clock());
        e.assign(A, 1);

        let b = a.clone();

        let mut expected = BTreeMap::new();
        expected.insert(&"key", Some(&1));

        assert_eq!(a.value(), expected);
        assert!(!a.merge(&b));
        assert_eq!(a.value(), expected);
    }

    #[test]
    fn ormap_associativity() {
        let mut a : ORMap<&str, ORSet<u32>> = ORMap::default();
        a.entry("key").or_default(A).insert(A, 1);
        let mut b : ORMap<&str, ORSet<u32>> = ORMap::default();
        b.entry("key").or_default(B).insert(B, 2);
        let mut c : ORMap<&str, ORSet<u32>> = ORMap::default();
        c.entry("key").or_default(C).insert(C, 3);

        let mut a2 = a.clone();
        let mut b2 = b.clone();
        let c2 = c.clone();

        let mut expected = BTreeMap::new();
        expected.insert(&"key", vec![&1,&2,&3].into_iter().collect::<BTreeSet<&u32>>());

        // (a + b) + c
        assert!(a.merge(&b));
        assert!(a.merge(&c));
        assert_eq!(a.value(), expected);

        // a + (b + c)
        assert!(b2.merge(&c2));
        assert!(a2.merge(&b2));
        assert_eq!(a.value(), expected);

        assert!(!a.merge(&a2));
    }

    #[test]
    fn orset_commutativity() {
        let mut a : ORMap<&str, ORSet<u32>> = ORMap::default();
        a.entry("key").or_default(A).insert(A, 1);
        let mut b : ORMap<&str, ORSet<u32>> = ORMap::default();
        b.entry("key").or_default(B).insert(B, 2);

        let mut a2 = a.clone();
        let mut b2 = b.clone();

        let mut expected = BTreeMap::new();
        expected.insert(&"key", vec![&1,&2].into_iter().collect::<BTreeSet<&u32>>());

        // a + b
        assert!(a.merge(&b));
        assert_eq!(a.value(), expected);

        // b + a
        assert!(b2.merge(&a2));
        assert_eq!(a.value(), expected);

        assert!(!a.merge(&b2));
    }

}