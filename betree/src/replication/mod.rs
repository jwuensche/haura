//! A replication structure can be used by the engine to store highly requested
//! data on a "replication layer" or persistent cache. The advantage is that
//! access to slow media can be avoided if data is found in this layer.
//!
//! Multiple Layered Layers: 1. Cache
//!                          2. Replication
//!                          3. Disks
//!                             3.1 FASTEST
//!                             3.2 FAST
//!                             3.3 SLOW
//!                             3.4 SLOWEST
//!
//! Map Keys
//! ========
//!
//! - `0[hash]`  data key-value pairs
//! - `1[hash]` - Lru node keys
//! - `2` - Lru root node

const PREFIX_KV: u8 = 0;
const PREFIX_LRU: u8 = 1;
const PREFIX_LRU_ROOT: u8 = 2;

use pmem_hashmap::{
    allocator::{Pal, PalPtr},
    PMap, PMapError,
};
use std::{
    collections::BTreeMap,
    hash::{Hash, Hasher},
    marker::PhantomData,
    mem::size_of,
    ops::{Deref, DerefMut},
    path::PathBuf,
    ptr::NonNull,
};
use twox_hash::XxHash64;

mod lru;
use lru::Plru;
use serde::{Deserialize, Serialize};

/// A pointer to a region in persistent memory.
pub struct Persistent<T>(NonNull<T>);
// Pointer to persistent memory can be assumed to be non-thread-local
unsafe impl<T> Send for Persistent<T> {}
impl<T> Deref for Persistent<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { self.0.as_ref() }
    }
}
impl<T> DerefMut for Persistent<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.0.as_mut() }
    }
}

/// Persistent byte array cache. Optimized for read performance, avoid frequent
/// updates.
pub struct PersistentCache<K, T> {
    pal: Pal,
    root: Persistent<PCacheRoot<T>>,
    // Fix key types
    key_type: PhantomData<K>,
}

pub struct PCacheRoot<T> {
    map: BTreeMap<u64, PCacheMapEntry, Pal>,
    lru: Plru<T>,
}

#[derive(Debug, Clone)]
pub struct PCacheMapEntry {
    size: usize,
    lru_node: PalPtr,
    data: PalPtr,
}

/// Configuration for a persistent cache.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PersistentCacheConfig {
    /// Path to underlying file representing cache.
    pub path: PathBuf,
    /// Cache capacity in bytes.
    pub bytes: usize,
}

/// Ephemeral struct created in the preparation for an insertion.
pub struct PersistentCacheInsertion<'a, K, T> {
    cache: &'a mut PersistentCache<K, T>,
    key: u64,
    value: &'a [u8],
    baggage: T,
}

impl<'a, K, T: Clone> PersistentCacheInsertion<'a, K, T> {
    /// Performs an execution and calls the given function on each evicted entry
    /// from the store. On error the entire insertion is aborted and has to be
    /// initiated anew.
    pub fn insert<F>(self, f: F) -> Result<(), PMapError>
    where
        F: Fn(&T, &[u8]) -> Result<(), crate::vdev::Error>,
    {
        while let Ok(Some((key, baggage))) = self.cache.root.lru.evict(self.value.len() as u64) {
            // let data = self.cache.pmap.get(key.key())?;
            let entry = self.cache.root.map.get(&key).unwrap();
            let data =
                unsafe { core::slice::from_raw_parts(entry.data.load() as *const u8, entry.size) };
            if f(baggage, data).is_err() {
                return Err(PMapError::ExternalError("Writeback failed".into()));
            }
            // Finally actually remove the entries
            let mut entry = self.cache.root.map.remove(&key).unwrap();
            entry.data.free();
            self.cache.root.lru.remove(&mut entry.lru_node)?;
            entry.lru_node.free();
        }
        let lru_ptr = self.cache.pal.allocate(lru::PLRU_NODE_SIZE).unwrap();
        let data_ptr = self.cache.pal.allocate(self.value.len()).unwrap();
        data_ptr.copy_from(self.value, &self.cache.pal);
        self.cache.root.lru.insert(
            lru_ptr.clone(),
            self.key,
            self.value.len() as u64,
            self.baggage,
        )?;
        let map_entry = PCacheMapEntry {
            lru_node: lru_ptr,
            data: data_ptr,
            size: self.value.len(),
        };
        self.cache.root.map.insert(self.key, map_entry);
        Ok(())
    }
}

impl<K: Hash, T> PersistentCache<K, T> {
    /// Open an existent [PersistentCache]. Fails if no cache exist or invalid.
    pub fn open<P: Into<std::path::PathBuf>>(path: P) -> Result<Self, PMapError> {
        let pal = Pal::open(path.into()).unwrap();
        let root = pal.root(size_of::<PCacheRoot<T>>()).unwrap();
        assert!(!root.load().is_null());
        if let Some(root) = NonNull::new(root.load() as *mut PCacheRoot<T>) {
            let root = Persistent(root);
            Ok(Self {
                pal,
                root,
                key_type: PhantomData::default(),
            })
        } else {
            Err(PMapError::DoesNotExist)
        }
    }

    /// Create a new [PersistentCache] in the specified path. Fails if underlying resources are not valid.
    pub fn create<P: Into<std::path::PathBuf>>(path: P, size: usize) -> Result<Self, PMapError> {
        let mut pal = Pal::create(path.into(), size, 0o666).unwrap();
        let root = pal.root(size_of::<PCacheRoot<T>>()).unwrap();
        assert!(!root.load().is_null());
        if let Some(root) = NonNull::new(root.load() as *mut PCacheRoot<T>) {
            unsafe {
                root.as_ptr().write_unaligned(PCacheRoot {
                    lru: Plru::init(size as u64),
                    map: BTreeMap::new_in(pal.clone()),
                })
            };
            let mut root = Persistent(root);
            Ok(Self {
                pal,
                root,
                key_type: PhantomData::default(),
            })
        } else {
            Err(PMapError::DoesNotExist)
        }
    }

    /// Fetch an entry from the hashmap.
    pub fn get(&mut self, key: K) -> Result<&[u8], PMapError> {
        let mut hasher = XxHash64::default();
        key.hash(&mut hasher);
        let hash = hasher.finish();
        let res = self.root.map.get(&hash).cloned();
        if let Some(entry) = res {
            self.root.lru.touch(&entry.lru_node)?;
            Ok(unsafe { core::slice::from_raw_parts(entry.data.load() as *const u8, entry.size) })
        } else {
            Err(PMapError::DoesNotExist)
        }
    }

    /// Start an insertion. An insertion can only be successfully completed if values are properly evicted from the cache
    pub fn prepare_insert<'a>(
        &'a mut self,
        key: K,
        value: &'a [u8],
        baggage: T,
    ) -> PersistentCacheInsertion<'a, K, T> {
        let mut hasher = XxHash64::default();
        key.hash(&mut hasher);
        let hash = hasher.finish();
        PersistentCacheInsertion {
            cache: self,
            key: hash,
            value,
            baggage,
        }
    }

    /// Remove an entry.
    pub fn remove(&mut self, key: K) -> Result<(), PMapError> {
        let mut hasher = XxHash64::default();
        key.hash(&mut hasher);
        let hash = hasher.finish();
        if let Some(mut entry) = self.root.map.remove(&hash) {
            self.root.lru.remove(&mut entry.lru_node).unwrap();
            entry.lru_node.free();
            entry.data.free();
            Ok(())
        } else {
            Err(PMapError::DoesNotExist)
        }
    }
}
