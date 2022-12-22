//! This module provides the Data Management Layer
//! which handles user-defined objects and includes caching and write back.

use crate::{
    cache::AddSize,
    migration::DmlMsg,
    size::{Size, StaticSize},
    storage_pool::DiskOffset,
    StoragePreference,
};
use parking_lot::Mutex;
use serde::{de::DeserializeOwned, Serialize};
use stable_deref_trait::StableDeref;
use std::{
    collections::HashMap,
    fmt::Debug,
    hash::Hash,
    io::{self, Write},
    ops::DerefMut,
    sync::Arc,
};

use crossbeam_channel::Sender;

/// Marker trait for plain old data types
pub trait PodType:
    Serialize + DeserializeOwned + Debug + Hash + Eq + Copy + StaticSize + Send + Sync + 'static
{
}
impl<
        T: Serialize
            + DeserializeOwned
            + Debug
            + Hash
            + Eq
            + Copy
            + StaticSize
            + Send
            + Sync
            + 'static,
    > PodType for T
{
}

/// A reference to an object managed by a `Dml`.
///
/// While this trait only has one known implementor [impls::ObjRef], it is
/// useful to hide away ugly types such as the ObjectPointer within the [Dml]
/// trait.
pub trait ObjectReference: Serialize + DeserializeOwned + StaticSize + Debug + 'static {
    /// The ObjectPointer for this ObjectRef.
    type ObjectPointer;
    /// Return a reference to an `Self::ObjectPointer`
    /// if this object reference is in the unmodified state.
    fn get_unmodified(&self) -> Option<&Self::ObjectPointer>;
}

/// Implementing types have an allocation preference, which can be invalidated
/// and recomputed as necessary.
pub trait HasStoragePreference {
    /// Return the [StoragePreference], if it is known to be correct,
    /// return None if it was invalidated and needs to be recalculated.
    fn current_preference(&self) -> Option<StoragePreference>;

    /// Recalculate the storage preference, potentially scanning through all
    /// data contained by this value.
    ///
    /// Implementations are expected to cache the computed preference, so that
    /// immediately subsequent calls to [HasStoragePreference::current_preference]
    /// return Some.
    fn recalculate(&self) -> StoragePreference;

    /// Returns a correct preference, recalculating it if needed.
    fn correct_preference(&self) -> StoragePreference {
        match self.current_preference() {
            Some(pref) => pref,
            None => self.recalculate(),
        }
    }

    /// Return the system storage preference. Returns None if none is set.
    fn system_storage_preference(&self) -> StoragePreference;

    /// Rewrite the system storage preference.
    fn set_system_storage_preference(&mut self, pref: StoragePreference);

    // /// Distribute a desired storage prefrence to all child nodes.
    // /// Cached prefrence are advised to be updated.
    // /// The size of the moved keys should not exceed the limit of the desired storage tier.
    // fn flood_storage_preference(&self, pref: StoragePreference);
}

/// An object managed by a `Dml`.
pub trait Object<R>: Size + Sized + HasStoragePreference {
    /// Packs the object into the given `writer`.
    fn pack<W: Write>(&self, writer: W) -> Result<(), io::Error>;
    /// Unpacks the object from the given `data`.
    fn unpack_at(disk_offset: DiskOffset, data: Box<[u8]>) -> Result<Self, io::Error>;

    /// Returns debug information about an object.
    fn debug_info(&self) -> String;

    /// Calls a closure on each child `ObjectRef` of this object.
    ///
    /// This method is short-circuiting on `Err(_)`.
    fn for_each_child<E, F>(&mut self, f: F) -> Result<(), E>
    where
        F: FnMut(&mut R) -> Result<(), E>;
}

/// A `Dml` for a specific `Handler`.
pub trait Dml: Sized {
    /// A reference to an object managed by this `Dmu`.
    type ObjectRef: ObjectReference<ObjectPointer = Self::ObjectPointer>;
    /// The pointer type to an on-disk object.
    type ObjectPointer: Serialize + DeserializeOwned + Clone;
    /// The info type which is tagged to each object.
    type Info: PodType;
    /// The object type managed by this Dml.
    type Object: Object<Self::ObjectRef>;
    /// A reference to a cached object.
    type CacheValueRef: StableDeref<Target = Self::Object> + AddSize + 'static;
    /// A mutable reference to a cached object.
    type CacheValueRefMut: StableDeref<Target = Self::Object> + DerefMut + AddSize + 'static;

    /// Provides immutable access to the object identified by the given
    /// `ObjectRef`.  Fails if the object was modified and has been evicted.
    fn try_get(&self, or: &Self::ObjectRef) -> Option<Self::CacheValueRef>;

    /// Provides immutable access to the object identified by the given
    /// `ObjectRef`.
    fn get(&self, or: &mut Self::ObjectRef) -> Result<Self::CacheValueRef, Error>;

    /// Provides mutable access to the object identified by the given
    /// `ObjectRef`.
    ///
    /// If the object is not mutable, it will be `CoW`ed and `info` will be
    /// attached to the object.
    fn get_mut(
        &self,
        or: &mut Self::ObjectRef,
        info: Self::Info,
    ) -> Result<Self::CacheValueRefMut, Error>;

    /// Provides mutable access to the object
    /// if this object is already mutable.
    fn try_get_mut(&self, or: &Self::ObjectRef) -> Option<Self::CacheValueRefMut>;

    /// Inserts a new mutable `object` into the cache.
    fn insert(&self, object: Self::Object, info: Self::Info) -> Self::ObjectRef;

    /// Inserts a new mutable `object` into the cache.
    fn insert_and_get_mut(
        &self,
        object: Self::Object,
        info: Self::Info,
    ) -> (Self::CacheValueRefMut, Self::ObjectRef);

    /// Removes the object referenced by `or`.
    fn remove(&self, or: Self::ObjectRef);

    /// Removes the object referenced by `or` and returns it.
    fn get_and_remove(&self, or: Self::ObjectRef) -> Result<Self::Object, Error>;

    /// Evicts excessive cache entries.
    fn evict(&self) -> Result<(), Error>;

    /// Turns an ObjectPointer into an ObjectReference.
    fn ref_from_ptr(r: Self::ObjectPointer) -> Self::ObjectRef;

    /// Run cache-internal self-validation.
    fn verify_cache(&self);

    /// Writes back an object and all its dependencies.
    /// `acquire_or_lock` shall return a lock guard
    /// that provides mutable access to the object reference.
    fn write_back<F, G>(&self, acquire_or_lock: F) -> Result<Self::ObjectPointer, Error>
    where
        F: FnMut() -> G,
        G: DerefMut<Target = Self::ObjectRef>;

    /// Prefetch session type.
    type Prefetch;

    /// Prefetches the on-disk object identified by `or`.
    /// Will return `None` if object is in cache.
    fn prefetch(&self, or: &Self::ObjectRef) -> Result<Option<Self::Prefetch>, Error>;

    /// Finishes the prefetching.
    fn finish_prefetch(&self, p: Self::Prefetch) -> Result<(), Error>;

    /// Drops the cache entries.
    fn drop_cache(&self);
}

/// Legible result of a copy-on-write call. This describes wether the given
/// offset has been removed or preserved depending on if existing snapshots
/// require them.
pub enum CopyOnWriteEvent {
    /// The current state still pertains to the given offset.
    Preserved,
    /// The given offset has been deallocated.
    Removed,
}

#[derive(Debug, PartialEq, Eq)]
/// The reason as to why copy on write has been called.
///
/// This is mostly relevant to the reporting of activity via the reporting trait.
pub enum CopyOnWriteReason {
    /// The copy on write call originated from a removal operation.
    Remove,
    /// The copy on write call originated from a stealing transition moving the
    /// just written back object from the InWriteback state back to the modified
    /// state.
    Steal,
}

/// Denotes if an implementor of the [Dml] can utilize an allocation handler.
pub trait DmlWithHandler {
    type Handler;

    fn handler(&self) -> &Self::Handler;
}

/// Denotes if an implementor of the [Dml] can utilize a storage pool layer.
pub trait DmlWithSpl {
    type Spl;

    fn spl(&self) -> &Self::Spl;
}

/// Denotes if an implementor of the [Dml] uses a cache.
pub trait DmlWithCache {
    type CacheStats: serde::Serialize;

    fn cache_stats(&self) -> Self::CacheStats;
}

/// Denotes if an implementor of the [Dml] can also handle storage hints emitted
/// by the migration policies.
pub trait DmlWithStorageHints {
    /// Returns a handle to the storage hint data structure.
    fn storage_hints(&self) -> Arc<Mutex<HashMap<DiskOffset, StoragePreference>>>;
    /// Returns the default storage class used when [StoragePreference] is `None`.
    fn default_storage_class(&self) -> StoragePreference;
}

/// Extension of an DMU to signal that it supports a message based report format.
/// Implemented via channels the DMU is allowed to send any number of messages to an consuming sink.
/// It is advised to use `unbound` channels for this purpose.
pub trait DmlWithReport {
    /// Attach a reporting channel to the DML
    fn with_report(self, tx: Sender<DmlMsg>) -> Self;
    /// Set a reporting channel to the DML
    fn set_report(&mut self, tx: Sender<DmlMsg>);
}

mod delegation;
pub(crate) mod errors;
pub(crate) mod impls;
mod object_ptr;

pub use self::{
    errors::{Error, ErrorKind},
    impls::Dmu,
    object_ptr::ObjectPointer,
};
