use super::{Persistent, PREFIX_LRU};
use pmem_hashmap::{allocator::PalPtr, PMap, PMapError};
use std::{marker::PhantomData, mem::size_of, ptr::NonNull};

/// Fetch and cast a pmem pointer to a [PlruNode].
///
/// Safety
/// ======
/// The internals of this method are highly unsafe. Concurrent usage and
/// removal are urgently discouraged.
fn fetch<T>(ptr: &PalPtr) -> Result<&mut PlruNode<T>, PMapError> {
    Ok(unsafe {
        std::mem::transmute::<&mut [u8; PLRU_NODE_SIZE], _>(
            core::slice::from_raw_parts_mut(ptr.load() as *mut u8, PLRU_NODE_SIZE)
                .try_into()
                .unwrap(),
        )
    })
}

/// Persistent LRU
#[repr(C)]
pub struct Plru<T> {
    head: Option<PalPtr>,
    tail: Option<PalPtr>,
    // in Blocks? Return evicted element when full
    capacity: u64,
    count: u64,
    size: u64,
    // Satisfy the rust compiler by adding a zero sized phantom here
    // Also, avoid the drop checker by using a raw ptr
    key_type: PhantomData<*const T>,
}

// hack ⛏
const PLRU_ROOT_LENGTH_READ_COMMENT: usize = size_of::<Plru<()>>();

impl<T> Plru<T> {
    pub fn init(capacity: u64) -> Plru<T> {
        Self {
            head: None,
            tail: None,
            capacity,
            size: 0,
            count: 0,
            key_type: PhantomData::default(),
        }
    }

    pub fn touch(&mut self, node_ptr: &PalPtr) -> Result<(), PMapError> {
        if self.head.as_ref() == Some(node_ptr) {
            return Ok(());
        }

        self.cut_node_and_stitch(node_ptr)?;

        // Fixate new head
        let old_head_ptr = self.head.as_ref().expect("Invalid State");
        let old_head: &mut PlruNode<T> = fetch(old_head_ptr).unwrap();
        old_head.fwd = Some(node_ptr.clone());
        let node: &mut PlruNode<T> = fetch(node_ptr).unwrap();
        node.back = self.head.clone();
        self.head = Some(node_ptr.clone());

        Ok(())
    }

    /// Add a new entry into the LRU. Will fail if already present.
    pub fn insert(
        &mut self,
        node_ptr: PalPtr,
        hash: u64,
        size: u64,
        baggage: T,
    ) -> Result<(), PMapError> {
        let new_node = fetch(&node_ptr).unwrap();
        new_node.fwd = None;
        new_node.back = self.head.clone();
        new_node.size = size;
        new_node.key = baggage;
        new_node.hash = hash;

        if let Some(ref mut head_ptr) = self.head.as_mut() {
            let head: &mut PlruNode<T> = fetch(head_ptr).unwrap();
            head.fwd = Some(node_ptr.clone());
            self.head = Some(node_ptr);
        } else {
            // no head existed yet -> newly initialized list
            self.head = Some(node_ptr.clone());
            self.tail = Some(node_ptr);
        }
        self.size += size;
        self.count += 1;
        Ok(())
    }

    /// Checks whether an eviction is necessary and which entry to evict.
    /// This call does not perform the removal itself.
    pub fn evict(&self, size: u64) -> Result<Option<(u64, &T)>, PMapError> {
        if let (Some(ref tail), true) = (self.tail.as_ref(), self.size + size > self.capacity) {
            let node = fetch(tail).unwrap();
            return Ok(Some((node.hash, node.key)));
        }
        Ok(None)
    }

    fn cut_node_and_stitch(&mut self, node_ptr: &PalPtr) -> Result<(), PMapError> {
        let node: &mut PlruNode<T> = fetch(node_ptr).unwrap();
        if let Some(ref mut forward_ptr) = node.fwd.as_mut() {
            let forward: &mut PlruNode<T> = fetch(forward_ptr).unwrap();
            forward.back = node.back.clone();
        }
        if self.tail.as_ref() == Some(node_ptr) {
            self.tail = node.fwd.clone();
        }
        if let Some(ref mut back_ptr) = node.back.as_mut() {
            let back: &mut PlruNode<T> = fetch(back_ptr).unwrap();
            back.fwd = node.fwd.clone();
        }
        if self.head.as_ref() == Some(node_ptr) {
            self.head = node.back.clone();
        }
        node.fwd = None;
        node.back = None;

        Ok(())
    }

    /// Remove a node from cache and deallocate.
    pub fn remove(&mut self, node_ptr: &PalPtr) -> Result<(), PMapError> {
        self.cut_node_and_stitch(node_ptr)?;
        let node: &mut PlruNode<T> = fetch(node_ptr).unwrap();
        self.size -= node.size;
        self.count -= 1;
        Ok(())
    }
}

/// Ephemeral Wrapper around a byte array for sane access code.
///
/// Structure
/// =========
///
///  fwd  ┌───┐
///  <────┤   │ ..
///    .. │   ├────>
///       └───┘ back
///
/// Size Constraint
/// ===============
/// This structure allows for a generic member which is to be returned when
/// evictions are happening. The size available to the entire object is 256
/// bytes of which the custom type can occupy at most 208 bytes.
///
/// Safety
/// ======
/// Using this wrapper requires transmuting the underlying byte array, which
/// invalidates, when used on references, all borrow checker guarantees. Use
/// with extrem caution, and be sure what you are doing.
#[repr(C)]
pub struct PlruNode<T> {
    fwd: Option<PalPtr>,
    back: Option<PalPtr>,
    size: u64,
    hash: u64,
    key: T,
}
pub(super) const PLRU_NODE_SIZE: usize = 256;

impl<T> PlruNode<T> {
    const SIZE_CONSTRAINT: () = assert!(
        std::mem::size_of::<PlruNode<T>>() < PLRU_NODE_SIZE,
        "Size of attached data to LRU entry surpasses size constraint."
    );

    pub fn new(fwd: Option<PalPtr>, back: Option<PalPtr>, size: u64, hash: u64, key: T) -> Self {
        // has to remain to ensure that the code path is evaluated by rustc
        let _ = Self::SIZE_CONSTRAINT;
        Self {
            fwd,
            back,
            size,
            hash,
            key,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pmem_hashmap::allocator::Pal;
    use std::{path::PathBuf, process::Command};
    use tempfile::Builder;

    struct TestFile(PathBuf);

    impl TestFile {
        pub fn new() -> Self {
            TestFile(
                Builder::new()
                    .tempfile()
                    .expect("Could not get tmpfile")
                    .path()
                    .to_path_buf(),
            )
        }

        pub fn path(&self) -> &PathBuf {
            &self.0
        }
    }
    impl Drop for TestFile {
        fn drop(&mut self) {
            if !Command::new("rm")
                .arg(self.0.to_str().expect("Could not pass tmpfile"))
                .output()
                .expect("Could not delete")
                .status
                .success()
            {
                eprintln!("Could not delete tmpfile");
            }
        }
    }

    #[test]
    fn new() {
        let file = TestFile::new();
        let pal = Pal::create(file.path(), 32 * 1024 * 1024, 0o666).unwrap();
        let root = pal.root(size_of::<Plru<()>>()).unwrap();
        let plru = root.load() as *mut Plru<()>;
        unsafe { plru.write_unaligned(Plru::<()>::init(32 * 1024 * 1024)) };
    }

    #[test]
    fn insert() {
        let file = TestFile::new();
        let pal = Pal::create(file.path(), 32 * 1024 * 1024, 0o666).unwrap();
        let root = pal.root(size_of::<Plru<()>>()).unwrap();
        let plru = root.load() as *mut Plru<()>;
        unsafe { plru.write_unaligned(Plru::<()>::init(32 * 1024 * 1024)) };

        // Insert 3 entries
        for id in 0..3 {
            let node_ptr = pal.allocate(PLRU_NODE_SIZE).unwrap();
            let node = unsafe { node_ptr.load() as *mut PlruNode<()> };
            let plru = unsafe { plru.as_mut().unwrap() };
            plru.insert(node_ptr.clone(), id, 312, ()).unwrap();
            assert_eq!(plru.head, Some(node_ptr));
        }
        let plru = unsafe { plru.as_mut().unwrap() };
        assert_eq!(plru.count, 3);
    }

    #[test]
    fn touch() {
        let file = TestFile::new();
        let pal = Pal::create(file.path(), 32 * 1024 * 1024, 0o666).unwrap();
        let root = pal.root(size_of::<Plru<()>>()).unwrap();
        let plru = root.load() as *mut Plru<()>;
        unsafe { plru.write_unaligned(Plru::<()>::init(32 * 1024 * 1024)) };

        // Insert 3 entries
        let mut ptr = vec![];
        for id in 0..3 {
            let node_ptr = pal.allocate(PLRU_NODE_SIZE).unwrap();
            ptr.push(node_ptr.clone());
            let node = unsafe { node_ptr.load() as *mut PlruNode<()> };
            let plru = unsafe { plru.as_mut().unwrap() };
            plru.insert(node_ptr.clone(), id, 312, ()).unwrap();
            assert_eq!(plru.head, Some(node_ptr));
        }
        let plru = unsafe { plru.as_mut().unwrap() };
        assert_eq!(plru.count, 3);

        for ptr in ptr.iter() {
            plru.touch(ptr);
            assert_eq!(plru.head, Some(ptr).cloned());
        }
    }

    #[test]
    fn evict() {
        let file = TestFile::new();
        let pal = Pal::create(file.path(), 32 * 1024 * 1024, 0o666).unwrap();
        let root = pal.root(size_of::<Plru<()>>()).unwrap();
        let plru = root.load() as *mut Plru<()>;
        unsafe { plru.write_unaligned(Plru::<()>::init(32 * 1024 * 1024)) };

        // Insert 3 entries
        let mut ptr = vec![];
        for id in 0..3 {
            let node_ptr = pal.allocate(PLRU_NODE_SIZE).unwrap();
            ptr.push(node_ptr.clone());
            let node = unsafe { node_ptr.load() as *mut PlruNode<()> };
            let plru = unsafe { plru.as_mut().unwrap() };
            plru.insert(node_ptr.clone(), id, 15 * 1024 * 1024, ())
                .unwrap();
            assert_eq!(plru.head, Some(node_ptr));
        }
        let plru = unsafe { plru.as_mut().unwrap() };
        assert_eq!(plru.count, 3);

        assert_eq!(plru.evict(0).unwrap(), Some((0, &())));
        plru.remove(&mut ptr[0]).unwrap();
        plru.insert(ptr[0].clone(), 3, 1 * 1024 * 1024, ()).unwrap();
        assert_eq!(plru.evict(0).unwrap(), None);
    }

    #[test]
    fn remove() {
        let file = TestFile::new();
        let pal = Pal::create(file.path(), 32 * 1024 * 1024, 0o666).unwrap();
        let root = pal.root(size_of::<Plru<()>>()).unwrap();
        let plru = root.load() as *mut Plru<()>;
        unsafe { plru.write_unaligned(Plru::<()>::init(32 * 1024 * 1024)) };

        // Insert 3 entries
        let mut ptr = vec![];
        for id in 0..3 {
            let node_ptr = pal.allocate(PLRU_NODE_SIZE).unwrap();
            ptr.push(node_ptr.clone());
            let node = unsafe { node_ptr.load() as *mut PlruNode<()> };
            let plru = unsafe { plru.as_mut().unwrap() };
            plru.insert(node_ptr.clone(), id, 15 * 1024 * 1024, ())
                .unwrap();
            assert_eq!(plru.head, Some(node_ptr));
        }
        let plru = unsafe { plru.as_mut().unwrap() };
        assert_eq!(plru.count, 3);

        for ptr in ptr.iter_mut() {
            plru.remove(ptr);
        }
        assert_eq!(plru.head, None);
        assert_eq!(plru.tail, None);
    }

    #[test]
    fn reinit() {
        let file = TestFile::new();
        let mut ptr = vec![];
        {
            let mut pal = Pal::create(file.path(), 32 * 1024 * 1024, 0o666).unwrap();
            let root = pal.root(size_of::<Plru<()>>()).unwrap();
            let plru = root.load() as *mut Plru<()>;
            unsafe { plru.write_unaligned(Plru::<()>::init(32 * 1024 * 1024)) };

            // Insert 3 entries
            for id in 0..3 {
                let node_ptr = pal.allocate(PLRU_NODE_SIZE).unwrap();
                ptr.push(node_ptr.clone());
                let node = unsafe { node_ptr.load() as *mut PlruNode<()> };
                let plru = unsafe { plru.as_mut().unwrap() };
                plru.insert(node_ptr.clone(), id, 15 * 1024 * 1024, ())
                    .unwrap();
                assert_eq!(plru.head, Some(node_ptr));
            }
            let plru = unsafe { plru.as_mut().unwrap() };
            assert_eq!(plru.count, 3);
        }
        {
            let pal = Pal::open(file.path()).unwrap();
            let root = pal.root(size_of::<Plru<()>>()).unwrap();
            let plru = root.load() as *mut Plru<()>;
            let plru = unsafe { plru.as_mut().unwrap() };

            assert_eq!(plru.head, Some(ptr.last().unwrap().clone()));
            assert_eq!(plru.tail, Some(ptr.first().unwrap().clone()));
            for ptr in ptr.iter().rev() {
                assert_eq!(plru.head, Some(ptr.clone()));
                plru.remove(ptr);
            }
            assert_eq!(plru.count, 0);
        }
    }
}
