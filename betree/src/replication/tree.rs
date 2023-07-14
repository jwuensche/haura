use pmem_hashmap::allocator::PalPtr;

/// A basic BTree implementation using PalPtr.
///
///

// Order of a BTree
const M: usize = 5;

struct Node<K, V> {
    values: [Option<(K, V)>; M],
    // Fine granular locking, could be a way to do some more efficient inserts *while* reading from the tree.
    child: [RwLock<PalPtr>; M - 1],
}

enum Child {
    Leaf,
    Node(RwLock<PalPtr>),
}

impl Node<K: Ord, V> {
    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        todo!()
    }

    pub fn get(&self, key: K) -> Option<V> {
        for pos in 0..M {
            if let Some(pair) = self.values[pos] {
                if pair.0 == key {
                   return Some(pair.1)
                }
                if pair.0 > == key {
                    self.child[pos]
                }
            } else {
                break;
            }
        }
        self.values.iter().find(|item| {
            item.is_some() && item.unwrap().0 <
        }).map(|idx| se)
        todo!()
    }
}
