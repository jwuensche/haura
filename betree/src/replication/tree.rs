use owning_ref::OwningRef;
use parking_lot::{RwLock, RwLockReadGuard};
use pmem_hashmap::allocator::{Pal, PalError, PalPtr};

/// A basic BTree implementation using PalPtr.
///
///

// Order of a BTree
const M: usize = 5;

struct Node<K, V> {
    values: [Option<(K, V)>; M],
    // Fine granular locking, could be a way to do some more efficient inserts *while* reading from the tree.
    child: [Child<Node<K, V>>; M + 1],
}

enum Child<T> {
    Leaf,
    Node(PalPtr<T>),
}

pub struct PBTree<K, V> {
    root: PalPtr<Node<K, V>>,
}

impl<K: Ord, V> PBTree<K, V> {
    pub fn new(pal: Pal) -> Result<Self, PalError> {
        let mut root = pal.allocate(std::mem::size_of::<Node<K, V>>())?;
        root.init(&Node::new(), std::mem::size_of::<Node<K, V>>());
        Ok(Self { root })
    }

    pub fn get(&self, key: &K) -> Option<&(K, V)> {
        let mut node = &self.root;
        loop {
            match node.load().walk(key) {
                NodeWalk::Miss => return None,
                NodeWalk::Found(idx) => return node.load().get(idx).as_ref(),
                NodeWalk::Child(idx) => match node.load().child.get(idx).unwrap() {
                    Child::Node(ref n) => node = n,
                    Child::Leaf => unreachable!(),
                },
            }
        }
    }

    pub fn insert(&mut self, key: K, val: V) {
        let mut node = &mut self.root;
        let mut path = vec![];
        loop {
            path.push(node.clone());
            match node.load().walk(&key) {
                NodeWalk::Miss => {
                    if let Some((left, median, right)) = node.load_mut().insert(key, val) {
                        // Deal with adjacent nodes
                        todo!();
                        for node in path.into_iter().rev() {}
                    }
                    return;
                }
                NodeWalk::Found(idx) => {
                    node.load_mut()
                        .values
                        .get_mut(idx)
                        .unwrap()
                        .as_mut()
                        .map(|entry| entry.1 = val);
                    return;
                }
                NodeWalk::Child(idx) => match node.load_mut().child.get_mut(idx).unwrap() {
                    Child::Node(ref mut n) => node = n,
                    Child::Leaf => unreachable!(),
                },
            }
        }
    }
}

enum NodeWalk {
    Miss,
    Found(usize),
    Child(usize),
}

impl<K: Ord, V> Node<K, V> {
    pub fn new() -> Self {
        Node {
            values: [0; M].map(|_| None),
            child: [0; M + 1].map(|_| Child::Leaf),
        }
    }

    pub fn walk(&self, key: &K) -> NodeWalk {
        for pos in 0..M {
            if let Some(ref pair) = self.values[pos] {
                if pair.0 == *key {
                    return NodeWalk::Found(pos);
                }
                if pair.0 < *key {
                    return match self.child[pos] {
                        Child::Leaf => NodeWalk::Miss,
                        Child::Node(_) => NodeWalk::Child(pos),
                    };
                }
            } else {
                break;
            }
        }
        match self.child[M] {
            Child::Leaf => NodeWalk::Miss,
            Child::Node(_) => NodeWalk::Child(M),
        }
    }

    pub fn insert(&mut self, key: K, value: V) -> Option<(Node<K, V>, (K, V), Node<K, V>)> {
        if self.values.last().is_some() {
            // TODO: Split the node and insert value
            let mut res = self.split_at(M / 2);
            if key <= res.1 .0 {
                assert!(res.0.insert(key, value).is_none());
            } else {
                assert!(res.2.insert(key, value).is_none());
            }
            Some(res)
        } else {
            // Insert entry into remaining space
            for entry in self.values.iter_mut() {
                if entry.is_none() {
                    *entry = Some((key, value));
                    break;
                }
            }
            None
        }
    }

    pub fn get(&self, idx: usize) -> &Option<(K, V)> {
        self.values.get(idx).unwrap()
    }
    pub fn remove(&mut self, key: K) -> Option<(K, V)> {
        todo!()
    }

    pub fn splice_at(&mut self, kv: (K, V), idx: usize) {
        assert!(idx > 0);
        assert!(idx < M + 1);
        assert!(self.values[M - 1].is_none());
        for cur in (idx..M).rev() {
            self.values[cur] = self.values[cur - 1].take();
        }
    }

    // Move left and right section of keys down to the
    pub fn split_at(&mut self, idx: usize) -> (Node<K, V>, (K, V), Node<K, V>) {
        let mut left = Self::new();
        let mut right = Self::new();
        let mut cur = 0;

        for (pos, c) in left.values.iter_mut().zip(left.child.iter_mut()) {
            if cur > idx {
                break;
            }
            *pos = self.values[cur].take();
            *c = std::mem::replace(&mut self.child[cur], Child::Leaf);
            cur += 1;
        }

        let median = self.values[cur].take().unwrap();
        cur += 1;

        for (pos, c) in right.values.iter_mut().zip(right.child.iter_mut()) {
            if cur == M {
                break;
            }
            *pos = self.values[cur].take();
            *c = std::mem::replace(&mut self.child[cur], Child::Leaf);
            cur += 1;
        }
        right.child[cur - idx + 1] = std::mem::replace(&mut self.child[cur], Child::Leaf);

        (left, median, right)
    }
}
