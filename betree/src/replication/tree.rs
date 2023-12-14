use parking_lot::{RwLock, RwLockReadGuard};
use pmem_hashmap::allocator::{Pal, PalError, PalPtr};

use super::shift_array::ShiftArray;

// Order of a BTree
const B: usize = 16;
const NUM_KEYS: usize = B - 1;
const MIN: usize = B / 2 + B % 2;

pub struct Node<K, V> {
    pivots: ShiftArray<K, NUM_KEYS>,
    children: ShiftArray<Link<K, V>, B>,
}

impl<K: std::fmt::Debug + Ord + Clone, V: std::fmt::Debug> std::fmt::Debug for Node<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Node")
            .field("pivots", &self.pivots)
            .field("count", &self.count())
            .field(
                "children",
                &self
                    .children
                    .iter()
                    .filter_map(|e| match e {
                        Link::Entry(_, val) => None,
                        Link::Child(n) => Some(n.load()),
                    })
                    .collect::<Vec<&Node<K, V>>>(),
            )
            .finish()
    }
}

pub enum Link<K, V> {
    Entry(K, V),
    Child(PalPtr<Node<K, V>>),
}

impl<K, V> Link<K, V> {
    fn assert_child(&mut self) -> &mut PalPtr<Node<K, V>> {
        match self {
            Link::Entry(_, _) => panic!("Link was not a child."),
            Link::Child(c) => c,
        }
    }
}

#[derive(Debug)]
pub struct PBTree<K, V> {
    root: PalPtr<Node<K, V>>,
}

impl<K: Ord + Clone + std::fmt::Debug, V> PBTree<K, V> {
    pub fn new(pal: &Pal) -> Result<Self, PalError> {
        let mut root = pal.allocate(std::mem::size_of::<Node<K, V>>())?;
        root.init(&Node::new(), std::mem::size_of::<Node<K, V>>());
        Ok(Self { root })
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        let mut node = &self.root;
        loop {
            // dbg!(node);
            // if node.load().children.size() > 100 {
            //     println!("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$ LINK IS BROKEN $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
            //     dbg!(node);
            //     println!("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
            // }
            match node.load().walk(key) {
                NodeWalk::Miss => return None,
                NodeWalk::Found(idx) => return node.load().get(idx),
                NodeWalk::Child(idx) => {
                    // println!("######################################################################################");
                    // dbg!(&node.load().pivots);
                    // for n in node.load().children.iter() {
                    //     match n {
                    //         Link::Entry(_) => unreachable!(),
                    //         Link::Child(ref n) => { dbg!(&n.load().pivots); },
                    //     }
                    // }
                    match node.load().children.get(idx).unwrap() {
                    Link::Entry(_, _) => unreachable!(),
                    Link::Child(ref n) => node = n,
                }},
            }
        }
    }

    // pub fn remove(&mut self, key: &K) {
    //     let mut node = &mut self.root;
    //     let mut path = vec![];
    //     loop {
    //         path.push(node.clone());
    //         match node.load().walk(&key) {
    //             NodeWalk::Miss => break,
    //             NodeWalk::Found(_) => {
    //                 // Found leaf containing node, escalate removal upwards until no more changes required
    //                 //
    //                 // Each deletion can have multiple cases:
    //                 //
    //                 // - Leafs are fine (OK)
    //                 // - Leafs are underfilled:
    //                 //  - Move elements from neighboring leafs (left or right) and change pivot elements accordingly
    //                 //  - All other leafs are of size MIN, merge children.
    //                 // - Parents contain key as index: Delete and replace with highest element from left child

    //                 if node.load_mut().remove(key) {
    //                     // Treat small leaf
    //                     // 1. Check if left or right child has enough elements
    //                     if path.is_empty() {
    //                         // emptied root node
    //                         return;
    //                     }
    //                     let mut parent = path.last_mut().unwrap().load_mut();
    //                     let idx = match parent.walk(key) {
    //                         NodeWalk::Child(idx) => idx,
    //                         _ => unreachable!(),
    //                     };

    //                     if idx > 0
    //                         && parent
    //                             .children
    //                             .get_mut(idx - 1)
    //                             .unwrap()
    //                             .assert_child()
    //                             .load()
    //                             .size()
    //                             > MIN
    //                     {
    //                         // Pick from left child
    //                         let left = parent
    //                             .children
    //                             .get_mut(idx - 1)
    //                             .unwrap()
    //                             .assert_child()
    //                             .load_mut();

    //                         let new_child = left.children.pop_back().unwrap();
    //                         let new_pivot = left.pivots.pop_back().unwrap();
    //                         node.load_mut().children.push_front(new_child);
    //                         node.load_mut().pivots.push_front(new_pivot);
    //                         *parent.pivots.get_mut(idx).unwrap() = left.pivot_high();
    //                     }

    //                     if idx + 1 < B
    //                         && parent
    //                             .children
    //                             .get_mut(idx + 1)
    //                             .unwrap()
    //                             .assert_child()
    //                             .load()
    //                             .size()
    //                             > MIN
    //                     {
    //                         // Pick from right child
    //                         let right = parent
    //                             .children
    //                             .get_mut(idx + 1)
    //                             .unwrap()
    //                             .assert_child()
    //                             .load_mut();

    //                         let new_child = right.children.pop_front().unwrap();
    //                         let new_pivot = right.pivots.pop_front().unwrap();
    //                         node.load_mut().children.push_back(new_child);
    //                         node.load_mut().pivots.push_back(new_pivot);
    //                         *parent.pivots.get_mut(idx).unwrap() = node.load().pivot_high();
    //                     }

    //                     todo!("Merge children")
    //                 } else {
    //                     // Remove from parents if they contain the key
    //                     for mut n in path.into_iter() {
    //                         assert!(!n.load_mut().remove(key))
    //                     }
    //                 }
    //                 break;
    //             }
    //             NodeWalk::Child(idx) => {
    //                 match node.clone().load_mut().children.get_mut(idx).unwrap() {
    //                     Link::Entry(_) => unreachable!(),
    //                     Link::Child(ref mut n) => node = n,
    //                 }
    //             }
    //         }
    //     }
    // }

    pub fn insert(&mut self, key: K, val: V, pal: &Pal) {
        if let Some((k, v)) = self.insert_from(key, val, pal, self.root.clone()) {
            assert!(self.insert_from(k, v, pal, self.root).is_none());
        }
        println!("inserted");
    }

    fn insert_from(
        &mut self,
        key: K,
        val: V,
        pal: &Pal,
        mut from: PalPtr<Node<K, V>>,
    ) -> Option<(K, V)> {
        println!("insert from");
        let mut node = from;
        let mut path = vec![];
        loop {
            path.push(node.clone());
            match node.load().walk(&key) {
                NodeWalk::Miss => {
                    return if let Some((median, new_node, value)) =
                        node.load_mut().insert(key.clone(), val)
                    {
                        // Insert facilitated a split, insert new node into parent
                        let mut pair = Some((median, new_node)).map(|(key, new_node)| {
                            // Allocate the new node
                            (key, pal.allocate_variable(new_node).unwrap())
                        });
                        for mut cur_node in path.iter_mut().rev().skip(1) {
                            // dbg!(&cur_node);
                            if let Some((key, new_node)) = pair {
                                // dbg!(cur_node.load().children.size());
                                let mut foo = cur_node.load_mut();
                                    pair = foo.escalate(key, new_node).map(|(key, new_node)| {
                                    // Allocate the new node
                                    (key, pal.allocate_variable(new_node).unwrap())
                                });
                                debug_assert!(foo.is_valid());
                                assert!(foo.children.size() < 1000);
                                // dbg!(foo.children.size());
                            } else {
                                break;
                            }
                        }

                        // Create a new root node
                        if let Some((key, new_node)) = pair {
                            println!("Creating new root");
                            let mut new_root = Node::new();
                            new_root.pivots.push_front(key);
                            // new_root.pivots.push_back(new_node.load().pivot_high());
                            println!("Old root: {:?}", self.root);
                            new_root.children.push_front(Link::Child(self.root));
                            new_root.children.push_back(Link::Child(new_node));
                            self.root = pal.allocate_variable(new_root).unwrap();
                            // dbg!(self.root);
                        }
                        Some((key, value))
                    } else {
                        None
                    };
                }
                NodeWalk::Found(idx) => {
                    node.load_mut()
                        .children
                        .get_mut(idx)
                        .map(|entry| match entry {
                            Link::Entry(_, ref mut v) => *v = val,
                            Link::Child(_) => unreachable!(),
                        });
                    return None;
                }
                NodeWalk::Child(idx) => {
                    match node.clone().load_mut().children.get_mut(idx).unwrap() {
                        Link::Entry(_, _) => unreachable!(),
                        Link::Child(ref mut n) => node = n.clone(),
                    }
                }
            }
        }
    }
}

#[derive(Debug)]
enum NodeWalk {
    Miss,
    Found(usize),
    Child(usize),
}

impl<K: Ord + Clone + std::fmt::Debug, V> Node<K, V> {
    pub fn new() -> Self {
        Node {
            pivots: ShiftArray::new(),
            children: ShiftArray::new(),
        }
    }

    fn walk(&self, key: &K) -> NodeWalk {
        let mut idx = 0;
        let pos = loop {
            if idx >= B - 1 {
                break B - 1;
            }
            if self.pivots.get(idx).is_none() {
                break idx;
            }
            if self.pivots.get(idx).unwrap() == key {
                // Inspect Child
                return match self.children.get(idx).as_ref().unwrap() {
                    Link::Entry(_, _) => NodeWalk::Found(idx),
                    Link::Child(_) => NodeWalk::Child(idx),
                };
            }
            if self.pivots.get(idx).unwrap() > key {
                break idx;
            }
            idx += 1;
        };

        match self.children.get(pos) {
            Some(ref ptr) => match ptr {
                Link::Entry(_, _) => NodeWalk::Miss,
                Link::Child(ref child) => NodeWalk::Child(idx),
            },
            None => NodeWalk::Miss,
        }
    }

    pub fn insert(&mut self, key: K, value: V) -> Option<(K, Node<K, V>, V)> {
        if self.pivots.size() < NUM_KEYS {
            self.splice(key, value);
            None
        } else {
            // Split the node and escalate
            let (new_key, mut right) = self.split();
            debug_assert!(self.is_valid());
            // assert!(right.insert(key, value).is_none());
            Some((new_key, right, value))
        }
    }

    pub fn split(&mut self) -> (K, Node<K, V>) {
        assert!(self.pivots.size() == NUM_KEYS);
        assert!(self.children.size() >= NUM_KEYS);
        const idx: usize = NUM_KEYS / 2 + NUM_KEYS % 2 - 1;
        // dbg!(idx);

        let right_pivots = self.pivots.split_after(idx);
        let right_children = self.children.split_after(idx);
        // dbg!(self.pivots.size());
        // dbg!(self.children.size());
        // dbg!(right_pivots.size());
        // dbg!(right_children.size());

        let right = Self {
            pivots: right_pivots,
            children: right_children,
        };
        assert!(self.pivot_high() < right.pivot_low());

        (self.pivot_high(), right)
    }

    pub fn is_valid(&self) -> bool {
        dbg!(&self.pivots);
        for (idx, pivot) in self.pivots.iter().enumerate() {
            // Left child must exist
            dbg!(self.children.iter().map(|c| match c {Link::Child(c) => c.load().pivot_low(), Link::Entry(_, _) => pivot.clone()}).collect::<Vec<_>>());
            let left_valid = match self.children.get(dbg!(idx)).unwrap() {
                Link::Entry(_,_) => true,
                Link::Child(c) => { dbg!(&c.load().pivots); dbg!(c.load().pivot_low() < *pivot) && dbg!(dbg!(c.load().pivot_high()) == *dbg!(pivot)) && dbg!(c.load().is_valid())},
            };
            // May not exist
            // let right_valid = self.children.get(idx + 1).map(|l| match l {
            //     Link::Entry(_) => true,
            //     Link::Child(c) => c.load().pivot_low() > *pivot && c.load().pivot_high() > *pivot && c.load().is_valid(),
            // }).unwrap_or(true);
            let right_valid = true;
            if !dbg!(left_valid) || !dbg!(right_valid) {
                return false;
            }
        }
        true
    }

    /// Add the node to self or split this node also and insert it to the right hand side?
    pub fn escalate(&mut self, key: K, right: PalPtr<Node<K, V>>) -> Option<(K, Node<K, V>)> {
        if self.pivots.size() <= NUM_KEYS && self.children.size() < B {
            println!("can buffer node");

            // If full either append only or insert and shift
            let mut idx = self.pivots.find(&key).unwrap();
            // let mut idx_test = self.pivots.find(&right.load().pivot_low()).unwrap();
            // assert_eq!(idx, idx_test);
            if self.pivots.size() == NUM_KEYS {
                if idx == self.pivots.size() {
                    self.children.push_back(Link::Child(right));
                    debug_assert!(self.is_valid());
                    return None
                }
                self.pivots.pop_back();
                self.pivots.insert(idx, key);
                self.children.insert(idx + 1, Link::Child(right));
                debug_assert!(self.is_valid());
                return None
            }

            self.pivots.insert(idx, key);
            self.children.insert(idx + 1, Link::Child(right));
            debug_assert!(self.is_valid());

            // // Shift pivot and child
            // if self.pivots.size() == NUM_KEYS {
            //     let _ = self.pivots.pop_back();
            // }
            // let mut idx = self.pivots.find(&key).unwrap();
            // // Children space is available, shift
            // let child_idx = if self.pivot_high() < key {
            //     idx + 1
            // } else { idx };
            // self.pivots.insert(idx, key.clone());
            // dbg!(child_idx);
            // dbg!(right.load().pivot_low());
            // self.children.insert(child_idx, Link::Child(right));
            // dbg!(&self.pivots);
            // dbg!((0..self.pivots.size()).map(|idx| match self.children.get(idx).unwrap() { Link::Child(c) => c.load().pivot_low(), Link::Entry(e) => k.clone()}).collect::<Vec<_>>());
            // debug_assert!(self.is_valid());
            // dbg!(self.children.size());
            None
        } else {
            let (upper, mut new_right) = self.split();

            if self.pivots.size() == self.children.size() - 1 {
                // Pull the last childs max key into the node to create a consistent state
                let last_child = self.children.last().unwrap();
                self.pivots.push_back(match last_child {
                    Link::Entry(k, e) => k.clone(),
                    Link::Child(c) => c.load().pivot_high(),
                })
            }

            // Check which side should contain the splitted node.  We don't know
            // the position from wich the node was splitted. If it was the left
            // most for example it would need to be inserted into the left old
            // node.
            if new_right.pivot_low() < key {
                assert!(new_right.escalate(key, right).is_none());
            } else {
                // Otherwise insert into self.
                assert!(self.escalate(key, right).is_none());
            }
            // dbg!(self.children.size());
            Some((upper, new_right))
        }
    }

    pub fn get(&self, idx: usize) -> Option<&V> {
        match self.children.get(idx).as_ref().unwrap() {
            Link::Entry(_, ref v) => Some(v),
            Link::Child(_) => None,
        }
    }

    pub fn pivot_high(&self) -> K {
        self.pivots.last().unwrap().clone()
    }

    pub fn pivot_low(&self) -> K {
        self.pivots.first().unwrap().clone()
    }

    /// Returns the number of valid pivot entries. If this number is larger than
    /// [MIN], entries may be revoked without tree restructure.
    pub fn size(&self) -> usize {
        self.pivots.size()
    }

    /// Returns true if merge is needed.
    pub fn remove(&mut self, key: &K) -> bool {
        let idx = self.pivots.find(key).unwrap();
        let remove_pivot;
        match self.children.get_mut(idx).unwrap() {
            Link::Entry(_, _) => {
                self.pivots.remove(idx);
                remove_pivot = true;
            }
            Link::Child(c) => {
                *self.pivots.get_mut(idx).unwrap() = c.load().pivot_high();
                remove_pivot = false;
            }
        }
        if remove_pivot {
            self.pivots.remove(idx);
        }
        self.pivots.size() < MIN
    }

    pub fn splice(&mut self, mut key: K, mut val: V) {
        assert!(self.pivots.size() < NUM_KEYS);
        let idx = self.pivots.find(&key).unwrap_or(0);
        self.pivots.insert(idx, key.clone());
        // This may not work
        self.children.insert(idx, Link::Entry(key, val));
    }

    pub fn count(&self) -> usize {
        self.children
            .iter()
            .map(|e| match e {
                Link::Entry(_, e) => 1,
                Link::Child(c) => c.load().count(),
            })
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pmem_hashmap::allocator::Pal;
    use std::{collections::HashSet, path::PathBuf, process::Command};
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
        let mut pal = Pal::create(file.path(), 32 * 1024 * 1024, 0o666).unwrap();
        let tree: PBTree<u8, u8> = PBTree::new(&pal).unwrap();
    }

    #[test]
    fn basic_insert() {
        let file = TestFile::new();
        let mut pal = Pal::create(file.path(), 32 * 1024 * 1024, 0o666).unwrap();
        let mut tree: PBTree<u8, u8> = PBTree::new(&pal).unwrap();
        tree.insert(1, 1, &pal);
    }

    #[test]
    fn basic_get() {
        let file = TestFile::new();
        let mut pal = Pal::create(file.path(), 32 * 1024 * 1024, 0o666).unwrap();
        let mut tree: PBTree<u8, u8> = PBTree::new(&pal).unwrap();
        assert!(tree.get(&1).is_none());
        tree.insert(1, 1, &pal);
        assert_eq!(tree.get(&1), Some(&1));
    }

    #[test]
    fn seq_insert() {
        let file = TestFile::new();
        let mut pal = Pal::create(file.path(), 128 * 1024 * 1024, 0o666).unwrap();
        let mut tree: PBTree<u8, u8> = PBTree::new(&pal).unwrap();

        for id in 0..=255 {
            println!("################ INSERTING ID: {id}");
            tree.insert(id, id, &pal);
            for n in 0..=id {
                assert_eq!(tree.get(&n), Some(&n));
            }
        }

        for id in 0..=255 {
            assert_eq!(tree.get(&id), Some(&id));
        }
        dbg!(tree.root.load());
    }

    #[test]
    fn rnd_insert() {
        let file = TestFile::new();
        let mut pal = Pal::create(file.path(), 128 * 1024 * 1024, 0o666).unwrap();
        let mut tree = PBTree::new(&pal).unwrap();

        use rand::Rng;
        let mut rng = rand::thread_rng();
        let vals = [0u8; 512].map(|_| rng.gen::<u16>());
        let set = HashSet::from(vals);

        let mut inserted = vec![];
        for id in set.iter() {
            println!("ID: {id}");
            dbg!(tree.root.load().count());
            tree.insert(id, id, &pal);
            dbg!(tree.root.load().count());
            assert!(tree.root.load().is_valid());
            inserted.push(id);
            for x in inserted.iter() {
                println!("Key: {x}");
                assert_eq!(tree.get(x), Some(x));
            }
        }

        for id in set.iter() {
            assert_eq!(tree.get(&id), Some(&id));
        }
    }
}
