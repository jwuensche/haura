use crossbeam_channel::Receiver;
use pmem_hashmap::allocator::PalPtr;

use super::{PCacheRoot, Persistent};

pub enum Msg<T> {
    Touch(PalPtr),
    Remove(PalPtr),
    Insert(PalPtr, u64, u64, T),
}

pub fn main<T>(rx: Receiver<Msg<T>>, mut root: Persistent<PCacheRoot<T>>) {
    // TODO: Error handling with return to valid state in the data section..
    while let Ok(msg) = rx.recv() {
        match msg {
            Msg::Touch(ptr) => {
                let mut lru = root.lru.write();
                lru.touch(&ptr);
            }
            Msg::Remove(mut ptr) => {
                let mut lru = root.lru.write();
                lru.remove(&ptr);
                ptr.free();
            }
            Msg::Insert(ptr, hash, size, baggage) => {
                let mut lru = root.lru.write();
                lru.insert(ptr, hash, size, baggage);
            }
        }
    }
}
