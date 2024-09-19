//! Various impl of a "leaf" type node.

use crate::cow_bytes::CowBytes;

/// Case-dependent outcome of a rebalance operation.
#[derive(Debug)]
pub(super) enum FillUpResult {
    Rebalanced {
        pivot_key: CowBytes,
        size_delta: isize,
    },
    Merged {
        size_delta: isize,
    },
}

pub(crate) mod copyless_leaf;
pub(crate) mod leaf;
pub(crate) mod packed;

pub use copyless_leaf::CopylessLeaf;
pub use leaf::LeafNode;
pub use packed::PackedMap;
