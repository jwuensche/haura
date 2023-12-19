//! WIP - data placement logic which ties together multiple layers of the tree
//! to gather holistic information for all data placement decisions required.
//!
//! The [PlacementPolicy] defines actions of a placement policy, which may be
//! used in the [crate::tree] or [crate::data_management] module to interact
//! with a policy.
use crate::{StoragePreference, tree::PivotKey};

use super::PlacementPolicy;


impl PlacementPolicy for () {
    fn migrate(&self) {
        // NO-OP
    }

    fn new_data(&self) -> StoragePreference {
        StoragePreference::NONE
    }

    fn new_meta(&self) -> StoragePreference {
        StoragePreference::NONE
    }

    fn query_new(&self) -> StoragePreference {
        StoragePreference::NONE
    }

    fn recommend_write_back(&self, _pivot_key: &PivotKey) -> StoragePreference {
        StoragePreference::NONE
    }

    fn terminate(&self) {
        todo!()
    }

    fn is_terminated(&self) -> bool {
        true
    }

    fn update(&self) -> super::Result<()> {
        Ok(())
    }

    fn config(&self) -> super::MigrationConfig<()> {
        todo!()
    }
}
