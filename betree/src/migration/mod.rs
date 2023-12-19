//! Automated migration policies which can migrate both nodes and objects in
//! Haura.
//!
//! There are multiple policies available, they can be found in the
//! [MigrationPolicies] enum.
//!
//! # Usage
//!
//! The migration policy has to be initiated with the initialization of the
//! database itself. See the configuration options of
//! [crate::database::DatabaseConfiguration].  Some policies may have additional
//! configuration options which can be seen in the specific documentation of the
//! policies.
//!
//! For example a simple LFU configuration which moves 1024 blocks of data at
//! once, this configuration could look like this:
//! ```
//! # use betree_storage_stack::{
//! #     database::AccessMode,
//! #     storage_pool::{LeafVdev, TierConfiguration, Vdev},
//! #     Database, DatabaseConfiguration, Dataset, StoragePoolConfiguration, StoragePreference, migration::{MigrationPolicies, MigrationConfig, LfuConfig}, vdev::Block,
//! # };
//! # fn main() {
//! let mut db = Database::build(DatabaseConfiguration {
//!     storage: StoragePoolConfiguration {
//!         tiers: vec![
//!         TierConfiguration::new(vec![Vdev::Leaf(LeafVdev::Memory {
//!             mem: 128 * 1024 * 1024,
//!         })]),
//!         TierConfiguration::new(vec![Vdev::Leaf(LeafVdev::Memory {
//!             mem: 32 * 1024 * 1024,
//!         })]),
//!         ],
//!         ..StoragePoolConfiguration::default()
//!     },
//!     access_mode: AccessMode::AlwaysCreateNew,
//!     migration_policy: Some(MigrationPolicies::Lfu(MigrationConfig {
//!         policy_config: LfuConfig {
//!             promote_size: Block(1024),
//!             ..LfuConfig::default()
//!         },
//!         ..MigrationConfig::default()
//!     })),
//!     ..DatabaseConfiguration::default()
//! }).unwrap();
//! # }
//! ```
//!
//! All policies implement a default configuration which can be used if no
//! specific knowledge is known beforehand. Although, it is good practice to
//! give some help to users for determining a fitting configuration in the
//! policy config documentation. You can find the according documentation from
//! [MigrationPolicies].
//!
//! # Types of Migrations
//!
//! We support two kinds of automated migrations, objects and nodes.
//! **Object migrations** are relatively easy to apply and allow for eager data
//! migration upwards and downwards in the stack.  **Node migration** is more
//! tricky and is currently only implemented lazily via hints given to the DML.
//! This makes downward migrations difficult as the lazy hints are resolved on
//! access.  For further information about this issue and the current state of
//! resolving look at the issue tracker.
//!
//! A policy can use a combination of these migration types and is not forced to
//! use any method over the other. Policies declare in their documentation which
//! kinds are used and how they impact the storage use.
//!
mod errors;
mod lfu;
mod msg;
pub mod placement;
mod reinforcment_learning;

use crossbeam_channel::{Receiver, Sender};
use errors::*;
use itertools::Itertools;
pub use lfu::{LfuConfig, LfuMode};
pub(crate) use msg::*;
use parking_lot::{Mutex, RwLock};
pub use reinforcment_learning::RlConfig;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{atomic::AtomicBool, Arc},
    thread::JoinHandle, mem::MaybeUninit,
};

use crate::{
    database::RootDmu, storage_pool::NUM_STORAGE_CLASSES, tree::PivotKey, vdev::Block, Database,
    StoragePreference,
};

use self::{lfu::Lfu, reinforcment_learning::ZhangHellanderToor};

/// Available policies for auto data placement.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub enum PlacementPolicies {
    Noop,
    /// Least frequently used, promote and demote nodes based on their usage in
    /// the current session.  This policy can use either objects, nodes, or a
    /// combination of these. Currently only objects are advised to be used.
    ///
    /// This policy optimistically promotes data as soon as space is available.
    /// Also a size categorization scheme is used to promote objects based on
    /// rough bucket sizes. This is partially due to other research in this area
    /// as for example Ge et al. 2022, as well as performance measurements with
    /// Haura which showed some contradictions to common assumptions due to
    /// Write-optimization.
    ///
    /// # Configuration
    ///
    /// The configuration of this policy has been shown to be finnicky and has
    /// to be chosen relative well to the existing storage utilization and
    /// access patterns. For more information about this look at [LfuConfig].
    Lfu(MigrationConfig<LfuConfig>),
    /// Reinforcment Learning based tier classfication by Vengerov 2008.
    /// This policy only uses objects and allows for a dynamic fitting of
    /// objects to current and experienced access patterns. The approach is
    /// similar to a temperature scaling of objects in the storage stack and has
    /// been shown to determine access frequencies well in randomized scenarios.
    /// If you will be using Haura with both objet stores and key-value stores
    /// this policies might perform suboptimally as it cannot gain a holistic
    /// view of the state of the storage.
    ///
    /// # Configuration
    ///
    /// The configuration is purely informational but may be expanded in the
    /// future to allow for some experimentation with set learning values. They
    /// are closer described in [RlConfig].
    ReinforcementLearning(MigrationConfig<Option<RlConfig>>),
}

/// Conjoined structure of all parts of a data placement policy.
pub(crate) struct PlacementPolicyAnatomy {
    config: PlacementPolicies,
    /// A recommender algorithm (e.g. Lowest Utilization First)
    predict: PredictionAlgorithm,
    /// A migration algorithm (e.g. LFU, VengerovRL)
    mig: Mutex<Box<dyn MigrationPolicy + Send + Sync>>,
    /// The abstracted view over the entire storage stack.
    /// TODO: Create a standard view over this?
    repr: (),
    terminated: AtomicBool,
}

/// Predictive Placement Selection algorithms, intended for new nodes after
/// splitting or merge operations.
pub enum PredictionAlgorithm {
    /// Pick the least utilized device.
    LeastUtilizedFirst,
    /// Always return [StoragePreference::NONE].
    Noop,
}

impl MigrationPolicy for () {
    fn update(&mut self) -> Result<()> {
        Ok(())
    }

    fn metrics(&self) -> Result<()> {
        Ok(())
    }

    fn promote(&mut self, storage_tier: u8, tight_space: bool) -> Result<Block<u64>> {
        Ok(Block(0))
    }

    fn demote(&mut self, storage_tier: u8, desired: Block<u64>) -> Result<Block<u64>> {
        Ok(Block(0))
    }

    fn db(&self) -> &Arc<RwLock<Database>> {
        todo!()
    }

    fn dmu(&self) -> &Arc<RootDmu> {
        todo!()
    }

    fn config(&self) -> MigrationConfig<()> {
        todo!()
    }
}

impl PlacementPolicyAnatomy {
    pub fn new(config: PlacementPolicies) -> Self {
        Self {
            config,
            predict: PredictionAlgorithm::Noop,
            mig: Mutex::new(Box::new(())),
            repr: (),
            terminated: AtomicBool::new(false),
        }
    }

    pub fn finish_init(&self, dml_rx: Receiver<DmlMsg>, db_rx: Receiver<DatabaseMsg>, db: Arc<RwLock<Database>>) {
        match &self.config {
            PlacementPolicies::Noop => {},
            PlacementPolicies::Lfu(config) => {
                let mut pol = self.mig.lock();
                *pol = Box::new(Lfu::build(dml_rx, db_rx, db, config.clone()));
            }
            PlacementPolicies::ReinforcementLearning(config) => {
                let mut pol = self.mig.lock();
                *pol = Box::new(ZhangHellanderToor::build(dml_rx, db_rx, db, config.clone()));
            }
        }
    }

    pub fn needs_channel(&self) -> bool {
        match self.config {
            PlacementPolicies::Noop => false,
            _ => true,
        }
    }

    pub fn terminate(&self) {
        self.terminated.store(true, Ordering::Relaxed)
    }

    pub fn is_terminated(&self) -> bool {
        self.terminated.load(Ordering::Acquire)
    }

    pub fn query_new(&self) -> StoragePreference {
        // FIXME
        match self.predict {
            PredictionAlgorithm::LeastUtilizedFirst => todo!(),
            PredictionAlgorithm::Noop => StoragePreference::NONE,
        }
    }

    pub fn new_data(&self) -> StoragePreference {
        // FIXME
        self.query_new()
    }

    pub fn new_meta(&self) -> StoragePreference {
        // FIXME
        self.query_new()
    }

    pub fn recommend_write_back(&self, _pivot_key: &PivotKey) -> StoragePreference {
        // FIXME
        self.query_new()
    }

    pub fn migrate(&self) {
        todo!()
    }

    pub fn update(&self) -> Result<()> {
        let mut mig = self.mig.lock();
        mig.update()?;
        Ok(())
    }

    pub fn config(&self) -> MigrationConfig<()> {
        todo!()
    }

    fn main(&self) -> Result<()> {
        let config = self.config();
        std::thread::sleep(config.grace_period);
        let mut migrated = std::time::Instant::now();
        loop {
            self.update()?;
            if migrated.elapsed() > config.update_period {
                self.migrate();
                migrated = std::time::Instant::now();
            }
            // FIXME: hardwired 1 sec polling
            std::thread::sleep(std::time::Duration::from_secs(1));
            if self.is_terminated() {
                break;
            }
        }
        Ok(())
    }
}

use std::sync::atomic::Ordering;
/// Meta-trait which defines a predictive and reactive placement policy for all
/// data in the storage stack.
///
/// All methods of a policy should rely on fine granular locking. Exterior
/// mutability cannot be used.
pub trait PlacementPolicy: Send + Sync {
    /// Stop all background tasks of this policy.
    fn terminate(&self);

    /// Check if background tasks might be occurring for this policy.
    fn is_terminated(&self) -> bool;

    /// Return a recommendation whereto place newly created nodes.
    fn query_new(&self) -> StoragePreference;

    /// Recommendation for new nodes which contain *data*.
    fn new_data(&self) -> StoragePreference;
    /// Recommendation for new nodes which contain *metadata*.
    fn new_meta(&self) -> StoragePreference;

    /// Call to modify an objects position when being the process of a write back.
    /// This call should never block under *any* circumstances.
    fn recommend_write_back(&self, pivot_key: &PivotKey) -> StoragePreference;

    /// Check if migration should be done right now.
    fn migrate(&self);
    /// Process buffered events.
    fn update(&self) -> Result<()>;

    /// A main loop, calling whichever regular operations (e.g. migration) might
    /// be desired by the policy and it's configuration.
    fn main(&self) -> Result<()> {
        let config = self.config();
        std::thread::sleep(config.grace_period);
        let mut migrated = std::time::Instant::now();
        loop {
            self.update()?;
            if migrated.elapsed() > config.update_period {
                self.migrate();
                migrated = std::time::Instant::now();
            }
            // FIXME: hardwired 1 sec polling
            std::thread::sleep(std::time::Duration::from_secs(1));
            if self.is_terminated() {
                break;
            }
        }
        Ok(())
    }

    /// Return a generalized configuration for this policy. Required for the
    /// generic thread main.
    fn config(&self) -> MigrationConfig<()>;
}

/// Spawns a thread running a main function for the given [PlacementPolicy].
/// This thread is mainly concerned with the automated scan & migrate workflow
/// but can depending on the policy encompass more than that.
pub fn spawn_policy(policy: Arc<PlacementPolicyAnatomy>) -> JoinHandle<Result<()>> {
    std::thread::spawn(move || policy.main())
}

impl PlacementPolicies {
    pub(crate) fn construct(
        &self,
    ) -> Arc<PlacementPolicyAnatomy> {
        Arc::new(PlacementPolicyAnatomy::new(self.clone()))
    }
}

use std::time::Duration;

/// Configuration type for [MigrationPolicies]
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct MigrationConfig<Config> {
    /// Time at start where operations are _only_ recorded. This may help in avoiding incorrect early migrations by depending on a larger historical data.
    pub grace_period: Duration,
    /// Threshold at which downwards migrations are considered. Or at which upwards migrations are blocked. Values are on a range of 0 to 1.
    pub migration_threshold: [f32; NUM_STORAGE_CLASSES],
    /// Duration between consumption of operational messages. Enlarging this leads to greater memory usage, but reduces ongoing computational load.
    pub update_period: Duration,
    /// Policy dependent configuration.
    pub policy_config: Config,
}

impl<Config> MigrationConfig<Config> {
    /// Create an erased version of this configuration with the specific
    /// migration policy options removed.
    fn erased(self) -> MigrationConfig<()> {
        MigrationConfig {
            policy_config: (),
            grace_period: self.grace_period,
            migration_threshold: self.migration_threshold,
            update_period: self.update_period,
        }
    }
}

impl<Config: Default> Default for MigrationConfig<Config> {
    fn default() -> Self {
        MigrationConfig {
            grace_period: Duration::from_secs(300),
            migration_threshold: [0.95; NUM_STORAGE_CLASSES],
            update_period: Duration::from_secs(30),
            policy_config: Default::default(),
        }
    }
}

/// An automated migration policy interface definition.
///
/// If you are adding a new policy also include a new variant in
/// [MigrationPolicies] with a short-hand of your policy name to allow the user
/// to create your policy from the database definition.
///
/// When implementing a migration policy you can use two types of messages which
/// are produced. They are divided by user interface and internal tree
/// representation. These messages are defined in the two message types [DmlMsg] and [DatabaseMsg]
pub(crate) trait MigrationPolicy {
    /// Consume all present messages and update the migration selection
    /// status for all afflicted objects
    fn update(&mut self) -> Result<()>;

    /// Run any relevant metric logic such as accumulation and writing out data.
    fn metrics(&self) -> Result<()>;

    /// Promote any amount of data from the given tier to the next higher one.
    ///
    /// This functions returns how many blocks have been migrated in total. When
    /// using lazy node migration also specify the amount of blocks hinted to be
    /// migrated.
    fn promote(&mut self, storage_tier: u8, tight_space: bool) -> Result<Block<u64>>;
    /// Demote atleast `desired` many blocks from the given storage tier to any
    /// tier lower than the given tier.
    fn demote(&mut self, storage_tier: u8, desired: Block<u64>) -> Result<Block<u64>>;

    /// Return a reference to the active [Database].
    fn db(&self) -> &Arc<RwLock<Database>>;

    /// Return a reference to the underlying DML.
    fn dmu(&self) -> &Arc<RootDmu>;

    /// Return the cleaned configuration.
    fn config(&self) -> MigrationConfig<()>;

    /// We provide a basic default implementation which may be used or discarded
    /// if desired.
    fn migrate(&mut self) -> Result<()> {
        use crate::database::StorageInfo;

        let threshold: Vec<f32> = self
            .config()
            .migration_threshold
            .iter()
            .map(|val| val.clamp(0.0, 1.0))
            .collect();
        let infos: Vec<(u8, StorageInfo)> = (0u8..NUM_STORAGE_CLASSES as u8)
            .filter_map(|class| {
                self.dmu()
                    .handler()
                    .free_space_tier(class)
                    .map(|blocks| (class, blocks))
            })
            .collect();

        for ((high_tier, high_info), (low_tier, _low_info)) in infos
            .iter()
            .tuple_windows()
            .filter(|(_, (_, low_info))| low_info.total != Block(0))
        {
            self.promote(
                *low_tier,
                high_info.percent_full() >= threshold[*high_tier as usize],
            )?;
        }

        // Update after iteration
        let infos: Vec<(u8, StorageInfo)> = (0u8..NUM_STORAGE_CLASSES as u8)
            .filter_map(|class| {
                self.dmu()
                    .handler()
                    .free_space_tier(class)
                    .map(|blocks| (class, blocks))
            })
            .collect();

        for ((high_tier, high_info), (_low_tier, _low_info)) in
            infos
                .iter()
                .tuple_windows()
                .filter(|((high_tier, high_info), (low_tier, low_info))| {
                    high_info.percent_full() > threshold[*high_tier as usize]
                        && low_info.percent_full() < threshold[*low_tier as usize]
                })
        {
            let desired: Block<u64> = Block(
                (high_info.total.as_u64() as f32 * (1.0 - threshold[*high_tier as usize])) as u64,
            ) - high_info.free.as_u64();
            self.demote(*high_tier, desired)?;
        }
        self.metrics()?;
        Ok(())
    }
}
