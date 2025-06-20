use core::str;
use std::sync::atomic::Ordering;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::usize;

use tracing::info;
use tracing_subscriber::filter::Targets;

use crate::db::common::*;
use crate::db::db_log;
use crate::db::level::LevelStorege;
use crate::db::logfile::LogFile;
use crate::db::memtable::Memtable;
use crate::db::store::Filestore;
use crate::db::store::Memstore;
use crate::db::store::Store;
use crate::db::table::TableReader;

use super::key::KeySlice;
use super::level;
use super::level::LevelStoregeConfig;
use super::level::TableChange;
use super::store::StoreId;
use super::table::TableConfig;

#[derive(Clone, Copy)]
pub struct LsmStorageConfig {
    pub memtable_size_limit: usize,
    pub level_config: LevelStoregeConfig,
}
impl Default for LsmStorageConfig {
    fn default() -> Self {
        LsmStorageConfig {
            level_config: LevelStoregeConfig::default(),
            memtable_size_limit: 2 * 1024 * 1024, //4MB
        }
    }
}
impl LsmStorageConfig {
    pub fn config_for_test() -> Self {
        LsmStorageConfig {
            level_config: LevelStoregeConfig::config_for_test(),
            memtable_size_limit: 1024,
        }
    }
}
pub struct LsmStorage<T: Store> {
    m: Arc<Memtable>,
    //  immutable memtable
    imm: Vec<Arc<Memtable>>,
    current: LevelStorege<T>,
}
impl<T: Store> Clone for LsmStorage<T> {
    fn clone(&self) -> Self {
        LsmStorage {
            m: self.m.clone(),
            imm: self.imm.clone(),
            current: self.current.clone(),
        }
    }
}
impl<T: Store> LsmStorage<T> {
    pub fn from(config: LsmStorageConfig, level: LevelStorege<T>) -> Self {
        LsmStorage {
            m: Arc::new(Memtable::new(config.memtable_size_limit)),
            imm: Vec::new(),
            current: level,
        }
    }
    pub fn new(config: LsmStorageConfig) -> Self {
        LsmStorage {
            m: Arc::new(Memtable::new(config.memtable_size_limit)),
            imm: Vec::new(),
            current: LevelStorege::new(vec![], config.level_config),
        }
    }

    pub fn table_num_in_levels(&self) -> Vec<usize> {
        self.current.table_num_in_levels()
    }

    pub fn need_compact(&self) -> bool {
        self.current.need_compact()
    }

    pub fn log_lsm_debug_info(&self) {
        if !tracing::event_enabled!(tracing::Level::INFO) {
            return;
        }
        let mut debug_info = String::new();
        debug_info.push_str("LsmStorage Debug Info:\n");

        // Active memtable size
        debug_info.push_str(&format!(
            "  Active Memtable Size: {} bytes\n",
            self.m.get_size()
        ));

        // Immutable memtable info
        let imm_tables_num = self.imm.len();
        let imm_tables_size: usize = self.imm.iter().map(|mem| mem.get_size()).sum();
        debug_info.push_str(&format!(
            "  Immutable Memtables: {} (Total Size: {} bytes)\n",
            imm_tables_num, imm_tables_size
        ));

        // Information for tables in each level
        debug_info.push_str("  Levels:\n");
        let levels_info = self.current.get_tables_level();
        for (level_idx, level_tables) in levels_info.iter().enumerate() {
            debug_info.push_str(&format!(
                "    Level {}: ({} tables)\n",
                level_idx,
                level_tables.len()
            ));
            if level_tables.is_empty() {
                debug_info.push_str("      [Empty]\n");
            } else {
                for table in level_tables {
                    let (first_key, last_key) = table.key_range();
                    debug_info.push_str(&format!(
                        "      Table ID: {}, Key Range: [\"{}\" -> \"{}\"]\n",
                        table.store_id(),
                        first_key.to_string(),
                        last_key.to_string()
                    ));
                }
            }
        }
        tracing::info!("{}", debug_info);
    }
    pub fn compact_level(&mut self, next_store_id: &mut StoreId) -> Vec<TableChange> {
        self.current.compact_storage(next_store_id)
    }

    /// Moves the current active memtable to the immutable list and creates a new active memtable.
    /// Modifies the LsmStorage instance in place.
    pub fn freeze_memtable(&mut self) {
        // Create a new active memtable based on the capacity of the old one.
        // Note: Accessing capacity might require changes if Memtable API changes.
        // Assuming Memtable::new takes capacity. If not, adjust based on Config.
        let new_m = Arc::new(Memtable::new(self.m.get_capacity_bytes()));

        // Replace the current active memtable with the new one, getting the old one back.
        let old_m = std::mem::replace(&mut self.m, new_m);

        // Add the old memtable to the immutable list.
        self.imm.push(old_m);
        info!("freeze_memtable");

        // No need to return Self, modification happens in place.
    }

    /// Dumps the oldest immutable memtable (if any) to a new SSTable in level 0.
    ///
    /// # Arguments
    /// * `next_sstable_id` - A mutable reference to the next available store ID, which will be incremented.
    pub fn dump_imm_memtable(&mut self, next_sstable_id: &mut StoreId) {
        // Check if there are any immutable memtables to dump
        if self.imm.is_empty() {
            return;
        }

        // take first immtable out (oldest one)
        let oldest_imm = self.imm.remove(0); // Remove from the front

        // create kv iterator from the memtable
        // Assuming Memtable has an `iter()` method that returns an iterator yielding KVOpertion
        // The iterator needs to own or reference the data appropriately.
        // Let's assume `oldest_imm.iter()` returns `impl Iterator<Item = KVOpertion>`.
        let kv_iterator = oldest_imm.to_iter();

        // call level.push_new_table to dump memtable
        self.current.push_new_table(kv_iterator, next_sstable_id);
    }

    pub fn immtable_num(&self) -> usize {
        self.imm.len()
    }
    pub fn memtable_size(&self) -> usize {
        self.m.get_size()
    }

    pub fn put(&self, query: KVOpertion) {
        // Insert the operation into the active memtable.
        // The memtable's insert method handles the key-op_id pair insertion.
        self.m.insert(query).expect("Memtable insert failed"); // Call insert on Memtable
    }

    pub fn get(&self, query: &KeyQuery) -> Option<KVOpertion> {
        // 1. Check active memtable
        if let Some((op_type, op_id)) = self.m.get(query) {
            match op_type {
                OpType::Write(value) => {
                    return Some(KVOpertion {
                        id: op_id,
                        key: query.key.clone(), // Reuse the key from the query
                        op: OpType::Write(value),
                    });
                }
                OpType::Delete => return None, // Found a delete marker, treat as not found
            }
        }

        // 2. Check immutable memtables in reverse order (newest first)
        for imm_table in self.imm.iter().rev() {
            if let Some((op_type, op_id)) = imm_table.get(query) {
                match op_type {
                    OpType::Write(value) => {
                        return Some(KVOpertion {
                            id: op_id,
                            key: query.key.clone(),
                            op: OpType::Write(value),
                        });
                    }
                    OpType::Delete => return None,
                }
            }
        }

        // 3. Check levels (SSTables)
        // Convert Key<Bytes> to KeySlice (&Key<&[u8]>) before calling find
        let key_slice: KeySlice = query.key.as_ref().into();
        if let Some((op_type, op_id)) = self.current.find(&key_slice, query.op_id) {
            // LevelStorege::find returns SearchResult (Option<(OpType, OpId)>)
            // We need to reconstruct KVOpertion if it's a Write
            match op_type {
                OpType::Write(value) => {
                    return Some(KVOpertion {
                        id: op_id,
                        key: query.key.clone(), // Reuse the key from the query
                        op: OpType::Write(value),
                    });
                }
                OpType::Delete => return None, // Found delete in levels
            }
        }

        // 4. Key not found in any component
        None
    }
    // return key and value in [start end)
    pub fn get_range(&self, start: KeyQuery, end: KeyQuery) -> Option<KVOpertion> {
        unimplemented!()
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use super::{LsmStorage, LsmStorageConfig};
    use crate::db::common::{KVOpertion, KeyQuery, OpType};
    use crate::db::key::{KeyBytes, KeySlice, KeyVec};
    use crate::db::level::LevelStoregeConfig;
    use crate::db::store::Memstore;

    /// Tests the basic put and get functionality of the LsmStorage.
    /// It inserts a key-value pair and then retrieves it to verify correctness.
    #[test]
    fn test_put() {
        let config = LsmStorageConfig::config_for_test();
        let lsm = LsmStorage::<Memstore>::new(config);

        let key: KeyVec = "test_key".as_bytes().into();
        let value: KeyVec = "test_value".as_bytes().into();
        let op_id = 1;

        // Use KeyBytes for OpType::Write
        let op = KVOpertion::new(
            op_id,
            key.clone(),
            OpType::Write(KeyBytes::from(value.as_ref())),
        );
        lsm.put(op);

        // Use KeyBytes for KeyQuery
        let query = KeyQuery {
            op_id,
            key: KeyBytes::from(key.as_ref()),
        };
        let result = lsm.get(&query);

        assert!(result.is_some());
        let retrieved_op = result.unwrap();
        // Compare KeyBytes with KeyBytes
        assert_eq!(retrieved_op.key, KeyBytes::from(key.as_ref()));
        assert_eq!(retrieved_op.id, op_id);
        match retrieved_op.op {
            // Compare KeyBytes with KeyBytes
            OpType::Write(v) => assert_eq!(v, KeyBytes::from(value.as_ref())),
            OpType::Delete => panic!("Expected Write, got Delete"),
        }
    }

    /// Tests retrieving data that exists only in the LevelStorege (SSTables).
    /// It constructs an LsmStorage with pre-populated levels and verifies
    /// that `get` can find keys in different levels and handles non-existent keys correctly.
    #[test]
    fn test_get_from_level() {
        let config = LsmStorageConfig::config_for_test();

        // Create a dummy table for Level 0
        let table_lvl0 = crate::db::table::test::create_test_table_with_id_offset(0..100, 1000); // keys "000000" to "000099", OpIds 1000-1099
        let level0 = crate::db::level::Level::new(vec![Arc::new(table_lvl0)], true);

        // Create a dummy table for Level 1
        let table_lvl1 = crate::db::table::test::create_test_table_with_id_offset(100..200, 2000); // keys "000100" to "000199", OpIds 2100-2199
        let level1 = crate::db::level::Level::new(vec![Arc::new(table_lvl1)], false);

        // Create LevelStorege with these levels
        let level_storage =
            crate::db::level::LevelStorege::new(vec![level0, level1], config.level_config);

        // Create LsmStorage from the configured levels
        let lsm = LsmStorage::<Memstore>::from(config, level_storage);

        // Test 1: Get a key that exists in Level 0
        let key1: KeyVec = "000050".as_bytes().into();
        let query1 = KeyQuery {
            op_id: 1050, // OpId within the range of table_lvl0
            key: KeyBytes::from(key1.as_ref()),
        };
        let result1 = lsm.get(&query1);
        assert!(result1.is_some());
        assert_eq!(
            result1.unwrap().op,
            OpType::Write(KeyBytes::from("50".as_bytes()))
        );

        // Test 2: Get a key that exists in Level 1
        let key2: KeyVec = "000150".as_bytes().into();
        let query2 = KeyQuery {
            op_id: 2150, // OpId within the range of table_lvl1
            key: KeyBytes::from(key2.as_ref()),
        };
        let result2 = lsm.get(&query2);
        assert!(result2.is_some());
        assert_eq!(
            result2.unwrap().op,
            OpType::Write(KeyBytes::from("150".as_bytes()))
        );

        // Test 3: Get a key that does not exist
        let key3: KeyVec = "000250".as_bytes().into();
        let query3 = KeyQuery {
            op_id: 3000,
            key: KeyBytes::from(key3.as_ref()),
        };
        let result3 = lsm.get(&query3);
        assert!(result3.is_none());

        // Test 4: Key exists in Level 0, but query op_id is too old
        let key4: KeyVec = "000010".as_bytes().into();
        let query4 = KeyQuery {
            op_id: 500, // OpId older than any in table_lvl0
            key: KeyBytes::from(key4.as_ref()),
        };
        let result4 = lsm.get(&query4);
        assert!(result4.is_none());
    }

    /// Tests retrieving data from immutable memtables.
    /// It covers scenarios where the key exists only in an immutable memtable,
    /// where a key exists in both active and immutable memtables (active should win),
    /// and where a key was deleted in the active memtable after existing in an immutable one.
    #[test]
    fn test_get_from_imm_memtable() {
        let config = LsmStorageConfig::config_for_test();
        let mut lsm = LsmStorage::<Memstore>::new(config);

        // Insert data into active memtable
        let key1: KeyVec = "imm_key1".as_bytes().into();
        let value1: KeyVec = "imm_value1".as_bytes().into();
        let op1_id = 10;
        lsm.put(KVOpertion::new(
            op1_id,
            key1.clone(),
            OpType::Write(KeyBytes::from(value1.as_ref())),
        ));

        // Freeze memtable, moving key1 to immutable list
        lsm.freeze_memtable();

        // Insert new data into the new active memtable
        let key2: KeyVec = "imm_key2".as_bytes().into();
        let value2: KeyVec = "imm_value2".as_bytes().into();
        let op2_id = 20;
        lsm.put(KVOpertion::new(
            op2_id,
            key2.clone(),
            OpType::Write(KeyBytes::from(value2.as_ref())),
        ));

        // Test 1: Get key from immutable memtable (key1)
        let query1 = KeyQuery {
            op_id: op1_id,
            key: KeyBytes::from(key1.as_ref()),
        };
        let result1 = lsm.get(&query1);
        assert!(result1.is_some());
        assert_eq!(
            result1.unwrap().op,
            OpType::Write(KeyBytes::from(value1.as_ref()))
        );

        // Test 2: Get key from active memtable (key2)
        let query2 = KeyQuery {
            op_id: op2_id,
            key: KeyBytes::from(key2.as_ref()),
        };
        let result2 = lsm.get(&query2);
        assert!(result2.is_some());
        assert_eq!(
            result2.unwrap().op,
            OpType::Write(KeyBytes::from(value2.as_ref()))
        );

        // Test 3: Overwrite a key in active memtable that exists in immutable
        let key_overwrite: KeyVec = "imm_key_overwrite".as_bytes().into();
        let value_original: KeyVec = "original_value".as_bytes().into();
        let value_new: KeyVec = "new_value".as_bytes().into();
        let op_id_original = 30;
        let op_id_new = 31;

        lsm.put(KVOpertion::new(
            op_id_original,
            key_overwrite.clone(),
            OpType::Write(KeyBytes::from(value_original.as_ref())),
        ));
        lsm.freeze_memtable(); // Move original to immutable

        lsm.put(KVOpertion::new(
            op_id_new,
            key_overwrite.clone(),
            OpType::Write(KeyBytes::from(value_new.as_ref())),
        )); // New value in active

        let query_latest = KeyQuery {
            op_id: op_id_new,
            key: KeyBytes::from(key_overwrite.as_ref()),
        };
        let result_latest = lsm.get(&query_latest);
        assert!(result_latest.is_some());
        assert_eq!(
            result_latest.unwrap().op,
            OpType::Write(KeyBytes::from(value_new.as_ref()))
        );

        let query_old = KeyQuery {
            op_id: op_id_original,
            key: KeyBytes::from(key_overwrite.as_ref()),
        };
        let result_old = lsm.get(&query_old);
        assert!(result_old.is_some());
        assert_eq!(
            result_old.unwrap().op,
            OpType::Write(KeyBytes::from(value_original.as_ref()))
        );

        // Test 4: Delete a key in active memtable that exists in immutable
        let key_delete: KeyVec = "imm_key_delete".as_bytes().into();
        let value_delete: KeyVec = "value_to_delete".as_bytes().into();
        let op_id_initial_delete = 40;
        let op_id_actual_delete = 41;

        lsm.put(KVOpertion::new(
            op_id_initial_delete,
            key_delete.clone(),
            OpType::Write(KeyBytes::from(value_delete.as_ref())),
        ));
        lsm.freeze_memtable(); // Move original to immutable

        lsm.put(KVOpertion::new(
            op_id_actual_delete,
            key_delete.clone(),
            OpType::Delete,
        )); // Delete in active

        let query_after_delete = KeyQuery {
            op_id: op_id_actual_delete,
            key: KeyBytes::from(key_delete.as_ref()),
        };
        let result_after_delete = lsm.get(&query_after_delete);
        assert!(result_after_delete.is_none()); // Should be None due to delete marker

        let query_before_delete = KeyQuery {
            op_id: op_id_initial_delete,
            key: KeyBytes::from(key_delete.as_ref()),
        };
        let result_before_delete = lsm.get(&query_before_delete);
        assert!(result_before_delete.is_some());
        assert_eq!(
            result_before_delete.unwrap().op,
            OpType::Write(KeyBytes::from(value_delete.as_ref()))
        );
    }

    /// Tests retrieving data specifically from the active memtable.
    /// It verifies that keys inserted into the active memtable can be retrieved,
    /// that non-existent keys return None, and that delete markers in the active
    /// memtable correctly hide previous values for the same key within that memtable.
    #[test]
    fn test_get_from_memtable() {
        let config = LsmStorageConfig::config_for_test();
        let lsm = LsmStorage::<Memstore>::new(config);

        let key1: KeyVec = "key_active_1".as_bytes().into();
        let value1: KeyVec = "value_active_1".as_bytes().into();
        let op1_id = 1;

        lsm.put(KVOpertion::new(
            op1_id,
            key1.clone(),
            OpType::Write(KeyBytes::from(value1.as_ref())),
        ));

        // Test: Get existing key from active memtable
        let query1 = KeyQuery {
            op_id: op1_id,
            key: KeyBytes::from(key1.as_ref()),
        };
        let result1 = lsm.get(&query1);
        assert!(result1.is_some());
        assert_eq!(
            result1.unwrap().op,
            OpType::Write(KeyBytes::from(value1.as_ref()))
        );

        // Test: Get non-existent key
        let key_non_existent: KeyVec = "non_existent".as_bytes().into();
        let query_non_existent = KeyQuery {
            op_id: 2,
            key: KeyBytes::from(key_non_existent.as_ref()),
        };
        let result_non_existent = lsm.get(&query_non_existent);
        assert!(result_non_existent.is_none());

        // Test: Get a key that was deleted in the active memtable
        let key_deleted: KeyVec = "key_deleted".as_bytes().into();
        let value_original: KeyVec = "original_value".as_bytes().into();
        let op_id_original = 3;
        lsm.put(KVOpertion::new(
            op_id_original,
            key_deleted.clone(),
            OpType::Write(KeyBytes::from(value_original.as_ref())),
        ));

        let op_id_delete = 4;
        lsm.put(KVOpertion::new(
            op_id_delete,
            key_deleted.clone(),
            OpType::Delete,
        ));

        // Query at or after delete op_id should return None
        let query_after_delete = KeyQuery {
            op_id: op_id_delete,
            key: KeyBytes::from(key_deleted.as_ref()),
        };
        let result_after_delete = lsm.get(&query_after_delete);
        assert!(result_after_delete.is_none());

        // Query before delete op_id should return the original value
        let query_before_delete = KeyQuery {
            op_id: op_id_original,
            key: KeyBytes::from(key_deleted.as_ref()),
        };
        let result_before_delete = lsm.get(&query_before_delete);
        assert!(result_before_delete.is_some());
        assert_eq!(
            result_before_delete.unwrap().op,
            OpType::Write(KeyBytes::from(value_original.as_ref()))
        );
    }

    #[test]
    fn test_dump_imm_memtable() {
        let config = LsmStorageConfig::config_for_test();
        let mut lsm = LsmStorage::<Memstore>::new(config);
        let mut next_sstable_id: u64 = 100;

        // 1. Insert data into active memtable
        let key1: KeyVec = "dump_key1".as_bytes().into();
        let value1: KeyVec = "dump_value1".as_bytes().into();
        let op1_id = 50;
        lsm.put(KVOpertion::new(
            op1_id,
            key1.clone(),
            OpType::Write(KeyBytes::from(value1.as_ref())),
        ));

        let key2: KeyVec = "dump_key2".as_bytes().into();
        let value2: KeyVec = "dump_value2".as_bytes().into();
        let op2_id = 51;
        lsm.put(KVOpertion::new(
            op2_id,
            key2.clone(),
            OpType::Write(KeyBytes::from(value2.as_ref())),
        ));

        // 2. Freeze the memtable
        lsm.freeze_memtable();
        assert_eq!(lsm.immtable_num(), 1); // Should have one immutable memtable
        assert_eq!(lsm.table_num_in_levels(), vec![] as Vec<usize>); // No tables in levels yet

        // 3. Dump the immutable memtable
        lsm.dump_imm_memtable(&mut next_sstable_id);

        // 4. Verify state after dump
        assert_eq!(lsm.immtable_num(), 0); // Immutable list should be empty
        assert_eq!(next_sstable_id, 101); // next_sstable_id should be incremented

        // Check that level 0 now has one table
        let level_counts = lsm.table_num_in_levels();
        assert_eq!(level_counts.len(), 1);
        assert_eq!(level_counts[0], 1); // One table in level 0

        // Verify the dumped data can be retrieved (it's now in level 0)
        let query1 = KeyQuery {
            op_id: op1_id,
            key: KeyBytes::from(key1.as_ref()),
        };
        let result1 = lsm.get(&query1);
        assert!(result1.is_some());
        assert_eq!(
            result1.unwrap().op,
            OpType::Write(KeyBytes::from(value1.as_ref()))
        );

        let query2 = KeyQuery {
            op_id: op2_id,
            key: KeyBytes::from(key2.as_ref()),
        };
        let result2 = lsm.get(&query2);
        assert!(result2.is_some());
        assert_eq!(
            result2.unwrap().op,
            OpType::Write(KeyBytes::from(value2.as_ref()))
        );

        // 5. Dump again when no immutable tables exist (should do nothing)
        let initial_level_counts = lsm.table_num_in_levels();
        lsm.dump_imm_memtable(&mut next_sstable_id);
        assert_eq!(lsm.immtable_num(), 0);
        assert_eq!(next_sstable_id, 101); // ID should not change
        assert_eq!(lsm.table_num_in_levels(), initial_level_counts); // Levels should not change
    }
}
