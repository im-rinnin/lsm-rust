use std::{sync::Arc, usize};

use super::{
    common::{KVOpertion, OpId, OpType, SearchResult},
    db_meta::{DBMeta, ThreadDbMeta},
    key::{KeySlice, KeyVec},
    store::{Store, StoreId},
    table::*,
};

pub struct SStablePositon {
    level: usize,
    // table position from level left(table contains mini key in level)
    index: usize,
}
struct LevelChange {
    delete_tables: Vec<SStablePositon>,
    insert_tables: Vec<(SStablePositon, StoreId)>,
}

pub struct Level<T: Store> {
    // sstable sorted  by (key,op id)
    sstables: Vec<Arc<TableReader<T>>>,
    is_level_zero: bool,
}
impl<T: Store> Level<T> {
    pub fn new(sstables: Vec<Arc<TableReader<T>>>, is_level_zero: bool) -> Self {
        Level {
            sstables,
            is_level_zero,
        }
    }

    pub fn find(&self, key: &KeySlice, opid: OpId) -> SearchResult {
        let mut best_result: SearchResult = None;

        for table in self.sstables.iter().rev() {
            let (first_key, last_key) = table.key_range();
            // Optimization: check if key is within table's range
            if key.as_ref().lt(first_key.as_ref()) {
                continue; // Key is too small for this table
            }
            if !self.is_level_zero && key.as_ref().gt(last_key.as_ref()) {
                // For level 0, tables can overlap and are ordered by creation time, newest first.
                // For other levels, tables are non-overlapping.
                // If key is greater than last key of this table, and it's level 0,
                // we should continue searching in older tables.
                // If it's level N (N>0), we can potentially break early if we assume sorted tables by range.
                break;
            }

            if let Some((op_type, id)) = table.find(key, opid) {
                if self.is_level_zero {
                    // For level 0, we need to find the one with the maximum opid among all matches
                    // because tables can overlap.
                    if best_result.is_none() || id > best_result.as_ref().unwrap().1 {
                        best_result = Some((op_type, id));
                    }
                } else {
                    // For Level N (N>0), tables are non-overlapping and don't contain duplicate keys.
                    // So the first match is the only and correct one.
                    return Some((op_type, id));
                }
            }
        }
        best_result
    }
}

pub struct LevelStorege<T: Store> {
    levels: Vec<Level<T>>,
    //level table increase ratio betweent two level.
    //level n+1 table len =level n table len *ratio
    level_ratio: usize,
}

impl<T: Store> LevelStorege<T> {
    pub fn new(tables: Vec<Level<T>>, r: usize) -> Self {
        LevelStorege {
            levels: tables,
            level_ratio: r,
        }
    }
    pub fn find(&self, key: &KeySlice, opid: OpId) -> SearchResult {
        // Search from level 0 upwards (newest data to oldest)
        for level in self.levels.iter() {
            if let Some(result) = level.find(key, opid) {
                return Some(result);
            }
        }
        None
    }
    // compact table to next level tables which key range overlay
    // table=self.sstables[index]
    pub fn compact(&self, index: usize, meta: ThreadDbMeta<T>) -> Vec<TableReader<T>> {
        unimplemented!()
    }
    // return level index and table index which need to be compacted
    fn check_level_size(&self) -> Option<(usize, usize)> {
        unimplemented!()
    }
}

#[cfg(test)]
mod test {
    use std::ops::Range;
    use std::sync::Arc; // Moved from test functions

    use crate::db::common::{KVOpertion, OpId, OpType}; // OpType moved from helper
    use crate::db::key::{KeySlice, KeyVec}; use crate::db::store::Store;
    // KeyVec moved from helper
    use crate::db::table::test::create_test_table;
    use crate::db::table::TableBuilder; // Moved from helper

    use crate::db::{store::Memstore, table::TableReader};

    // Helper function to reduce code duplication in tests for Level struct
    fn assert_level_find_and_check(
        level: &super::Level<Memstore>,
        key: &KeySlice,
        expected_value: Option<&[u8]>,
    ) {
        let result = level.find(key, u64::MAX); // Use u64::MAX for opid to find latest

        match (result, expected_value) {
            (Some((OpType::Write(val), _)), Some(expected)) => {
                assert_eq!(
                    val.as_ref(),
                    expected,
                    "Key: {:?} found value mismatch",
                    key
                );
            }
            (Some((OpType::Delete, _)), Some(_)) => {
                panic!("Expected write for key {:?}, but found delete", key);
            }
            (Some(_), None) => {
                // If it's Some((OpType::Write(_), _)) or Some((OpType::Delete, _))
                // and expected_value is None, it means we found something but expected nothing.
                panic!("Expected no result for key {:?}, but found one", key);
            }
            (None, Some(expected)) => {
                panic!(
                    "Expected result {:?} for key {:?}, but found none",
                    String::from_utf8_lossy(expected),
                    key
                );
            }
            (None, None) => {
                // This is the expected case, do nothing
            } // The following two arms are redundant due to the (Some(_), None) arm
              // (Some((OpType::Write(_), _)), None) => {
              //     panic!("Expected no result for key {:?}, but found write", key);
              // }
              // (Some((OpType::Delete, _)), None) => {
              //     panic!("Expected no result for key {:?}, but found delete", key);
              // }
        }
    }

    // Helper function to reduce code duplication in tests for LevelStorege struct
    fn assert_level_storage_find_and_check(
        level_storage: &super::LevelStorege<Memstore>,
        key: &KeySlice,
        expected_value: Option<&[u8]>,
        opid: OpId,
    ) {
        let result = level_storage.find(key, opid);

        match (result, expected_value) {
            (Some((OpType::Write(val), _)), Some(expected)) => {
                assert_eq!(
                    val.as_ref(),
                    expected,
                    "Key: {:?} found value mismatch",
                    key
                );
            }
            (Some((OpType::Delete, _)), Some(_)) => {
                panic!("Expected write for key {:?}, but found delete", key);
            }
            (Some(_), None) => {
                // If it's Some((OpType::Write(_), _)) or Some((OpType::Delete, _))
                // and expected_value is None, it means we found something but expected nothing.
                panic!("Expected no result for key {:?}, but found one", key);
            }
            (None, Some(expected)) => {
                panic!(
                    "Expected result {:?} for key {:?}, but found none",
                    String::from_utf8_lossy(expected),
                    key
                );
            }
            (None, None) => {
                // This is the expected case, do nothing
            }
        }
    }

    // New helper to create test table with custom value transform
    fn create_test_table_with_value_transform(
        range: Range<usize>,
        id_offset: OpId,
        value_transform: impl Fn(usize) -> String,
    ) -> TableReader<Memstore> {
        let mut v = Vec::new();
        for i in range.clone() {
            let tmp = KVOpertion::new(
                i as u64 + id_offset,
                crate::db::block::test::pad_zero(i as u64).as_bytes().into(),
                OpType::Write(value_transform(i).as_bytes().into()),
            );
            v.push(tmp);
        }
        let id = format!("test_id_{}_{}", range.start, range.end); // Unique ID for Memstore
        let mut store = Memstore::create(&id);
        let mut table = TableBuilder::new_with_store(store);
        table.fill_with_op(v.iter()); // Use iter()
        let res = table.flush();
        res
    }

    #[test]
    fn test_level_find_in_level_zero() {
        // create table a: range [0,100), value is key. (older table)
        let table_a = Arc::new(create_test_table(0..100));
        // create table b: range [50,150), value is key + 1. (newer table)
        let table_b = Arc::new(create_test_table_with_value_transform(50..150, 100, |i| {
            (i + 1).to_string()
        }));

        // create level from tables. For level zero, tables are ordered from newest to oldest
        // by the find logic, so table_b should be first in the vector.
        let level = super::Level::new(vec![table_b.clone(), table_a.clone()], true);

        // find key 20 (only in table_a)
        let key_20 = KeySlice::from("000020".as_bytes());
        assert_level_find_and_check(&level, &key_20, Some("20".as_bytes()));

        // find key 50 (in both table_a and table_b, newest (table_b) should win)
        let key_50 = KeySlice::from("000050".as_bytes());
        // let key_51 = KeySlice::from("000051".as_bytes()); // This variable is not used
        assert_level_find_and_check(&level, &key_50, Some("51".as_bytes())); // value from table_b (50+1)

        // find key 80 (in both table_a and table_b, newest (table_b) should win)
        let key_80 = KeySlice::from("000080".as_bytes());
        assert_level_find_and_check(&level, &key_80, Some("81".as_bytes())); // value from table_b (80+1)

        // find key 120 (only in table_b)
        let key_120 = KeySlice::from("000120".as_bytes());
        assert_level_find_and_check(&level, &key_120, Some("121".as_bytes())); // value from table_b (120+1)

        // find key 200 (not in any table)
        let key_200 = KeySlice::from("000200".as_bytes());
        assert_level_find_and_check(&level, &key_200, None);

        // find key 0 (only in table_a)
        let key_0 = KeySlice::from("000000".as_bytes());
        assert_level_find_and_check(&level, &key_0, Some("0".as_bytes()));
    }
    #[test]
    fn test_level_find() {
        // create table a, b, c (non-overlapping ranges, mimicking Level N behavior)
        // Table A: keys "000000" to "000099"
        let table_a = Arc::new(create_test_table(0..100));
        // Table B: keys "000100" to "000199"
        let table_b = Arc::new(create_test_table(100..200));
        // Table C: keys "000200" to "000299"
        let table_c = Arc::new(create_test_table(200..300));

        // create level from tables (is_level_zeor: false for non-overlapping behavior)
        let level = super::Level::new(
            vec![table_a.clone(), table_b.clone(), table_c.clone()],
            false,
        );

        // test find kv in table a
        let key_a = KeySlice::from("000050".as_bytes());
        assert_level_find_and_check(&level, &key_a, Some("50".as_bytes()));

        // test find kv in table b
        let key_b = KeySlice::from("000150".as_bytes());
        assert_level_find_and_check(&level, &key_b, Some("150".as_bytes()));

        // test find kv in table c
        let key_c = KeySlice::from("000250".as_bytes());
        assert_level_find_and_check(&level, &key_c, Some("250".as_bytes()));

        // test find key at boundary (e.g., first key of table B)
        let key_boundary = KeySlice::from("000100".as_bytes());
        assert_level_find_and_check(&level, &key_boundary, Some("100".as_bytes()));

        // test find key > table c max key ("000299")
        let key_gt_c = KeySlice::from("000300".as_bytes());
        assert_level_find_and_check(&level, &key_gt_c, None);

        // test find key < table a min key ("000000")
        let key_lt_a = KeySlice::from("0".as_bytes()); // Lexicographically smaller than "000000"
        assert_level_find_and_check(&level, &key_lt_a, None);
    }
    #[test]
    fn test_search_in_level_storage() {
        // Create tables for Level 0 (overlapping, newest data wins)
        // Table A: keys "000000" to "000099", values are key_str (older)
        let table_a_lvl0 = Arc::new(create_test_table(0..100));
        // Table B: keys "000050" to "000149", values are key_str + 1 (newer for overlaps)
        let table_b_lvl0 = Arc::new(create_test_table_with_value_transform(
            50..150,
            1000, // Higher opid for newer data
            |i| (i + 1).to_string(),
        ));
        // Level 0: tables ordered newest to oldest to match find logic
        let level0 = super::Level::new(vec![table_b_lvl0.clone(), table_a_lvl0.clone()], true);

        // Create tables for Level 1 (non-overlapping)
        // Table C: keys "000200" to "000299", values are key_str (e.g., 200)
        let table_c_lvl1 = Arc::new(create_test_table(200..300));
        // Table D: keys "000300" to "000399", values are key_str (e.g., 300)
        let table_d_lvl1 = Arc::new(create_test_table(300..400));
        // Level 1: tables are non-overlapping, order does not strictly matter for correctness
        // but typically sorted by key range for efficiency.
        let level1 = super::Level::new(vec![table_c_lvl1.clone(), table_d_lvl1.clone()], false);

        // Create LevelStorege with levels
        let level_storage = super::LevelStorege::new(vec![level0, level1], 2); // r is level_ratio, not relevant for this test

        // Test cases for find:

        // 1. Find key only in level 0 (e.g., "000020" from table A)
        let key_lvl0_only = KeySlice::from("000020".as_bytes());
        assert_level_storage_find_and_check(
            &level_storage,
            &key_lvl0_only,
            Some("20".as_bytes()),
            u64::MAX,
        );

        // 2. Find key in both level 0 and level 1, but level 0 should win (e.g., "000050")
        // table_a_lvl0 has "000050" -> "50"
        // table_b_lvl0 has "000050" -> "51" (higher opid)
        let key_overlap = KeySlice::from("000050".as_bytes());
        assert_level_storage_find_and_check(
            &level_storage,
            &key_overlap,
            Some("51".as_bytes()),
            u64::MAX,
        );

        // 3. Find key only in level 1 (e.g., "000250" from table C)
        let key_lvl1_only = KeySlice::from("000250".as_bytes());
        assert_level_storage_find_and_check(
            &level_storage,
            &key_lvl1_only,
            Some("250".as_bytes()),
            u64::MAX,
        );

        // 4. Find key not found in any level (e.g., "000500")
        let key_not_found = KeySlice::from("000500".as_bytes());
        assert_level_storage_find_and_check(&level_storage, &key_not_found, None, u64::MAX);

        // 5. Find key in level 0 with opid constraint (old value of "000050" from table_a_lvl0)
        let key_overlap_old_opid = KeySlice::from("000050".as_bytes());
        // We want to find the value before table_b_lvl0 was created (opid 1000)
        // If we search with opid 999, we should get the value from table_a_lvl0 (opid 50)
        assert_level_storage_find_and_check(
            &level_storage,
            &key_overlap_old_opid,
            Some("50".as_bytes()),
            999,
        );
    }

    #[test]
    fn test_compact() {}
}
