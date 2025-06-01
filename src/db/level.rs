use std::{sync::Arc, usize};

use bincode::Options;

use crate::db::common::KViterAgg;

use super::{
    common::{KVOpertion, OpId, OpType, SearchResult},
    db_meta::{DBMeta, ThreadDbMeta},
    key::{KeySlice, KeyVec},
    lsm_storage::LsmStorage,
    store::{Store, StoreId},
    table::{self, *},
};

const MAX_LEVEL_ZERO_TABLE_SIZE: usize = 4;
#[derive(Debug, PartialEq, Eq)]
pub enum ChangeType {
    Add,
    Delete,
}

#[derive(Debug)]
pub struct TableChange {
    level: usize,
    index: usize,
    id: String,
    change_type: ChangeType,
}

pub struct Level<T: Store> {
    // sstable sorted  by (key,op id)
    sstables: Vec<ThreadSafeTableReader<T>>,
    is_level_zero: bool,
}

impl<T: Store> Clone for Level<T> {
    fn clone(&self) -> Self {
        Level {
            sstables: self.sstables.clone(), // This clones the Arc pointers, not the underlying data
            is_level_zero: self.is_level_zero,
        }
    }
}
impl<T: Store> Level<T> {
    pub fn new(sstables: Vec<ThreadSafeTableReader<T>>, is_level_zero: bool) -> Self {
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
    level_zero_num_limit: usize,
    level_ratio: usize,
}

impl<T: Store> LevelStorege<T> {
    pub fn new(tables: Vec<Level<T>>, r: usize) -> Self {
        LevelStorege {
            levels: tables,
            level_zero_num_limit: MAX_LEVEL_ZERO_TABLE_SIZE, // This should ideally come from configuration
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
    pub fn compact(&self, index: usize) -> Vec<TableReader<T>> {
        unimplemented!()
    }
    fn max_table_in_level(ratio: usize, level_depth: usize) -> usize {
        if level_depth == 0 {
            return MAX_LEVEL_ZERO_TABLE_SIZE;
        }
        return MAX_LEVEL_ZERO_TABLE_SIZE.pow(ratio as u32);
    }
    fn compact_storage(&mut self, mut next_store_id: u64) ->Vec<TableChange>{
            let mut table_change = Vec::new();
        // Iterate through all levels starting from level 0
        for level_depth in 0..self.levels.len() {
            // Check if this level needs compaction
            let max_tables = Self::max_table_in_level(self.level_ratio, level_depth);
            if self.levels[level_depth].sstables.len() <= max_tables {
                continue; // No compaction needed for this level
            }


            // Take out tables that exceed the limit
            let (tables_to_compact, table_changes_from_take_out) =
                self.take_out_table_to_compact(level_depth);
            if tables_to_compact.is_empty() {
                continue; // Nothing to compact
            }
            table_change.extend(table_changes_from_take_out);

            // Ensure we have a target level (level_depth + 1)
            let target_level_depth = level_depth + 1;
            if self.levels.len() <= target_level_depth {
                // Create new empty level if it doesn't exist
                self.levels.push(Level::new(vec![], false));
            }

            // Perform compaction to the next level
            let (new_next_store_id, table_changes_from_compact_level) =
                self.compact_level(next_store_id, tables_to_compact, level_depth);

            // Update the next store ID for subsequent operations
            next_store_id = new_next_store_id;
            table_change.extend(table_changes_from_compact_level);


            // After compacting one level, we might need to check if the target level
            // now also needs compaction, but we'll handle that in the next iteration
        }
        return table_change;
    }

    /// Gets the overall key range (min and max keys) across a collection of tables.
    ///
    /// This function examines all provided tables and returns the smallest minimum key
    /// and the largest maximum key across all tables. This is useful for determining
    /// the key range that will be affected during compaction operations.
    ///
    /// # Arguments
    /// * `tables` - A slice of table readers to examine (must not be empty)
    ///
    /// # Returns
    /// A tuple containing (min_key, max_key) representing the overall range
    ///
    /// # Panics
    /// Panics if the tables slice is empty
    fn get_key_range(&self, tables: &[ThreadSafeTableReader<T>]) -> (KeyVec, KeyVec) {
        assert!(!tables.is_empty());
        let (mut min_key, mut max_key) = tables[0].key_range();
        for table in tables.iter().skip(1) {
            let (current_min, current_max) = table.key_range();
            if current_min.as_ref().lt(min_key.as_ref()) {
                min_key = current_min;
            }
            if current_max.as_ref().gt(max_key.as_ref()) {
                max_key = current_max;
            }
        }
        (min_key, max_key)
    }

    /// Compacts tables from one level to the next level.
    ///
    /// This function takes input tables from `level_depth` and merges them with
    /// overlapping tables from `level_depth + 1`. The merged data is written to
    /// new SSTable files in the target level, and the original tables are removed.
    ///
    /// # Arguments
    /// * `store_id_start` - Starting store ID for new SSTable files
    /// * `input_tables` - Tables to be compacted from the source level
    /// * `level_depth` - Source level index (target will be level_depth + 1)
    ///
    /// # Returns
    /// The next available store ID after creating new SSTable files
    fn compact_level(
        &mut self,
        store_id_start: u64,
        input_tables: Vec<ThreadSafeTableReader<T>>,
        level_depth: usize,
    ) -> (u64, Vec<TableChange>) {
        assert!(!input_tables.is_empty());

        // 1. Get key range in input_tables
        let (min_key, max_key) = self.get_key_range(&input_tables);

        let target_level_depth = level_depth + 1;
        let mut tables_to_compact: Vec<ThreadSafeTableReader<T>> = input_tables;
        let mut non_overlapping_tables: Vec<ThreadSafeTableReader<T>> = Vec::new();
        let mut table_change: Vec<TableChange> = Vec::new();

        let check_res = self.table_range_overlap(
            &min_key.as_ref().into(),
            &max_key.as_ref().into(),
            target_level_depth,
        );

        let mut next_sstable_id_counter = store_id_start as u64;
        // 2. Find tables which key range overlap with key range in target level
        if let Some(target_level) = self.levels.get_mut(target_level_depth) {
            if let Some((first_overlap_idx, last_overlap_idx)) = check_res {
                // Collect overlapping tables and remove them from target level
                let removed_tables: Vec<ThreadSafeTableReader<T>> = target_level
                    .sstables
                    .drain(first_overlap_idx..=last_overlap_idx)
                    .collect();

                // Record removed tables in table_change
                for (i, table) in removed_tables.iter().enumerate() {
                    table_change.push(TableChange {
                        level: target_level_depth,
                        index: first_overlap_idx + i,
                        id: table.store_id().parse().unwrap(),
                        change_type: ChangeType::Delete,
                    });
                }

                tables_to_compact.extend(removed_tables);
            }

            // 3. Change these table to table iter, pass them to KvIterAgg, build compacted table from it , get output tables
            // Collect the ThreadSafeTableReader instances into a Vec first.
            // This ensures they are owned and persist while their iterators are used.
            let owned_tables: Vec<ThreadSafeTableReader<T>> =
                tables_to_compact.into_iter().collect();

            let mut table_iters: Vec<TableIter<T>> = owned_tables
                .iter() // Iterate over references to the owned tables
                .map(|table_reader| table_reader.to_iter())
                .collect();

            let iter_refs: Vec<&mut dyn Iterator<Item = KVOpertion>> = table_iters
                .iter_mut()
                .map(|iter| iter as &mut dyn Iterator<Item = KVOpertion>)
                .collect();
            let mut kv_iter_agg = KViterAgg::new(iter_refs);
            let mut compacted_output_tables: Vec<ThreadSafeTableReader<T>> = Vec::new();

            let mut store_id_generator = || {
                let id = next_sstable_id_counter.to_string();
                next_sstable_id_counter += 1;
                id
            };

            let mut current_table_builder = {
                let new_store_id = store_id_generator();
                let new_store = T::create(&new_store_id);
                // Using usize::MAX for block_count as no specific config is available for it.
                TableBuilder::new_with_block_count(new_store, usize::MAX)
            };

            while let Some(kv_op) = kv_iter_agg.next() {
                // Try to add the operation. If it fails (table is full), flush and start a new one.
                if !current_table_builder.add(kv_op.clone()) {
                    // Table is full, flush it
                    compacted_output_tables.push(Arc::new(current_table_builder.flush()));

                    // Start a new table builder
                    let new_store_id = store_id_generator();
                    let new_store = T::create(&new_store_id);
                    current_table_builder =
                        TableBuilder::new_with_block_count(new_store, usize::MAX);
                    // Add the current operation to the new table
                    let add_res = current_table_builder.add(kv_op.clone()); // This should succeed on a new empty builder
                    assert!(add_res);
                }
            }
            // Flush any remaining data in the last builder
            if !current_table_builder.is_empty() {
                compacted_output_tables.push(Arc::new(current_table_builder.flush()));
            }

            // Record each compacted output table in table_change
            // Determine the start index for new tables based on whether there was overlap
            let start_index = if let Some((first_overlap_idx, _)) = check_res {
                // Case 2: There was overlap, insert at the position where overlapping tables were removed
                first_overlap_idx
            } else {
                // Case 1: No overlap, determine position based on key comparison
                if target_level.sstables.is_empty() {
                    // Empty target level, insert at beginning
                    0
                } else {
                    // Compare input tables' min key with target level's max key
                    let target_max_key = target_level.sstables.last().unwrap().key_range().1;
                    if min_key.as_ref() > target_max_key.as_ref() {
                        // Input tables' min key is greater than target level's max key, insert at end
                        target_level.sstables.len()
                    } else {
                        // Input tables' min key is less than or equal to target level's max key, insert at beginning
                        0
                    }
                }
            };

            for (i, table) in compacted_output_tables.iter().enumerate() {
                table_change.push(TableChange {
                    level: target_level_depth,
                    index: start_index + i,
                    id: table.store_id().parse().unwrap(),
                    change_type: ChangeType::Add,
                });
            }

            // Insert all compacted output tables at the correct position
            for (i, table) in compacted_output_tables.into_iter().enumerate() {
                target_level.sstables.insert(start_index + i, table);
            }
        }
        return (next_sstable_id_counter, table_change);
    }

    /// Determines which tables from a given level need to be compacted and removes them from the level.
    /// It identifies tables that exceed the `max_num` limit for the level and selects
    /// `n` tables with the smallest `store_id` to be compacted.
    ///
    /// Returns a vector of `ThreadSafeTableReader` instances that are to be compacted.
    fn take_out_table_to_compact(
        &mut self,
        level_depth: usize,
    ) -> (Vec<ThreadSafeTableReader<T>>, Vec<TableChange>) {
        let max_num = Self::max_table_in_level(self.level_ratio, level_depth);
        let mut level = &mut self.levels[level_depth];
        let n = if level.sstables.len() > max_num {
            level.sstables.len() - max_num
        } else {
            0
        };

        if n == 0 {
            return (Vec::new(), vec![]);
        }

        // get (store_id, position_in_sstables) of level.sstables
        let mut store_id_positions: Vec<(String, usize)> = level
            .sstables
            .iter()
            .enumerate()
            .map(|(pos, table)| (table.store_id(), pos))
            .collect();

        // sort it by store_id so we can find mini store_id and its position of sstable
        store_id_positions.sort_by(|a, b| a.0.cmp(&b.0));

        // take n sstable from level.sstable by its position and return
        let mut tables_to_compact = Vec::new();
        let mut positions_to_remove: Vec<usize> = store_id_positions
            .into_iter()
            .take(n)
            .map(|(_, pos)| pos)
            .collect();

        // Sort positions in descending order to remove from the end first
        positions_to_remove.sort_by(|a, b| b.cmp(a));

        let mut table_change = Vec::new();
        for pos in positions_to_remove {
            let table = level.sstables.remove(pos);
            table_change.push(TableChange {
                id: table.store_id(),
                index: pos,
                level: level_depth,
                change_type: ChangeType::Delete,
            });
            tables_to_compact.push(table);
        }

        (tables_to_compact, table_change)
    }

    fn table_to_compact(&self, level_index: usize) -> Vec<ThreadSafeTableReader<T>> {
        if level_index >= self.levels.len() {
            return Vec::new();
        }

        let level = &self.levels[level_index];

        if level.is_level_zero {
            // For level zero, return all tables
            level.sstables.clone()
        } else {
            // For other levels, return the table with minimum store_id
            if let Some(min_table) = level.sstables.iter().min_by_key(|table| table.store_id()) {
                vec![min_table.clone()]
            } else {
                Vec::new()
            }
        }
    }
    /// Finds the range of tables in a specific level that overlap with a given key range.
    ///
    /// Returns `Some((first_overlap_idx, last_overlap_idx))` if an overlap is found,
    /// where `first_overlap_idx` is the index of the first overlapping table and
    /// `last_overlap_idx` is the index of the last overlapping table.
    /// Returns `None` if no tables in the specified level overlap with the given key range.
    fn table_range_overlap(
        &self,
        start_key: &KeySlice,
        end_key: &KeySlice,
        level_depth: usize,
    ) -> Option<(usize, usize)> {
        let level = self.levels.get(level_depth)?;

        let mut first_overlap_idx: Option<usize> = None;
        let mut last_overlap_idx: Option<usize> = None;

        for (i, table) in level.sstables.iter().enumerate() {
            let (table_min_key, table_max_key) = table.key_range();

            // Check for overlap between [start_key, end_key] and [table_min_key, table_max_key]
            // Overlap occurs if (start_key <= table_max_key) AND (end_key >= table_min_key)
            if start_key.as_ref().le(table_max_key.as_ref())
                && end_key.as_ref().ge(table_min_key.as_ref())
            {
                if first_overlap_idx.is_none() {
                    first_overlap_idx = Some(i);
                }
                last_overlap_idx = Some(i);
            }
        }

        match (first_overlap_idx, last_overlap_idx) {
            (Some(first), Some(last)) => Some((first, last)),
            _ => None, // No overlap found
        }
    }

    // return level index and table index which need to be compacted
    fn check_level(&self) -> Option<(usize, usize)> {
        // start from level zero to last level
        // if
        unimplemented!()
    }
}

#[cfg(test)]
mod test {
    use std::ops::Range;
    use std::sync::Arc; // Moved from test functions

    use crate::db::common::{KVOpertion, OpId, OpType}; // OpType moved from helper
    use crate::db::key::{KeySlice, KeyVec};
    use crate::db::level::{ChangeType, Level, LevelStorege};
    use crate::db::store::Store;
    // KeyVec moved from helper
    use crate::db::table::test::{create_test_table, create_test_table_with_id_offset};
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

    // New helper to create test table with custom ID
    fn create_test_table_with_id(range: Range<usize>, id: String) -> TableReader<Memstore> {
        let mut v = Vec::new();
        for i in range.clone() {
            let tmp = KVOpertion::new(
                i as u64,
                crate::db::block::test::pad_zero(i as u64).as_bytes().into(),
                OpType::Write(i.to_string().as_bytes().into()),
            );
            v.push(tmp);
        }
        let mut store = Memstore::create(&id);
        let mut table = TableBuilder::new_with_store(store);
        table.fill_with_op(v.iter());
        let res = table.flush();
        res
    }

    #[test]
    fn test_table_to_compact() {
        // Case 1: Level Zero (is_level_zero = true)
        let table_a = Arc::new(create_test_table(0..10)); // id: "test_id_0_10"
        let table_b = Arc::new(create_test_table(10..20)); // id: "test_id_10_20"
        let table_c = Arc::new(create_test_table(20..30)); // id: "test_id_20_30"

        let level_zero = super::Level::new(
            vec![table_a.clone(), table_b.clone(), table_c.clone()],
            true,
        );
        let level_storage_zero = super::LevelStorege::new(vec![level_zero], 2);

        let compacted_tables_zero = level_storage_zero.table_to_compact(0);
        assert_eq!(compacted_tables_zero.len(), 3);
        let mut compacted_ids: Vec<String> = compacted_tables_zero
            .iter()
            .map(|table| table.store_id())
            .collect();
        compacted_ids.sort(); // Sort to ensure consistent order for comparison

        let mut expected_ids = vec![table_a.store_id(), table_b.store_id(), table_c.store_id()];
        expected_ids.sort(); // Sort expected IDs as well

        assert_eq!(compacted_ids, expected_ids);

        // Case 2: Non-Level Zero (is_level_zero = false)
        // Create tables with specific IDs to control the min_by_key behavior
        let table_x = Arc::new(create_test_table_with_id(0..10, "id_001".to_string()));
        let table_y = Arc::new(create_test_table_with_id(10..20, "id_000".to_string())); // This should be the min
        let table_z = Arc::new(create_test_table_with_id(20..30, "id_002".to_string()));

        let level_n = super::Level::new(
            vec![table_x.clone(), table_y.clone(), table_z.clone()],
            false,
        );
        let level_storage_n = super::LevelStorege::new(vec![level_n], 2);

        let compacted_tables_n = level_storage_n.table_to_compact(0);
        assert_eq!(compacted_tables_n.len(), 1);
        assert_eq!(compacted_tables_n[0].store_id(), "id_000".to_string());
        assert_eq!(compacted_tables_n[0].key_range(), table_y.key_range()); // Ensure it's table_y
    }

    #[test]
    fn test_get_key_range() {
        // Create some test tables with known key ranges
        let table_1 = Arc::new(create_test_table_with_id_offset(0..10, 0)); // "000000" to "000009"
        let table_2 = Arc::new(create_test_table_with_id_offset(20..30, 0)); // "000020" to "000029"
        let table_3 = Arc::new(create_test_table_with_id_offset(15..25, 0)); // "000015" to "000024"

        // Scenario 1: Single table
        let level_storage_single = super::LevelStorege::new(vec![], 2); // Levels vector doesn't matter for this test
        let (min_key, max_key) = level_storage_single.get_key_range(&[table_1.clone()]);
        assert_eq!(min_key, KeyVec::from("000000".as_bytes()));
        assert_eq!(max_key, KeyVec::from("000009".as_bytes()));

        // Scenario 2: Multiple tables, ordered
        let tables_ordered = vec![table_1.clone(), table_2.clone()];
        let (min_key, max_key) = level_storage_single.get_key_range(&tables_ordered);
        assert_eq!(min_key, KeyVec::from("000000".as_bytes()));
        assert_eq!(max_key, KeyVec::from("000029".as_bytes()));

        // Scenario 3: Multiple tables, overlapping and out of order
        let tables_mixed = vec![table_2.clone(), table_1.clone(), table_3.clone()];
        let (min_key, max_key) = level_storage_single.get_key_range(&tables_mixed);
        assert_eq!(min_key, KeyVec::from("000000".as_bytes())); // From table_1
        assert_eq!(max_key, KeyVec::from("000029".as_bytes())); // From table_2
    }

    #[test]
    fn test_compact() {
        // Scenario: Compact Level 0 into Level 1
        // Level 0: table_l0_a (0-99, opid 10000), table_l0_b (50-149, opid 20000)
        // Level 1: table_l1_x (100-199, opid 0), table_l1_y (200-299, opid 100)

        // Create tables for Level 0 (overlapping, newest data wins)
        let table_l0_a = Arc::new(create_test_table_with_id_offset(0..100, 10000)); // keys "000000" to "000099", OpIds 10000-10099
        let table_l0_b = Arc::new(create_test_table_with_value_transform(
            50..150,
            20000, // Higher opid offset for newer data, OpIds 20050-20149
            |i| format!("new_val_{}", i),
        )); // keys "000050" to "000149"

        // Level 0: tables ordered newest to oldest to match find logic
        let level0 = super::Level::new(vec![table_l0_b.clone(), table_l0_a.clone()], true);

        // Create tables for Level 1 (non-overlapping) with distinct opid offsets
        let table_l1_x = Arc::new(create_test_table_with_id_offset(100..200, 0)); // keys "000100" to "000199", OpIds 100-199
        let table_l1_y = Arc::new(create_test_table_with_id_offset(200..300, 100)); // keys "000200" to "000299", OpIds 300-399
        let level1 = super::Level::new(vec![table_l1_x.clone(), table_l1_y.clone()], false);

        // Create LevelStorege with levels
        let level_storage = super::LevelStorege::new(vec![level0.clone(), level1], 2);

        // Input tables for compaction (from level 0)
        let input_tables_for_compact = vec![table_l0_b.clone(), table_l0_a.clone()];
        let store_id_start = 10000; // Starting ID for new SSTables

        // Create a mutable copy of level_storage to perform compaction
        let mut level_storage_mut = level_storage;

        // Perform compaction
        let (next_id, _table_changes) =
            level_storage_mut.compact_level(store_id_start, input_tables_for_compact, 0);

        // Get the compacted level (level 1)
        let compacted_level = &level_storage_mut.levels[1];

        // Find the maximum store ID in the compacted level and assert next_id
        let max_store_id_in_compacted_level = compacted_level
            .sstables
            .iter()
            .map(|table| table.store_id().parse::<u64>().unwrap())
            .max()
            .unwrap_or(0); // Default to 0 if no tables, though we assert !is_empty() below

        assert_eq!(
            next_id,
            max_store_id_in_compacted_level + 1,
            "next_id should be max store id in compacted_level + 1"
        );

        // Assertions on the compacted level
        assert!(!compacted_level.is_level_zero); // Should not be level zero
        assert!(!compacted_level.sstables.is_empty()); // Should contain tables

        // Verify the content of the compacted level by querying
        // Expected merged range: 0-299
        // Keys 0-49: from table_l0_a (original value)
        assert_level_find_and_check(
            &compacted_level,
            &KeySlice::from("000025".as_bytes()),
            Some("25".as_bytes()),
        );
        // Keys 50-99: from table_l0_b (new_val_X)
        assert_level_find_and_check(
            &compacted_level,
            &KeySlice::from("000075".as_bytes()),
            Some("new_val_75".as_bytes()),
        );
        // Keys 100-149: from table_l0_b (new_val_X)
        assert_level_find_and_check(
            &compacted_level,
            &KeySlice::from("000125".as_bytes()),
            Some("new_val_125".as_bytes()),
        );
        // Keys 150-199: from table_l1_x (original value)
        assert_level_find_and_check(
            &compacted_level,
            &KeySlice::from("000175".as_bytes()),
            Some("175".as_bytes()),
        );
        // Keys 200-299: from table_l1_y (original value)
        assert_level_find_and_check(
            &compacted_level,
            &KeySlice::from("000250".as_bytes()),
            Some("250".as_bytes()),
        );

        // Key outside the range
        assert_level_find_and_check(&compacted_level, &KeySlice::from("000300".as_bytes()), None);
        assert_level_find_and_check(
            &compacted_level,
            &KeySlice::from("000000".as_bytes()),
            Some("0".as_bytes()),
        );
        assert_level_find_and_check(
            &compacted_level,
            &KeySlice::from("000299".as_bytes()),
            Some("299".as_bytes()),
        );

        // Test case: No overlap in target level
        // Create an empty level 1 to serve as the target for compaction
        let empty_level1 = super::Level::new(vec![], false);
        let mut level_storage_no_overlap =
            super::LevelStorege::new(vec![level0.clone(), empty_level1], 2);
        let input_tables_no_overlap = vec![table_l0_b.clone(), table_l0_a.clone()];
        let (_next_id_no_overlap, _table_changes_no_overlap) =
            level_storage_no_overlap.compact_level(store_id_start, input_tables_no_overlap, 0);

        // Get the compacted level (level 1)
        let compacted_level_no_overlap = &level_storage_no_overlap.levels[1];

        // The compacted level should contain only the merged data from level 0, as there was no level 1 to merge with.
        assert_level_find_and_check(
            &compacted_level_no_overlap,
            &KeySlice::from("000025".as_bytes()),
            Some("25".as_bytes()),
        );
        assert_level_find_and_check(
            &compacted_level_no_overlap,
            &KeySlice::from("000075".as_bytes()),
            Some("new_val_75".as_bytes()),
        );
        assert_level_find_and_check(
            &compacted_level_no_overlap,
            &KeySlice::from("000125".as_bytes()),
            Some("new_val_125".as_bytes()),
        );
        assert_level_find_and_check(
            &compacted_level_no_overlap,
            &KeySlice::from("000175".as_bytes()),
            None,
        ); // No table_l1_x
    }

    #[test]
    fn test_table_range_overlap() {
        // Create tables for a non-level zero scenario (non-overlapping tables)
        // Table 0: keys "000000" to "000099"
        let table_0 = Arc::new(create_test_table(0..100));
        // Table 1: keys "000100" to "000199"
        let table_1 = Arc::new(create_test_table(100..200));
        // Table 2: keys "000200" to "000299"
        let table_2 = Arc::new(create_test_table(200..300));

        let level_n = super::Level::new(
            vec![table_0.clone(), table_1.clone(), table_2.clone()],
            false, // Not level zero
        );

        let level_storage = super::LevelStorege::new(vec![level_n], 2); // Level 0 is the only level here

        let level_depth = 0; // We are testing the first (and only) level

        // Test Case 1: Range fully within one table (table_0)
        let start_key = KeySlice::from("000020".as_bytes());
        let end_key = KeySlice::from("000070".as_bytes());
        assert_eq!(
            level_storage.table_range_overlap(&start_key, &end_key, level_depth),
            Some((0, 0))
        );

        // Test Case 2: Range exactly matches one table (table_1)
        let start_key = KeySlice::from("000100".as_bytes());
        let end_key = KeySlice::from("000199".as_bytes());
        assert_eq!(
            level_storage.table_range_overlap(&start_key, &end_key, level_depth),
            Some((1, 1))
        );

        // Test Case 3: Range overlaps with the end of one table and start of another (table_0 and table_1)
        let start_key = KeySlice::from("000050".as_bytes());
        let end_key = KeySlice::from("000150".as_bytes());
        assert_eq!(
            level_storage.table_range_overlap(&start_key, &end_key, level_depth),
            Some((0, 1))
        );

        // Test Case 4: Range overlaps with the end of one table and start of another (table_1 and table_2)
        let start_key = KeySlice::from("000150".as_bytes());
        let end_key = KeySlice::from("000250".as_bytes());
        assert_eq!(
            level_storage.table_range_overlap(&start_key, &end_key, level_depth),
            Some((1, 2))
        );

        // Test Case 5: Range spans across all tables
        let start_key = KeySlice::from("000000".as_bytes());
        let end_key = KeySlice::from("000299".as_bytes());
        assert_eq!(
            level_storage.table_range_overlap(&start_key, &end_key, level_depth),
            Some((0, 2))
        );

        // Test Case 7: Range completely after all tables
        let start_key = KeySlice::from("000300".as_bytes());
        let end_key = KeySlice::from("000350".as_bytes());
        assert_eq!(
            level_storage.table_range_overlap(&start_key, &end_key, level_depth),
            None
        );

        // Test Case 8: Range between two tables (no overlap)
        let start_key = KeySlice::from("000099X".as_bytes()); // Just after table 0
        let end_key = KeySlice::from("000099Z".as_bytes()); // Just before table 1
        assert_eq!(
            level_storage.table_range_overlap(&start_key, &end_key, level_depth),
            None
        );

        // Test Case 9: Empty level
        let empty_level: Level<Memstore> = super::Level::new(vec![], false);
        let empty_level_storage = super::LevelStorege::new(vec![empty_level], 2);
        let start_key = KeySlice::from("key_start".as_bytes());
        let end_key = KeySlice::from("key_end".as_bytes());
        assert_eq!(
            empty_level_storage.table_range_overlap(&start_key, &end_key, 0),
            None
        );

        // Test Case 10: Overlap at exact boundaries
        let start_key = KeySlice::from("000099".as_bytes()); // Ends exactly at Table 0 max key
        let end_key = KeySlice::from("000100".as_bytes()); // Starts exactly at Table 1 min key
        assert_eq!(
            level_storage.table_range_overlap(&start_key, &end_key, level_depth),
            Some((0, 1))
        );

        // Test Case 11: Single table in level
        let single_table_level = super::Level::new(vec![table_1.clone()], false);
        let single_table_level_storage = super::LevelStorege::new(vec![single_table_level], 2);
        let start_key = KeySlice::from("000120".as_bytes());
        let end_key = KeySlice::from("000180".as_bytes());
        assert_eq!(
            single_table_level_storage.table_range_overlap(&start_key, &end_key, 0),
            Some((0, 0))
        );

        let start_key = KeySlice::from("000090".as_bytes()); // Before
        let end_key = KeySlice::from("000095".as_bytes());
        assert_eq!(
            single_table_level_storage.table_range_overlap(&start_key, &end_key, 0),
            None
        );

        let start_key = KeySlice::from("000200".as_bytes()); // After
        let end_key = KeySlice::from("000210".as_bytes());
        assert_eq!(
            single_table_level_storage.table_range_overlap(&start_key, &end_key, 0),
            None
        );
    }

    #[test]
    fn test_take_out_table_to_compact() {
        // Create test tables with specific IDs to control ordering
        let table_a = Arc::new(create_test_table_with_id(0..10, "id_003".to_string()));
        let table_b = Arc::new(create_test_table_with_id(10..20, "id_001".to_string())); // Should be first when sorted
        let table_c = Arc::new(create_test_table_with_id(20..30, "id_002".to_string())); // Should be second when sorted
        let table_d = Arc::new(create_test_table_with_id(30..40, "id_004".to_string()));

        // Test Case 1: Take out tables from level 0 (should take tables exceeding limit)
        let level0 = super::Level::new(
            vec![
                table_a.clone(),
                table_b.clone(),
                table_c.clone(),
                table_d.clone(),
            ],
            true, // Level 0
        );
        let mut level_storage = super::LevelStorege::new(vec![level0], 2);

        // Level 0 limit is MAX_LEVEL_ZERO_TABLE_SIZE = 4, so with 4 tables, no compaction needed
        let (tables_to_compact, changes) = level_storage.take_out_table_to_compact(0);
        assert_eq!(tables_to_compact.len(), 0);
        assert_eq!(changes.len(), 0); // No changes expected
        assert_eq!(level_storage.levels[0].sstables.len(), 4); // No tables removed

        // Add one more table to exceed the limit
        let table_e = Arc::new(create_test_table_with_id(40..50, "id_000".to_string())); // Smallest ID
        level_storage.levels[0].sstables.push(table_e.clone());

        // Now we have 5 tables, should take out 1 (5 - 4 = 1)
        let (tables_to_compact, changes) = level_storage.take_out_table_to_compact(0);
        assert_eq!(tables_to_compact.len(), 1);
        // Should get table with smallest store_id
        assert_eq!(tables_to_compact[0].store_id(), "id_000".to_string());
        // Verify table_change
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].id, "id_000".to_string());
        assert_eq!(changes[0].level, 0);
        assert_eq!(changes[0].change_type, ChangeType::Delete);

        // Remaining tables should be 4
        assert_eq!(level_storage.levels[0].sstables.len(), 4);

        // Test Case 2: Level 1 (non-level-zero)
        let level1 = super::Level::new(
            vec![table_a.clone(), table_b.clone(), table_c.clone()],
            false, // Not level 0
        );
        let mut level_storage2 = super::LevelStorege::new(
            vec![
                super::Level::new(vec![], true), // Empty level 0
                level1,
            ],
            2,
        );

        // Level 1 limit is MAX_LEVEL_ZERO_TABLE_SIZE.pow(2) = 4.pow(2) = 16
        // With only 3 tables, no compaction needed
        let (tables_to_compact2, changes2) = level_storage2.take_out_table_to_compact(1);
        assert_eq!(tables_to_compact2.len(), 0);
        assert_eq!(changes2.len(), 0); // No changes expected
        assert_eq!(level_storage2.levels[1].sstables.len(), 3); // No tables removed

        // Test Case 3: Empty level
        let empty_level: Level<Memstore> = super::Level::new(vec![], false);
        let mut empty_level_storage = super::LevelStorege::new(vec![empty_level], 2);
        let (tables_to_compact3, changes3) = empty_level_storage.take_out_table_to_compact(0);

        assert_eq!(tables_to_compact3.len(), 0);
        assert_eq!(changes3.len(), 0); // No changes expected
        assert_eq!(empty_level_storage.levels[0].sstables.len(), 0);

        // Test Case 4: Level with many tables exceeding limit
        let many_tables: Vec<Arc<TableReader<Memstore>>> = (0..20)
            .map(|i| {
                Arc::new(create_test_table_with_id(
                    i * 10..(i * 10 + 10),
                    format!("id_{:03}", i),
                ))
            })
            .collect();

        let level_many = super::Level::new(many_tables, false);
        let mut level_storage_many = super::LevelStorege::new(
            vec![
                super::Level::new(vec![], true), // Empty level 0
                level_many,
            ],
            2,
        );

        // Level 1 limit is 16, with 20 tables, should take out 4 (20 - 16 = 4)
        let (tables_to_compact4, changes4) = level_storage_many.take_out_table_to_compact(1);
        assert_eq!(tables_to_compact4.len(), 4);
        assert_eq!(level_storage_many.levels[1].sstables.len(), 16);

        // Should get tables with smallest store_ids (id_000, id_001, id_002, id_003)
        let mut compacted_ids: Vec<String> = tables_to_compact4
            .iter()
            .map(|table| table.store_id())
            .collect();
        compacted_ids.sort();
        assert_eq!(
            compacted_ids,
            vec![
                "id_000".to_string(),
                "id_001".to_string(),
                "id_002".to_string(),
                "id_003".to_string()
            ]
        );
        // Verify table_change for multiple deletions
        assert_eq!(changes4.len(), 4);
        let mut changed_ids: Vec<String> = changes4.iter().map(|c| c.id.clone()).collect();
        changed_ids.sort();
        assert_eq!(changed_ids, compacted_ids); // IDs should match
        for change in changes4 {
            assert_eq!(change.level, 1);
            assert_eq!(change.change_type, ChangeType::Delete);
        }
    }

    #[test]
    fn test_compact_storage() {
        // Test Case 1: Level 0 exceeds limit, should compact to Level 1
        let table_a = Arc::new(create_test_table_with_id(0..10, "id_001".to_string()));
        let table_b = Arc::new(create_test_table_with_id(10..20, "id_002".to_string()));
        let table_c = Arc::new(create_test_table_with_id(20..30, "id_003".to_string()));
        let table_d = Arc::new(create_test_table_with_id(30..40, "id_004".to_string()));
        let table_e = Arc::new(create_test_table_with_id(40..50, "id_005".to_string())); // This will exceed limit

        // Create level 0 with 5 tables (exceeds MAX_LEVEL_ZERO_TABLE_SIZE = 4)
        let level0 = super::Level::new(
            vec![
                table_a.clone(),
                table_b.clone(),
                table_c.clone(),
                table_d.clone(),
                table_e.clone(),
            ],
            true,
        );
        let mut level_storage = super::LevelStorege::new(vec![level0], 2);

        // Before compaction: Level 0 has 5 tables, no Level 1
        assert_eq!(level_storage.levels.len(), 1);
        assert_eq!(level_storage.levels[0].sstables.len(), 5);

        // Perform compaction
        let table_changes = level_storage.compact_storage(1000);

        // After compaction: Level 0 should have 4 tables, Level 1 should exist with compacted table
        assert_eq!(level_storage.levels.len(), 2); // Level 1 should be created
        assert_eq!(level_storage.levels[0].sstables.len(), 4); // Level 0 reduced to limit
        assert!(!level_storage.levels[1].sstables.is_empty()); // Level 1 has compacted data

        // Verify that the table with smallest ID was moved to Level 1
        let remaining_ids: Vec<String> = level_storage.levels[0]
            .sstables
            .iter()
            .map(|table| table.store_id())
            .collect();
        assert!(!remaining_ids.contains(&"id_001".to_string())); // Smallest ID should be compacted

        // Verify table_changes
        let delete_changes: Vec<_> = table_changes
            .iter()
            .filter(|tc| tc.change_type == ChangeType::Delete)
            .collect();
        let add_changes: Vec<_> = table_changes
            .iter()
            .filter(|tc| tc.change_type == ChangeType::Add)
            .collect();

        // Expect one delete from level 0 (id_001)
        assert_eq!(delete_changes.len(), 1);
        assert_eq!(delete_changes[0].level, 0);
        assert_eq!(delete_changes[0].id, "id_001".to_string());
        assert_eq!(delete_changes[0].change_type, ChangeType::Delete);

        // Expect at least one add to level 1 (the compacted table)
        assert!(!add_changes.is_empty());
        for add_change in add_changes {
            assert_eq!(add_change.level, 1);
            assert_eq!(add_change.change_type, ChangeType::Add);
            assert!(add_change.id.parse::<u64>().unwrap() >= 1000); // Should use the provided store_id_start
        }

        // KV checks after compaction: Verify all data is still accessible
        // Check keys from the compacted table (id_001, range 0..10)
        for i in 0..10 {
            let data = crate::db::block::test::pad_zero(i as u64)
                .as_bytes()
                .to_vec();
            let key = KeySlice::from(data.as_ref());
            assert_level_storage_find_and_check(
                &level_storage,
                &key,
                Some(i.to_string().as_bytes()),
                u64::MAX,
            );
        }

        // Check keys from remaining tables in level 0
        for i in 10..50 {
            let data = crate::db::block::test::pad_zero(i as u64)
                .as_bytes()
                .to_vec();
            let key = KeySlice::from(data.as_ref());
            assert_level_storage_find_and_check(
                &level_storage,
                &key,
                Some(i.to_string().as_bytes()),
                u64::MAX,
            );
        }

        // Test Case 2: Multiple levels need compaction
        let many_tables_l1: Vec<Arc<TableReader<Memstore>>> = (0..20)
            .map(|i| {
                Arc::new(create_test_table_with_id(
                    i * 10..(i * 10 + 10),
                    format!("l1_id_{:03}", i),
                ))
            })
            .collect();

        let level1_many = super::Level::new(many_tables_l1, false);
        let mut level_storage_multi = super::LevelStorege::new(
            vec![
                super::Level::new(vec![], true), // Empty level 0
                level1_many,
            ],
            2,
        );

        // Before compaction: Level 1 has 20 tables (exceeds limit of 16)
        assert_eq!(level_storage_multi.levels[1].sstables.len(), 20);

        // Perform compaction
        level_storage_multi.compact_storage(2000);

        // After compaction: Level 1 should have 16 tables, Level 2 should exist
        assert_eq!(level_storage_multi.levels.len(), 3); // Level 2 should be created
        assert_eq!(level_storage_multi.levels[1].sstables.len(), 16); // Level 1 reduced to limit
        assert!(!level_storage_multi.levels[2].sstables.is_empty()); // Level 2 has compacted data

        // Test Case 3: No compaction needed (all levels within limits)
        let small_table = Arc::new(create_test_table_with_id(0..10, "small_id".to_string()));
        let level0_small = super::Level::new(vec![small_table], true);
        let mut level_storage_small = super::LevelStorege::new(vec![level0_small], 2);

        let original_len = level_storage_small.levels.len();
        let original_table_count = level_storage_small.levels[0].sstables.len();

        // Perform compaction
        level_storage_small.compact_storage(3000);

        // Should remain unchanged
        assert_eq!(level_storage_small.levels.len(), original_len);
        assert_eq!(
            level_storage_small.levels[0].sstables.len(),
            original_table_count
        );

        // Test Case 4: Empty level storage
        let mut empty_level_storage: LevelStorege<Memstore> = super::LevelStorege::new(vec![], 2);

        // Should not panic and remain empty
        empty_level_storage.compact_storage(4000);
        assert_eq!(empty_level_storage.levels.len(), 0);
    }

    #[test]
    fn test_max_table_in_level() {
        // Test level 0
        assert_eq!(
            super::LevelStorege::<Memstore>::max_table_in_level(2, 0),
            super::MAX_LEVEL_ZERO_TABLE_SIZE
        );
        assert_eq!(
            super::LevelStorege::<Memstore>::max_table_in_level(3, 0),
            super::MAX_LEVEL_ZERO_TABLE_SIZE
        );

        // Test level 1 with ratio 2
        // MAX_LEVEL_ZERO_TABLE_SIZE.pow(2) = 4.pow(2) = 16
        assert_eq!(
            super::LevelStorege::<Memstore>::max_table_in_level(2, 1),
            super::MAX_LEVEL_ZERO_TABLE_SIZE.pow(2)
        );

        // Test level 2 with ratio 2
        // MAX_LEVEL_ZERO_TABLE_SIZE.pow(2) = 4.pow(2) = 16
        assert_eq!(
            super::LevelStorege::<Memstore>::max_table_in_level(2, 2),
            super::MAX_LEVEL_ZERO_TABLE_SIZE.pow(2)
        );

        // Test level 1 with ratio 3
        // MAX_LEVEL_ZERO_TABLE_SIZE.pow(3) = 4.pow(3) = 64
        assert_eq!(
            super::LevelStorege::<Memstore>::max_table_in_level(3, 1),
            super::MAX_LEVEL_ZERO_TABLE_SIZE.pow(3)
        );
    }

    #[test]
    fn test_compact_level_table_ordering() {
        // Test that tables in a level are ordered by their first key after compaction

        // Create tables with non-sequential key ranges to test ordering
        let table_c = Arc::new(create_test_table_with_id(200..300, "table_c".to_string())); // keys "000200" to "000299"
        let table_a = Arc::new(create_test_table_with_id(0..100, "table_a".to_string())); // keys "000000" to "000099"
        let table_b = Arc::new(create_test_table_with_id(100..200, "table_b".to_string())); // keys "000100" to "000199"

        // Create level 0 with tables in non-sorted order
        let level0 = super::Level::new(
            vec![table_c.clone(), table_a.clone(), table_b.clone()],
            true,
        );

        // Create empty level 1 as target
        let empty_level1 = super::Level::new(vec![], false);
        let mut level_storage = super::LevelStorege::new(vec![level0, empty_level1], 2);

        // Perform compaction from level 0 to level 1
        let input_tables = vec![table_c.clone(), table_a.clone(), table_b.clone()];
        let (_next_id, _table_changes) = level_storage.compact_level(1000, input_tables, 0);

        // Get the compacted level (level 1)
        let compacted_level = &level_storage.levels[1];

        // Verify that tables are ordered by their first key
        assert!(
            !compacted_level.sstables.is_empty(),
            "Compacted level should not be empty"
        );

        // Check that tables are sorted by first key
        for i in 1..compacted_level.sstables.len() {
            let prev_first_key = compacted_level.sstables[i - 1].key_range().0;
            let curr_first_key = compacted_level.sstables[i].key_range().0;
            assert!(
                prev_first_key.as_ref() <= curr_first_key.as_ref(),
                "Tables should be ordered by first key: {:?} should be <= {:?}",
                prev_first_key.to_string(),
                curr_first_key.to_string()
            );
        }

        // Verify that all data is still accessible in the correct order
        // Test a key from each original range to ensure data integrity
        assert_level_find_and_check(
            &compacted_level,
            &KeySlice::from("000050".as_bytes()),
            Some("50".as_bytes()),
        );
        assert_level_find_and_check(
            &compacted_level,
            &KeySlice::from("000150".as_bytes()),
            Some("150".as_bytes()),
        );
        assert_level_find_and_check(
            &compacted_level,
            &KeySlice::from("000250".as_bytes()),
            Some("250".as_bytes()),
        );
    }

    #[test]
    fn test_compact_level_table_change() {
        // Case 1: Overlap with target level
        // Create input tables from level 0
        let table_l0_a = Arc::new(create_test_table_with_id_offset(0..50, 1000)); // keys "000000" to "000049"
        let table_l0_b = Arc::new(create_test_table_with_id_offset(25..75, 2000)); // keys "000025" to "000074"

        // Create existing tables in target level 1 that overlap
        let table_l1_x = Arc::new(create_test_table_with_id_offset(40..90, 0)); // keys "000040" to "000089" - overlaps
        let table_l1_y = Arc::new(create_test_table_with_id_offset(100..150, 100)); // keys "000100" to "000149" - no overlap

        let level0 = super::Level::new(vec![table_l0_a.clone(), table_l0_b.clone()], true);
        let level1 = super::Level::new(vec![table_l1_x.clone(), table_l1_y.clone()], false);
        let mut level_storage = super::LevelStorege::new(vec![level0, level1], 2);

        // Perform compaction
        let input_tables = vec![table_l0_a.clone(), table_l0_b.clone()];
        let (next_id, table_changes) = level_storage.compact_level(5000, input_tables, 0);

        // Verify table changes for overlap case
        // Should have:
        // - Delete operations for overlapping tables in target level
        // - Add operations for new compacted tables
        let delete_changes: Vec<_> = table_changes
            .iter()
            .filter(|tc| tc.change_type == ChangeType::Delete)
            .collect();
        let add_changes: Vec<_> = table_changes
            .iter()
            .filter(|tc| tc.change_type == ChangeType::Add)
            .collect();

        // Should delete the overlapping table (table_l1_x)
        assert_eq!(delete_changes.len(), 1);
        assert_eq!(delete_changes[0].level, 1);
        assert_eq!(
            delete_changes[0].id.parse::<u64>().unwrap(),
            table_l1_x.store_id().parse::<u64>().unwrap()
        );

        // Should add new compacted tables
        assert!(!add_changes.is_empty());
        for add_change in &add_changes {
            assert_eq!(add_change.level, 1);
            assert_eq!(add_change.change_type, ChangeType::Add);
            assert!(add_change.id.parse::<u64>().unwrap() >= 5000); // Should use the provided store_id_start
        }

        // Case 2: No overlap with target level, input tables' keys < target level min key
        // Create input tables with keys smaller than existing target level
        let table_l0_c = Arc::new(create_test_table_with_id_offset(0..30, 3000)); // keys "000000" to "000029"
        let table_l0_d = Arc::new(create_test_table_with_id_offset(10..40, 4000)); // keys "000010" to "000039"

        // Create target level with tables that have larger keys (no overlap)
        let table_l1_z = Arc::new(create_test_table_with_id_offset(200..250, 200)); // keys "000200" to "000249"
        let table_l1_w = Arc::new(create_test_table_with_id_offset(300..350, 300)); // keys "000300" to "000349"

        let level0_case2 = super::Level::new(vec![table_l0_c.clone(), table_l0_d.clone()], true);
        let level1_case2 = super::Level::new(vec![table_l1_z.clone(), table_l1_w.clone()], false);
        let mut level_storage_case2 = super::LevelStorege::new(vec![level0_case2, level1_case2], 2);

        // Perform compaction
        let input_tables_case2 = vec![table_l0_c.clone(), table_l0_d.clone()];
        let (next_id_case2, table_changes_case2) =
            level_storage_case2.compact_level(6000, input_tables_case2, 0);

        // Verify table changes for no overlap case
        let delete_changes_case2: Vec<_> = table_changes_case2
            .iter()
            .filter(|tc| tc.change_type == ChangeType::Delete)
            .collect();
        let add_changes_case2: Vec<_> = table_changes_case2
            .iter()
            .filter(|tc| tc.change_type == ChangeType::Add)
            .collect();

        // Should have no delete operations (no overlap)
        assert_eq!(delete_changes_case2.len(), 0);

        // Should have add operations for new compacted tables
        assert!(!add_changes_case2.is_empty());
        for add_change in &add_changes_case2 {
            assert_eq!(add_change.level, 1);
            assert_eq!(add_change.change_type, ChangeType::Add);
            assert!(add_change.id.parse::<u64>().unwrap() >= 6000); // Should use the provided store_id_start
                                                                    // Since input keys are smaller than target level keys, new tables should be inserted at the beginning
            assert_eq!(add_change.index, 0);
        }

        // Verify that the original tables in target level are still there and shifted
        let final_level = &level_storage_case2.levels[1];
        assert!(final_level.sstables.len() >= 2); // At least the original 2 tables plus compacted ones

        // The last tables should be the original ones (table_l1_z and table_l1_w)
        let last_tables: Vec<String> = final_level
            .sstables
            .iter()
            .skip(final_level.sstables.len() - 2)
            .map(|t| t.store_id())
            .collect();
        assert!(last_tables.contains(&table_l1_z.store_id()));
        assert!(last_tables.contains(&table_l1_w.store_id()));
    }
}
