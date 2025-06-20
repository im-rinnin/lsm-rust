use std::{
    collections::{HashMap, HashSet},
    default,
    fs::File,
    io::{Read, Write},
    sync::Arc,
    usize,
};

use anyhow::Result;
use byteorder::{LittleEndian, WriteBytesExt};
use crc32fast::Hasher;
use tracing::info;

use crate::db::{common::KViterAgg, key::KeyBytes};
use tracing::debug;

use super::{
    common::{KVOpertion, OpId, OpType, SearchResult},
    key::{KeySlice, KeyVec},
    lsm_storage::LsmStorage,
    store::{Filestore, Store, StoreId},
    table::{self, *},
};

const DEFAULT_MAX_LEVEL_ZERO_TABLE_SIZE: usize = 4;
const MAX_INPUT_TABLE_IN_COMPACT: usize = 1;
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ChangeType {
    Add,    // encode to 0
    Delete, // encode to 1
}

#[derive(Debug, Clone)]
pub struct TableChange {
    level: usize,
    index: usize,
    id: u64,
    change_type: ChangeType,
}

impl TableChange {
    fn encode<W: Write>(&self, writer: &mut W) {
        writer.write_u64::<LittleEndian>(self.level as u64).unwrap();
        writer.write_u64::<LittleEndian>(self.index as u64).unwrap();
        writer.write_u64::<LittleEndian>(self.id).unwrap();
        writer
            .write_u8(match self.change_type {
                ChangeType::Add => 0,
                ChangeType::Delete => 1,
            })
            .unwrap();
    }
    fn decode<R: Read>(mut data: R) -> Self {
        use byteorder::{LittleEndian, ReadBytesExt};

        let level = data.read_u64::<LittleEndian>().unwrap() as usize;
        let index = data.read_u64::<LittleEndian>().unwrap() as usize;
        let id = data.read_u64::<LittleEndian>().unwrap();
        let change_type_byte = data.read_u8().unwrap();
        let change_type = match change_type_byte {
            0 => ChangeType::Add,
            1 => ChangeType::Delete,
            _ => panic!("Unknown ChangeType byte: {}", change_type_byte),
        };
        TableChange {
            level,
            index,
            id,
            change_type,
        }
    }
}

// level change 0: [table_change_count(u64)][table_change_entry_0][table_change_entry_1][check_sum of all table change]
pub struct TableChangeLog<T: Store> {
    storage: T,
}
const U64_SIZE: usize = std::mem::size_of::<u64>();
const U32_SIZE: usize = std::mem::size_of::<u32>();
// level (u64) + index (u64) + id (u64) + change_type (u8)
const TABLE_CHANGE_ENCODED_SIZE: usize = U64_SIZE * 3 + std::mem::size_of::<u8>();

impl TableChangeLog<Filestore> {
    pub fn from_file(f: File, id: StoreId) -> Self {
        TableChangeLog {
            storage: Filestore::open_with_file(f, id),
        }
    }
}
impl<T: Store> TableChangeLog<T> {
    pub fn from(id: StoreId) -> Self {
        TableChangeLog {
            storage: T::open_with(id, "table_changes", "data"),
        }
    }

    #[cfg(test)]
    pub fn new_with_store(storage: T) -> Self {
        TableChangeLog { storage }
    }

    pub fn append(&mut self, changes: Vec<TableChange>) {
        // Write the number of changes as a u64
        self.storage.append(&(changes.len() as u64).to_le_bytes());

        // Write each TableChange entry
        let mut hasher = Hasher::new();
        for change in changes {
            let mut buffer = Vec::new();
            change.encode(&mut buffer);
            hasher.update(&buffer); // Update checksum with each change's bytes
            self.storage.append(&buffer);
        }
        let checksum = hasher.finalize();
        self.storage.append(&checksum.to_le_bytes()); // Append the checksum
        self.storage.flush();
    }

    fn get_all_changes(&self) -> Result<Vec<TableChange>> {
        let storage_len = self.storage.len();
        let mut all_decoded_changes = Vec::new();
        let mut current_read_offset = 0;

        while current_read_offset < storage_len {
            // Read count (u64)
            if current_read_offset + U64_SIZE > storage_len {
                eprintln!("TableChangeLog: Corrupted log file - incomplete count header at offset {}. Remaining bytes: {}", current_read_offset, storage_len - current_read_offset);
                break;
            }
            let mut count_buf = [0u8; U64_SIZE];
            self.storage.read_at(&mut count_buf, current_read_offset);
            let num_changes_in_batch = u64::from_le_bytes(count_buf) as usize;
            current_read_offset += U64_SIZE;

            let batch_changes_data_len = num_changes_in_batch * TABLE_CHANGE_ENCODED_SIZE;
            let batch_expected_total_len = batch_changes_data_len + U32_SIZE; // Data + Checksum

            if current_read_offset + batch_expected_total_len > storage_len {
                eprintln!("TableChangeLog: Corrupted log file - incomplete batch data or checksum at offset {}. Expected total batch bytes: {}, Remaining bytes: {}", current_read_offset, batch_expected_total_len, storage_len - current_read_offset);
                break;
            }

            let mut hasher = Hasher::new();
            let mut change_bytes_buf = [0u8; TABLE_CHANGE_ENCODED_SIZE];

            for _i in 0..num_changes_in_batch {
                self.storage
                    .read_at(&mut change_bytes_buf, current_read_offset);
                hasher.update(&change_bytes_buf);

                let decoded_change = TableChange::decode(&change_bytes_buf[..]);
                all_decoded_changes.push(decoded_change);
                current_read_offset += TABLE_CHANGE_ENCODED_SIZE;
            }

            // Read checksum (u32)
            let mut checksum_buf = [0u8; U32_SIZE];
            self.storage.read_at(&mut checksum_buf, current_read_offset);
            let expected_checksum = u32::from_le_bytes(checksum_buf);
            current_read_offset += U32_SIZE;

            let calculated_checksum = hasher.finalize();
            if calculated_checksum != expected_checksum {
                eprintln!("TableChangeLog: Checksum mismatch detected for a batch at offset {}. Calculated: {}, Expected: {}. Log might be corrupted.", current_read_offset - U32_SIZE, calculated_checksum, expected_checksum);
                return Err(anyhow::anyhow!("decode table change checksum"));
            }
        }
        Ok(all_decoded_changes)
    }
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
    config: LevelStoregeConfig,
}

impl<T: Store> Clone for LevelStorege<T> {
    fn clone(&self) -> Self {
        LevelStorege {
            levels: self.levels.clone(),
            config: self.config.clone(),
        }
    }
}
#[derive(Clone, Copy)]
pub struct LevelStoregeConfig {
    pub level_zero_num_limit: usize,
    pub level_ratio: usize,
    pub max_input_table_num_in_compact: usize,
    pub table_config: TableConfig,
}

impl Default for LevelStoregeConfig {
    fn default() -> Self {
        LevelStoregeConfig {
            level_zero_num_limit: DEFAULT_MAX_LEVEL_ZERO_TABLE_SIZE,
            level_ratio: 4,
            max_input_table_num_in_compact: MAX_INPUT_TABLE_IN_COMPACT,
            table_config: TableConfig::default(),
        }
    }
}

impl LevelStoregeConfig {
    pub fn config_for_test() -> Self {
        LevelStoregeConfig {
            level_zero_num_limit: 2,
            level_ratio: 2,
            max_input_table_num_in_compact: MAX_INPUT_TABLE_IN_COMPACT,
            table_config: TableConfig::new_for_test(),
        }
    }
}

impl<T: Store> LevelStorege<T> {
    pub fn new(tables: Vec<Level<T>>, config: LevelStoregeConfig) -> Self {
        LevelStorege {
            levels: tables,
            config,
        }
    }
    // return table number in every level, table number in level[0]=vec[0]
    pub fn table_num_in_levels(&self) -> Vec<usize> {
        self.levels
            .iter()
            .map(|level| level.sstables.len())
            .collect()
    }

    pub fn get_tables_level(&self) -> Vec<Vec<ThreadSafeTableReader<T>>> {
        self.levels
            .iter()
            .map(|level| level.sstables.clone())
            .collect()
    }

    // add new table from iterator `it` to level 0.
    // `next_sstable_id` is used to generate a unique ID for the new table and is incremented.
    pub fn push_new_table<P: Iterator<Item = KVOpertion>>(
        &mut self,
        mut it: P, // Take iterator by value or mutable ref depending on usage
        next_sstable_id: &mut StoreId,
    ) {
        // Ensure level 0 exists
        if self.levels.is_empty() {
            self.levels.push(Level::new(vec![], true)); // Create level 0 if it doesn't exist
        } else if !self.levels[0].is_level_zero {
            // If level 0 exists but isn't marked as level zero, insert a new level 0
            self.levels.insert(0, Level::new(vec![], true));
        }

        // Generate ID for the new table
        let new_table_id = *next_sstable_id;
        *next_sstable_id += 1;

        let mut table_builder =
            TableBuilder::new_with_id_config(new_table_id, self.config.table_config);

        // Fill the table builder with data from the iterator
        // Use a loop to handle potential errors or specific logic if `fill` isn't suitable
        // Keep track of the tables created in this push operation
        let mut created_tables = Vec::new();

        while let Some(op) = it.next() {
            // Try adding the operation to the current builder
            if !table_builder.add(op.clone()) {
                // If add fails, the current builder is full.
                // Flush the full builder if it's not empty.
                if !table_builder.is_empty() {
                    let new_table_reader = table_builder.flush();
                    created_tables.push(Arc::new(new_table_reader));
                }

                // Create a new builder for the next table.
                let new_table_id = *next_sstable_id;
                *next_sstable_id += 1;
                table_builder =
                    TableBuilder::new_with_id_config(new_table_id, self.config.table_config);

                // Add the operation that didn't fit into the previous builder to the new one.
                // This should always succeed on a fresh builder unless the single op is too large.
                if !table_builder.add(op.clone()) {
                    // Handle the edge case where a single operation is too large for a block/table.
                    // This might indicate a configuration issue or an unexpectedly large KV pair.
                    panic!("Error: Single KVOperation is too large to fit in a new table block during push_new_table. Operation ID: {}", op.id);
                }
            }
        }

        // After the loop, flush the last builder if it contains any data.
        if !table_builder.is_empty() {
            let new_table_reader = table_builder.flush();
            created_tables.push(Arc::new(new_table_reader));
        }

        // Add all newly created tables to the beginning of level 0 in reverse order
        // so the table containing the latest data appears first.
        for table_reader in created_tables.into_iter().rev() {
            info!(id = table_reader.store_id(), "dump memtable to level zero");
            self.levels[0].sstables.insert(0, table_reader);
        }
        // If the iterator was empty and no tables were created, this function effectively does nothing.
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

    // Calculate the maximum number of tables allowed in a given level.
    fn max_table_in_level(&self, level_depth: usize) -> usize {
        return self.config.level_zero_num_limit * self.config.level_ratio.pow(level_depth as u32);
    }

    pub fn need_compact(&self) -> bool {
        for level_depth in 0..self.levels.len() {
            let max_tables = self.max_table_in_level(level_depth);
            if self.levels[level_depth].sstables.len() > max_tables {
                return true;
            }
        }
        false
    }

    /// Initiates the compaction process across all levels of the LSM storage.
    ///
    /// This function iterates through each level, identifies if compaction is needed
    /// based on the number of tables exceeding the configured limit for that level,
    /// and then performs the compaction. Compaction involves merging tables from
    /// the current level with overlapping tables from the next level, creating
    /// new compacted tables, and updating the level metadata.
    ///
    /// It returns a vector of `TableChange` events that describe the additions
    /// and deletions of tables during the compaction process. These changes
    /// should be persisted to the table change log.
    ///
    /// # Arguments
    /// * `next_store_id` - The starting ID to use for newly created SSTable files.
    ///
    /// # Returns
    /// A `Vec<TableChange>` detailing all table additions and deletions that occurred.
    pub fn compact_storage(&mut self, mut next_store_id: &mut u64) -> Vec<TableChange> {
        info!("start compact storage");
        let mut table_change = Vec::new();
        // Iterate through all levels starting from level 0
        for level_depth in 0..self.levels.len() {
            // Check if this level needs compaction
            let max_tables = self.max_table_in_level(level_depth); // Use instance method
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
            let table_changes_from_compact_level =
                self.compact_level(&mut next_store_id, tables_to_compact, level_depth);

            table_change.extend(table_changes_from_compact_level);

            // After compacting one level, we might need to check if the target level
            // now also needs compaction, but we'll handle that in the next iteration
        }
        for change in &table_change {
            debug!(
                level = change.level,
                index = change.index,
                id = change.id,
                change_type = ?change.change_type,
                "table change during lsm compaction"
            );
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

    fn get_target_level_key_ranges(sstables: &Vec<Arc<TableReader<T>>>) -> Vec<(KeyVec, KeyVec)> {
        let mut target_level_key_value_range = vec![];
        for table in sstables {
            target_level_key_value_range.push(table.key_range());
        }
        target_level_key_value_range.sort_by(|a, b| a.0.cmp(&b.0));
        target_level_key_value_range
    }

    // case 1 target level contains no table
    // case 2 no overlap in target level but overlap in input_tables
    // case 3 overlap in target level and no overlap in input_tables
    // case 4 no overlap in target level and no overlap in input_tables
    // case 5 input_tables overlap with target_level

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
    /// A `Vec<TableChange>` detailing all table additions and deletions that occurred.
    fn compact_level(
        &mut self,
        store_id_start: &mut u64,
        input_tables: Vec<ThreadSafeTableReader<T>>,
        level_depth: usize,
    ) -> Vec<TableChange> {
        info!(level = level_depth, "compact level");
        if tracing::event_enabled!(tracing::Level::DEBUG) {
            for table in &input_tables {
                let (min_key, max_key) = table.key_range();
                debug!(
                    store_id = table.store_id(),
                    min_key = %min_key.to_string(),
                    max_key = %max_key.to_string(),
                    "compacting input table"
                );
            }
        }
        let target_level = level_depth + 1;

        if input_tables.is_empty() {
            return vec![];
        }

        let mut table_change = vec![];

        //  find all table in target_level overlap with input_tables and also get input table overlap with target_level
        let mut target_overlap_index_set = HashSet::new();
        let mut input_no_overlap_index_set = HashSet::new();

        let target_level_key_value_range =
            Self::get_target_level_key_ranges(&self.levels.get(target_level).unwrap().sstables);

        for (input_index, table) in input_tables.iter().enumerate() {
            let key_range = table.key_range();
            let table_index = Self::key_range_overlap(key_range, &target_level_key_value_range);
            if table_index.is_empty() {
                input_no_overlap_index_set.insert(input_index);
            } else {
                target_overlap_index_set.extend(table_index);
            }
        }
        //
        // for table in no_overlap_tables_in_target_level, find table no overlap with input_tables,
        // name it as pass_table(don't need compacet)
        let mut no_need_to_compact_input_table_index = vec![];
        let input_key_range = Self::get_target_level_key_ranges(&input_tables);
        for index in input_no_overlap_index_set {
            let table = input_tables.get(index).unwrap();
            let check_res = Self::key_range_overlap(table.key_range(), &input_key_range);
            assert!(check_res.len() >= 1);
            if check_res.len() == 1 {
                no_need_to_compact_input_table_index.push(index);
            }
        }
        // Separate input_tables into those that need compaction and those that don't
        let mut tables_to_compact_from_input = Vec::new();
        let mut pass_through_input_tables = Vec::new();
        for (index, table) in input_tables.into_iter().enumerate() {
            if no_need_to_compact_input_table_index.contains(&index) {
                pass_through_input_tables.push(table);
            } else {
                tables_to_compact_from_input.push(table);
            }
        }

        // 4. Do compaction for tables that overlap from both input_tables and target_level
        let mut tables_from_target_to_compact: Vec<ThreadSafeTableReader<T>> = Vec::new();
        let mut tables_to_pass_through_target: Vec<ThreadSafeTableReader<T>> = Vec::new();
        let current_target_sstables = std::mem::take(&mut self.levels[target_level].sstables); // Temporarily take ownership

        for (index, table) in current_target_sstables.into_iter().enumerate() {
            if target_overlap_index_set.contains(&index) {
                tables_from_target_to_compact.push(table.clone()); // Clone Arc for compaction
                table_change.push(TableChange {
                    id: table.store_id(),
                    index: index, // This index refers to the original position in target_level
                    level: target_level,
                    change_type: ChangeType::Delete,
                });
            } else {
                tables_to_pass_through_target.push(table);
            }
        }

        let mut all_tables_for_merge = Vec::new();
        all_tables_for_merge.extend(tables_to_compact_from_input);
        all_tables_for_merge.extend(tables_from_target_to_compact);

        // Sort tables for merge by first key to ensure correct merge order
        all_tables_for_merge.sort_by(|a, b| a.key_range().0.cmp(&b.key_range().0));

        // Create a vector to own the concrete iterator instances.
        // The TableIter will borrow the TableReader inside the Arc.
        // all_tables_for_merge (which owns the Arcs) must live longer than concrete_iterators.
        let mut concrete_iterators: Vec<TableIter<T>> = Vec::new();
        for table_reader_arc in &all_tables_for_merge {
            // Iterate over references (&)
            concrete_iterators.push(table_reader_arc.to_iter()); // Borrow happens here
        }

        // Create a vector of mutable trait object references from the concrete iterators.
        // concrete_iterators owns the TableIter instances.
        let iterators_for_agg: Vec<&mut dyn Iterator<Item = KVOpertion>> = concrete_iterators
            .iter_mut()
            .map(|it| it as &mut dyn Iterator<Item = KVOpertion>)
            .collect();

        let mut kv_iter_agg = KViterAgg::new(iterators_for_agg);
        let mut compacted_tables = Vec::new();

        if let Some(mut op) = kv_iter_agg.next() {
            let mut table_builder =
                TableBuilder::new_with_id_config(*store_id_start, self.config.table_config);
            *store_id_start += 1;

            loop {
                // If add fails, current builder is full. Flush and create new builder.
                if !table_builder.add(op.clone()) {
                    let new_table_reader = table_builder.flush();
                    table_change.push(TableChange {
                        id: new_table_reader.store_id(),
                        index: 0, // Index will be sorted later
                        level: target_level,
                        change_type: ChangeType::Add,
                    });
                    compacted_tables.push(Arc::new(new_table_reader));

                    table_builder =
                        TableBuilder::new_with_id_config(*store_id_start, self.config.table_config);
                    *store_id_start += 1;
                    if !table_builder.add(op.clone()) {
                        panic!("Error: Single KVOperation is too large to fit in a new table block during compaction. Operation ID: {}", op.id);
                    }
                }

                if let Some(next_op) = kv_iter_agg.next() {
                    op = next_op;
                } else {
                    // No more operations, flush the last builder
                    if !table_builder.is_empty() {
                        let new_table_reader = table_builder.flush();
                        table_change.push(TableChange {
                            id: new_table_reader.store_id(),
                            index: 0, // Index will be sorted later
                            level: target_level,
                            change_type: ChangeType::Add,
                        });
                        compacted_tables.push(Arc::new(new_table_reader));
                    }
                    break;
                }
            }
        }

        // 5. Merge pass_through_input_tables, compacted_tables, and remaining tables in target_level (tables_to_pass_through_target)
        let mut final_tables_for_target_level = Vec::new();
        for table in &pass_through_input_tables {
            table_change.push(TableChange {
                level: target_level,
                index: 0,
                id: table.store_id(),
                change_type: ChangeType::Add,
            });
        }
        final_tables_for_target_level.extend(pass_through_input_tables);
        final_tables_for_target_level.extend(compacted_tables);
        final_tables_for_target_level.extend(tables_to_pass_through_target);

        // Sort all tables in the target level by their first key
        final_tables_for_target_level.sort_by(|a, b| a.key_range().0.cmp(&b.key_range().0));

        // Update the sstables for the target level
        self.levels[target_level].sstables = final_tables_for_target_level;

        // Re-index TableChange.Add entries after sorting
        let mut current_level_tables_ids: HashMap<u64, usize> = HashMap::new();
        for (idx, table) in self.levels[target_level].sstables.iter().enumerate() {
            current_level_tables_ids.insert(table.store_id(), idx);
        }

        for change in table_change.iter_mut() {
            if change.change_type == ChangeType::Add && change.level == target_level {
                if let Some(&new_index) = current_level_tables_ids.get(&change.id) {
                    change.index = new_index;
                } else {
                    // This should not happen if logic is correct
                    panic!(
                        "Added table ID not found in final target level after sort: {}",
                        change.id
                    );
                }
            }
        }

        for change in &table_change {
            debug!(
                level = change.level,
                index = change.index,
                id = change.id,
                change_type = ?change.change_type,
                "compact_level table change"
            );
        }
        table_change
    }

    // find all key_range overlap with k, return index in key_ranges vec
    // sorted_key_ranges order by first_key
    // e.g. k:[2,10] key_ranges:[[1,3],[5,7],[12,20]] return (0,1)
    fn key_range_overlap(
        k: (KeyVec, KeyVec),
        sorted_key_ranges: &Vec<(KeyVec, KeyVec)>,
    ) -> Vec<usize> {
        let (k_min, k_max) = k;
        let mut overlapping_indices = Vec::new();

        for (idx, (r_min, r_max)) in sorted_key_ranges.into_iter().enumerate() {
            // Check for overlap: (k_min <= r_max) AND (k_max >= r_min)
            if k_min.as_ref().le(r_max.as_ref()) && k_max.as_ref().ge(r_min.as_ref()) {
                overlapping_indices.push(idx);
            } else if r_min.as_ref().gt(k_max.as_ref()) {
                // Since sorted_key_ranges is sorted by first_key,
                // if the current range's min_key is already greater than k_max,
                // no subsequent ranges will overlap.
                break;
            }
        }
        overlapping_indices
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
        let max_num = self.max_table_in_level(level_depth); // Use instance method
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
        let mut store_id_positions: Vec<(u64, usize)> = level // Change String to u64
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
            .take(self.config.max_input_table_num_in_compact)
            .map(|(_, pos)| pos)
            .collect();

        // Sort positions in descending order to remove from the end first
        positions_to_remove.sort_by(|a, b| b.cmp(a));

        let mut table_change = Vec::new();
        for pos in positions_to_remove {
            let table = level.sstables.remove(pos);
            table_change.push(TableChange {
                id: table.store_id(), // Convert u64 to String
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
    use super::LevelStoregeConfig;
    use std::ops::Range;
    use std::sync::Arc; // Moved from test functions
    const DEFAULT_MAX_LEVEL_ZERO_TABLE_SIZE: usize = 4; // Moved here for local test scope

    use crc32fast::Hasher;

    use crate::db::common::{KVOpertion, OpId, OpType}; // OpType moved from helper
    use crate::db::key::{KeySlice, KeyVec};
    use crate::db::level::{ChangeType, Level, LevelStorege, TableChange, U32_SIZE};
    use crate::db::store::{Filestore, Store, StoreId};
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
                OpType::Write(value_transform(i).to_string().as_bytes().into()),
            );
            v.push(tmp);
        }
        let mut table = TableBuilder::new_with_id(0);
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
        let level_storage =
            super::LevelStorege::new(vec![level0, level1], LevelStoregeConfig::config_for_test()); // r is level_ratio, not relevant for this test

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
    fn create_test_table_with_id(range: Range<usize>, id: u64) -> TableReader<Memstore> {
        // Change id type to u64
        let mut v = Vec::new();
        for i in range.clone() {
            let tmp = KVOpertion::new(
                i as u64,
                crate::db::block::test::pad_zero(i as u64).as_bytes().into(),
                OpType::Write(i.to_string().as_bytes().into()),
            );
            v.push(tmp);
        }
        let mut table = TableBuilder::new_with_id(id);
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
        let level_storage_zero =
            super::LevelStorege::new(vec![level_zero], LevelStoregeConfig::config_for_test());
        let compacted_tables_zero = level_storage_zero.table_to_compact(0);
        assert_eq!(compacted_tables_zero.len(), 3);
        let mut compacted_ids: Vec<u64> = compacted_tables_zero
            .iter()
            .map(|table| table.store_id())
            .collect();
        compacted_ids.sort(); // Sort to ensure consistent order for comparison

        let mut expected_ids = vec![table_a.store_id(), table_b.store_id(), table_c.store_id()];
        expected_ids.sort(); // Sort expected IDs as well

        assert_eq!(compacted_ids, expected_ids);

        // Case 2: Non-Level Zero (is_level_zero = false)
        // Create tables with specific IDs to control the min_by_key behavior
        let table_x = Arc::new(create_test_table_with_id(0..10, 1u64)); // Pass u64
        let table_y = Arc::new(create_test_table_with_id(10..20, 0u64)); // Pass u64
        let table_z = Arc::new(create_test_table_with_id(20..30, 2u64)); // Pass u64

        let level_n = super::Level::new(
            vec![table_x.clone(), table_y.clone(), table_z.clone()],
            false,
        );
        let level_storage_n =
            super::LevelStorege::new(vec![level_n], LevelStoregeConfig::config_for_test());

        let compacted_tables_n = level_storage_n.table_to_compact(0);
        assert_eq!(compacted_tables_n.len(), 1);
        assert_eq!(compacted_tables_n[0].store_id(), 0u64); // Compare u64 with u64
        assert_eq!(compacted_tables_n[0].key_range(), table_y.key_range()); // Ensure it's table_y
    }

    #[test]
    fn test_get_key_range() {
        // Create some test tables with known key ranges
        let table_1 = Arc::new(create_test_table_with_id_offset(0..10, 0)); // "000000" to "000009"
        let table_2 = Arc::new(create_test_table_with_id_offset(20..30, 0)); // "000020" to "000029"
        let table_3 = Arc::new(create_test_table_with_id_offset(15..25, 0)); // "000015" to "000024"

        // Scenario 1: Single table
        let level_storage_single =
            super::LevelStorege::new(vec![], LevelStoregeConfig::config_for_test()); // Levels vector doesn't matter for this test
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
            20000,
            |i| format!("new_val_{}", i),
        )); // keys "000050" to "000149"

        // Level 0: tables ordered newest to oldest to match find logic
        let level0 = super::Level::new(vec![table_l0_b.clone(), table_l0_a.clone()], true);

        // Create tables for Level 1 (non-overlapping) with distinct opid offsets
        let table_l1_x = Arc::new(create_test_table_with_id_offset(100..200, 0)); // keys "000100" to "000199", OpIds 100-199
        let table_l1_y = Arc::new(create_test_table_with_id_offset(200..300, 100)); // keys "000200" to "000299", OpIds 300-399
        let level1 = super::Level::new(vec![table_l1_x.clone(), table_l1_y.clone()], false);

        // Create LevelStorege with levels
        let level_storage = super::LevelStorege::new(
            vec![level0.clone(), level1],
            LevelStoregeConfig::config_for_test(),
        );

        // Input tables for compaction (from level 0)
        let input_tables_for_compact = vec![table_l0_b.clone(), table_l0_a.clone()];
        let mut store_id_start = 10000; // Starting ID for new SSTables

        // Create a mutable copy of level_storage to perform compaction
        let mut level_storage_mut = level_storage;

        // Perform compaction
        let table_changes =
            level_storage_mut.compact_level(&mut store_id_start, input_tables_for_compact, 0);

        // Get the compacted level (level 1)
        let compacted_level = &level_storage_mut.levels[1];

        // Find the maximum store ID in the compacted level and assert next_id
        let max_store_id_in_compacted_level = compacted_level
            .sstables
            .iter()
            .map(|table| table.store_id()) // store_id() already returns u64
            .max()
            .unwrap_or(0); // Default to 0 if no tables, though we assert !is_empty() below

        assert_eq!(
            store_id_start,
            max_store_id_in_compacted_level + 1,
            "store_id_start should be max store id in compacted_level + 1"
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
        let mut level_storage_no_overlap = super::LevelStorege::new(
            vec![level0.clone(), empty_level1],
            LevelStoregeConfig::config_for_test(),
        );
        let input_tables_no_overlap = vec![table_l0_b.clone(), table_l0_a.clone()];
        let mut store_id_start_no_overlap = store_id_start; // Use a new mutable variable for this test case
        let _table_changes_no_overlap = level_storage_no_overlap.compact_level(
            &mut store_id_start_no_overlap,
            input_tables_no_overlap,
            0,
        );

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

        let level_storage =
            super::LevelStorege::new(vec![level_n], LevelStoregeConfig::config_for_test()); // Level 0 is the only level here

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
        let empty_level_storage =
            super::LevelStorege::new(vec![empty_level], LevelStoregeConfig::config_for_test());
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
        let single_table_level_storage = super::LevelStorege::new(
            vec![single_table_level],
            LevelStoregeConfig::config_for_test(),
        );
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
        let table_a = Arc::new(create_test_table_with_id(0..10, 3u64));
        let table_b = Arc::new(create_test_table_with_id(10..20, 1u64)); // Should be first when sorted
        let table_c = Arc::new(create_test_table_with_id(20..30, 2u64)); // Should be second when sorted
        let table_d = Arc::new(create_test_table_with_id(30..40, 4u64));

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
        let mut config = LevelStoregeConfig::config_for_test();
        config.level_zero_num_limit = 4;
        let mut level_storage = super::LevelStorege::new(vec![level0], config);

        // Level 0 limit is MAX_LEVEL_ZERO_TABLE_SIZE = 4, so with 4 tables, no compaction needed
        let (tables_to_compact, changes) = level_storage.take_out_table_to_compact(0);
        assert_eq!(tables_to_compact.len(), 0);
        assert_eq!(changes.len(), 0); // No changes expected
        assert_eq!(level_storage.levels[0].sstables.len(), 4); // No tables removed

        // Add one more table to exceed the limit
        let table_e = Arc::new(create_test_table_with_id(40..50, 0u64)); // Smallest ID
        level_storage.levels[0].sstables.push(table_e.clone());

        // Now we have 5 tables, should take out 1 (5 - 4 = 1)
        let (tables_to_compact, changes) = level_storage.take_out_table_to_compact(0);
        assert_eq!(tables_to_compact.len(), 1);
        // Should get table with smallest store_id
        assert_eq!(tables_to_compact[0].store_id(), 0);
        // Verify table_change
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].id, 0u64);
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
            LevelStoregeConfig::config_for_test(),
        );

        // Level 1 limit is level_zero_num_limit * level_ratio^1 = 2 * 10^1 = 20
        // With only 3 tables, no compaction needed
        let (tables_to_compact2, changes2) = level_storage2.take_out_table_to_compact(1);
        assert_eq!(tables_to_compact2.len(), 0);
        assert_eq!(changes2.len(), 0); // No changes expected
        assert_eq!(level_storage2.levels[1].sstables.len(), 3); // No tables removed

        // Test Case 3: Empty level
        let empty_level: Level<Memstore> = super::Level::new(vec![], false);
        let mut empty_level_storage =
            super::LevelStorege::new(vec![empty_level], LevelStoregeConfig::config_for_test());
        let (tables_to_compact3, changes3) = empty_level_storage.take_out_table_to_compact(0);

        assert_eq!(tables_to_compact3.len(), 0);
        assert_eq!(changes3.len(), 0); // No changes expected
        assert_eq!(empty_level_storage.levels[0].sstables.len(), 0);

        // Test Case 4: Level with many tables, check limit calculation
        let num_tables = 25; // Use a number clearly exceeding the limit
        let many_tables: Vec<Arc<TableReader<Memstore>>> = (0..num_tables) // Create 25 tables
            .map(|i| {
                Arc::new(create_test_table_with_id(
                    i * 10..(i * 10 + 10),
                    i as u64, // Pass u64 directly
                ))
            })
            .collect();

        let level_many = super::Level::new(many_tables, false);
        let mut level_storage_many = super::LevelStorege::new(
            vec![
                super::Level::new(vec![], true), // Empty level 0
                level_many,
            ],
            LevelStoregeConfig::config_for_test(),
        );

        // Level 1 limit is 2 * 10^1 = 20.
        // With 25 tables, should take out 1
        let (tables_to_compact4, changes4) = level_storage_many.take_out_table_to_compact(1);
        assert_eq!(
            tables_to_compact4.len(),
            config.max_input_table_num_in_compact
        ); // Expect 1 tables to be compacted
        assert_eq!(
            level_storage_many.levels[1].sstables.len(),
            num_tables - config.max_input_table_num_in_compact
        ); // Expect 20 tables remaining

        let mut compacted_ids: Vec<u64> = tables_to_compact4
            .iter()
            .map(|table| table.store_id())
            .collect();
        compacted_ids.sort();
        assert_eq!(
            compacted_ids,
            vec![0] // Expect IDs 0
        );
        // Verify table_change for multiple deletions
        assert_eq!(changes4.len(), 1); // Expect 5 changes
        let mut changed_ids: Vec<u64> = changes4.iter().map(|c| c.id).collect();
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
        let max_origin_id = 5;
        let table_a = Arc::new(create_test_table_with_id(0..10, 1u64)); // Smallest ID
        let table_b = Arc::new(create_test_table_with_id(10..20, 2u64));
        let table_c = Arc::new(create_test_table_with_id(20..30, 3u64));
        let table_d = Arc::new(create_test_table_with_id(30..40, 4u64));
        let table_e = Arc::new(create_test_table_with_id(40..50, max_origin_id)); // This will exceed limit

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
        let mut level_storage =
            super::LevelStorege::new(vec![level0], LevelStoregeConfig::config_for_test());

        // Before compaction: Level 0 has 5 tables, no Level 1
        assert_eq!(level_storage.levels.len(), 1);
        assert_eq!(level_storage.levels[0].sstables.len(), 5);

        // Perform compaction
        let mut start_id_case1 = 1000;
        let original_start_id = start_id_case1;
        let table_changes = level_storage.compact_storage(&mut start_id_case1);

        // Find the maximum store ID among the newly added tables in level 1
        let max_new_id_case1 = table_changes
            .iter()
            .filter(|tc| tc.change_type == ChangeType::Add && tc.level == 1)
            .map(|tc| tc.id)
            .max()
            .expect("Should have added tables in level 1");

        assert!(max_new_id_case1 <= max_origin_id, "id is from level0 table");

        // After compaction: Level 0 should have 4 tables, Level 1 should exist with compacted table
        assert_eq!(level_storage.levels.len(), 2); // Level 1 should be created
        assert_eq!(level_storage.levels[0].sstables.len(), 4); // Level 0 reduced to limit
        assert!(!level_storage.levels[1].sstables.is_empty()); // Level 1 has compacted data

        // Verify that the table with smallest ID was moved to Level 1
        let remaining_ids: Vec<u64> = level_storage.levels[0]
            .sstables
            .iter()
            .map(|table| table.store_id()) // Convert u64 to String
            .collect();
        assert!(!remaining_ids.contains(&table_a.store_id())); // Smallest ID (1) should be compacted

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
        assert_eq!(delete_changes[0].id, table_a.store_id()); // ID 1
        assert_eq!(delete_changes[0].change_type, ChangeType::Delete);

        // Expect at least one add to level 1 (the compacted table)
        assert!(!add_changes.is_empty());
        for add_change in add_changes {
            assert_eq!(add_change.level, 1);
            assert_eq!(add_change.change_type, ChangeType::Add);
            assert!(add_change.id <= max_origin_id); // Should use the provided store_id_start
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

        // Test Case 2: Level 1 exceeds limit, should compact to Level 2
        let num_tables_l1 = 25; // Create 25 tables to exceed limit
        let many_tables_l1: Vec<Arc<TableReader<Memstore>>> = (0..num_tables_l1)
            .map(|i| {
                Arc::new(create_test_table_with_id(
                    i * 10..(i * 10 + 10),
                    (1000 + i) as u64, // Assign unique u64 IDs for L1 tables
                ))
            })
            .collect();
        let max_id_in_l1 = many_tables_l1
            .iter()
            .map(|table| table.store_id())
            .max()
            .expect("many_tables_l1 should not be empty");

        let level1_many = super::Level::new(many_tables_l1, false);
        let mut level_storage_multi = super::LevelStorege::new(
            vec![
                super::Level::new(vec![], true), // Empty level 0
                level1_many,
            ],
            LevelStoregeConfig::config_for_test(),
        );

        // Before compaction: Level 1 has 25 tables. Limit is 2 * 10^1 = 20.
        assert_eq!(level_storage_multi.levels[1].sstables.len(), 25);

        // Perform compaction
        let mut start_id_case2 = 2000;
        let table_changes_case2 = level_storage_multi.compact_storage(&mut start_id_case2);

        // Find the maximum store ID among the newly added tables in level 2
        let max_new_id_case2 = table_changes_case2
            .iter()
            .filter(|tc| tc.change_type == ChangeType::Add && tc.level == 2)
            .map(|tc| tc.id)
            .max()
            .expect("Should have added tables in level 2");

        assert!(max_new_id_case2 < max_id_in_l1);

        // After compaction: Level 1 should have 20 tables (limit). Level 2 should be created.
        // 5 tables (25 - 20) should be compacted from L1 to L2.
        assert_eq!(level_storage_multi.levels.len(), 3); // Level 2 should be created
        assert_eq!(level_storage_multi.levels[1].sstables.len(), 24); // Level 1 reduced to limit
        assert!(!level_storage_multi.levels[2].sstables.is_empty()); // Level 2 has compacted data

        // Test Case 3: No compaction needed (all levels within limits)
        let small_table = Arc::new(create_test_table_with_id(0..10, 999u64));
        let level0_small = super::Level::new(vec![small_table], true);
        let mut level_storage_small =
            super::LevelStorege::new(vec![level0_small], LevelStoregeConfig::config_for_test());

        let original_len = level_storage_small.levels.len();
        let original_table_count = level_storage_small.levels[0].sstables.len();

        // Perform compaction
        let mut start_id_case3 = 3000;
        let table_changes_case3 = level_storage_small.compact_storage(&mut start_id_case3);

        // Assert next_id is unchanged if no tables were created
        assert_eq!(
            start_id_case3, start_id_case3,
            "start_id_case3 should be unchanged if no compaction happened"
        );
        assert!(
            table_changes_case3.is_empty(),
            "No table changes expected if no compaction happened"
        );

        // Should remain unchanged
        assert_eq!(level_storage_small.levels.len(), original_len);
        assert_eq!(
            level_storage_small.levels[0].sstables.len(),
            original_table_count
        );

        // Test Case 4: Empty level storage
        let mut empty_level_storage: LevelStorege<Memstore> =
            super::LevelStorege::new(vec![], LevelStoregeConfig::config_for_test());

        // Should not panic and remain empty
        let mut start_id_case4 = 4000;
        let table_changes_case4 = empty_level_storage.compact_storage(&mut start_id_case4);
        assert_eq!(
            start_id_case4, start_id_case4,
            "start_id_case4 should be unchanged for empty storage"
        );
        assert!(
            table_changes_case4.is_empty(),
            "No table changes expected for empty storage"
        );

        assert_eq!(empty_level_storage.levels.len(), 0);
    }

    #[test]
    fn test_table_change_encode_decode() {
        let original_change = super::TableChange {
            level: 5,
            index: 10,
            id: 12345,
            change_type: super::ChangeType::Add,
        };

        let mut encoded_data = Vec::new();
        original_change.encode(&mut encoded_data);

        let decoded_change = super::TableChange::decode(encoded_data.as_slice());

        assert_eq!(original_change.level, decoded_change.level);
        assert_eq!(original_change.index, decoded_change.index);
        assert_eq!(original_change.id, decoded_change.id);
        assert_eq!(original_change.change_type, decoded_change.change_type);

        let original_change_delete = super::TableChange {
            level: 1,
            index: 0,
            id: 67890,
            change_type: super::ChangeType::Delete,
        };

        let mut encoded_data_delete = Vec::new();
        original_change_delete.encode(&mut encoded_data_delete);

        let decoded_change_delete = super::TableChange::decode(encoded_data_delete.as_slice());

        assert_eq!(original_change_delete.level, decoded_change_delete.level);
        assert_eq!(original_change_delete.index, decoded_change_delete.index);
        assert_eq!(original_change_delete.id, decoded_change_delete.id);
        assert_eq!(
            original_change_delete.change_type,
            decoded_change_delete.change_type
        );
    }

    #[test]
    fn test_max_table_in_level() {
        // Create LevelStorege instances with specific limits
        let mut config = LevelStoregeConfig::config_for_test();
        config.level_zero_num_limit = 4;
        let level_storage_ratio2 = LevelStorege::<Memstore>::new(vec![], config); // level_zero_limit=4, ratio=2
        config.level_ratio = 3;
        config.level_zero_num_limit = 4;
        let level_storage_ratio3 = LevelStorege::<Memstore>::new(vec![], config); // level_zero_limit=4, ratio=3
        config.level_zero_num_limit = 2;
        config.level_ratio = 10;
        let level_storage_limit2_ratio10 = LevelStorege::<Memstore>::new(vec![], config); // level_zero_limit=2, ratio=10

        // Test level 0
        assert_eq!(level_storage_ratio2.max_table_in_level(0), 4);
        assert_eq!(level_storage_ratio3.max_table_in_level(0), 4);
        assert_eq!(level_storage_limit2_ratio10.max_table_in_level(0), 2);

        // Test level 1 with ratio 2, limit 4
        // Expected: 4 * 2^1 = 8
        assert_eq!(level_storage_ratio2.max_table_in_level(1), 8);

        // Test level 2 with ratio 2, limit 4
        // Expected: 4 * 2^2 = 16
        assert_eq!(level_storage_ratio2.max_table_in_level(2), 16);

        // Test level 1 with ratio 3, limit 4
        // Expected: 4 * 3^1 = 12
        assert_eq!(level_storage_ratio3.max_table_in_level(1), 12);

        // Test level 2 with ratio 3, limit 4
        // Expected: 4 * 3^2 = 36
        assert_eq!(level_storage_ratio3.max_table_in_level(2), 36);

        // Test level 1 with ratio 10, limit 2
        // Expected: 2 * 10^1 = 20
        assert_eq!(level_storage_limit2_ratio10.max_table_in_level(1), 20);

        // Test level 2 with ratio 10, limit 2
        // Expected: 2 * 10^2 = 200
        assert_eq!(level_storage_limit2_ratio10.max_table_in_level(2), 200);
    }

    #[test]
    fn test_compact_level_table_ordering() {
        // Test that tables in a level are ordered by their first key after compaction

        // Create tables with non-sequential key ranges to test ordering
        let table_c = Arc::new(create_test_table_with_id(200..300, 200u64)); // keys "000200" to "000299"
        let table_a = Arc::new(create_test_table_with_id(0..100, 0u64)); // keys "000000" to "000099"
        let table_b = Arc::new(create_test_table_with_id(100..200, 100u64)); // keys "000100" to "000199"

        // Create level 0 with tables in non-sorted order
        let level0 = super::Level::new(
            vec![table_c.clone(), table_a.clone(), table_b.clone()],
            true,
        );

        // Create empty level 1 as target
        let empty_level1 = super::Level::new(vec![], false);
        let mut level_storage = super::LevelStorege::new(
            vec![level0, empty_level1],
            LevelStoregeConfig::config_for_test(),
        );

        // Perform compaction from level 0 to level 1
        let input_tables = vec![table_c.clone(), table_a.clone(), table_b.clone()];
        let mut store_id_start_for_test = 1000; // Define a mutable variable for the store_id
        let _table_changes =
            level_storage.compact_level(&mut store_id_start_for_test, input_tables, 0);

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
    fn test_table_change_log_get_all_changes() {
        use std::fs::OpenOptions;
        use tempfile::NamedTempFile;

        use tempfile::tempdir;
        let store_id = 999;
        // Create a temporary directory
        let tmp_dir = tempdir().expect("Failed to create temp directory");
        // Get a file path in the temporary directory and record it
        let path = tmp_dir.path().join(format!("{}.data", store_id));
        // Open file by path and pass it to log
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .expect("Failed to open file in temp dir");
        let mut log = super::TableChangeLog::<Filestore>::from_file(file, store_id);

        // Case 1: Empty log
        let empty_tmp_dir = tempdir().expect("Failed to create temp directory for empty log");
        let empty_path = empty_tmp_dir.path().join(format!("{}.data", store_id + 1));
        let empty_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&empty_path)
            .expect("Failed to open empty file in temp dir");
        let empty_log = super::TableChangeLog::<Filestore>::from_file(empty_file, store_id + 1);
        assert!(empty_log.get_all_changes().unwrap().is_empty());

        // Case 2: Log with one change batch
        let change1 = TableChange {
            level: 0,
            index: 0,
            id: 100,
            change_type: ChangeType::Add,
        };
        log.append(vec![change1.clone()]);
        // To read from the same log instance, we need to re-open the file or ensure the Filestore's internal cursor is reset.
        // Since Filestore is append-only and doesn't reset its internal cursor for reads,
        // we'll simulate re-opening the log from the file for each read operation to get all changes from the beginning.
        // This is consistent with how a log would be read from disk.
        let file_for_read1 = OpenOptions::new()
            .read(true)
            .open(&path)
            .expect("Failed to open file for read");
        let retrieved_changes_batch1 =
            super::TableChangeLog::<Filestore>::from_file(file_for_read1, store_id)
                .get_all_changes()
                .unwrap();
        assert_eq!(retrieved_changes_batch1.len(), 1);
        assert_eq!(retrieved_changes_batch1[0].id, 100);
        assert_eq!(retrieved_changes_batch1[0].level, 0);
        assert_eq!(retrieved_changes_batch1[0].index, 0);
        assert_eq!(retrieved_changes_batch1[0].change_type, ChangeType::Add);

        // Case 3: Log with multiple change batches
        let change2 = TableChange {
            level: 1,
            index: 5,
            id: 200,
            change_type: ChangeType::Delete,
        };
        let change3 = TableChange {
            level: 0,
            index: 1,
            id: 101,
            change_type: ChangeType::Add,
        };
        log.append(vec![change2.clone(), change3.clone()]);

        // Re-create log from same store_id to simulate opening an existing log
        let file_for_read2 = OpenOptions::new()
            .read(true)
            .open(&path)
            .expect("Failed to open file for read");
        let log_reopened = super::TableChangeLog::<Filestore>::from_file(file_for_read2, store_id);
        let retrieved_changes_reopened = log_reopened.get_all_changes().unwrap();

        assert_eq!(retrieved_changes_reopened.len(), 3); // Total changes: 1 from first batch + 2 from second batch
        assert_eq!(retrieved_changes_reopened[0].id, 100);
        assert_eq!(retrieved_changes_reopened[1].id, 200);
        assert_eq!(retrieved_changes_reopened[2].id, 101);

        // Verify details of the second batch
        assert_eq!(retrieved_changes_reopened[1].level, 1);
        assert_eq!(retrieved_changes_reopened[1].index, 5);
        assert_eq!(
            retrieved_changes_reopened[1].change_type,
            ChangeType::Delete
        );

        assert_eq!(retrieved_changes_reopened[2].level, 0);
        assert_eq!(retrieved_changes_reopened[2].index, 1);
        assert_eq!(retrieved_changes_reopened[2].change_type, ChangeType::Add);

        // Case 4: Test with a batch of zero changes (should be handled gracefully)
        log.append(vec![]);
        let file_for_read3 = OpenOptions::new()
            .read(true)
            .open(&path)
            .expect("Failed to open file for read");
        let log_reopened_empty_batch =
            super::TableChangeLog::<Filestore>::from_file(file_for_read3, store_id);
        let retrieved_changes_empty_batch = log_reopened_empty_batch.get_all_changes().unwrap();
        assert_eq!(retrieved_changes_empty_batch.len(), 3); // Should still be 3, as empty batch adds no changes
    }

    /// Tests the `need_compact` method to ensure it correctly identifies when compaction is needed.
    #[test]
    fn test_need_compact() {
        // Setup: Use a test config for LevelStorege
        let config = LevelStoregeConfig::config_for_test(); // level_zero_num_limit = 2, level_ratio = 2

        // Case 1: Empty storage - no compaction needed
        let empty_storage = LevelStorege::<Memstore>::new(vec![], config);
        assert!(
            !empty_storage.need_compact(),
            "Empty storage should not need compaction"
        );

        // Case 2: Level 0 below limit (1 table, limit is 2)
        let table_a = Arc::new(create_test_table_with_id(0..10, 1u64));
        let level0_one_table = Level::new(vec![table_a.clone()], true);
        let storage_one_table = LevelStorege::new(vec![level0_one_table], config);
        assert!(
            !storage_one_table.need_compact(),
            "Level 0 with 1 table should not need compaction"
        );

        // Case 3: Level 0 at limit (2 tables, limit is 2)
        let table_b = Arc::new(create_test_table_with_id(10..20, 2u64));
        let level0_at_limit = Level::new(vec![table_a.clone(), table_b.clone()], true);
        let storage_at_limit = LevelStorege::new(vec![level0_at_limit], config);
        assert!(
            !storage_at_limit.need_compact(),
            "Level 0 with 2 tables should not need compaction"
        );

        // Case 4: Level 0 exceeds limit (3 tables, limit is 2) - should need compaction
        let table_c = Arc::new(create_test_table_with_id(20..30, 3u64));
        let level0_exceeds_limit = Level::new(vec![table_a, table_b, table_c], true);
        let storage_exceeds_limit = LevelStorege::new(vec![level0_exceeds_limit], config);
        assert!(
            storage_exceeds_limit.need_compact(),
            "Level 0 with 3 tables should need compaction"
        );

        // Case 5: Multiple levels, one exceeds limit
        // Level 0 (1 table, limit 2) - OK
        // Level 1 (limit: 2 * 2^1 = 4)
        // Level 1 with 5 tables - needs compaction
        let table_d = Arc::new(create_test_table_with_id(30..40, 4u64));
        let table_e = Arc::new(create_test_table_with_id(40..50, 5u64));
        let table_f = Arc::new(create_test_table_with_id(50..60, 6u64));
        let table_g = Arc::new(create_test_table_with_id(60..70, 7u64));
        let table_h = Arc::new(create_test_table_with_id(70..80, 8u64)); // 5th table for L1

        let level0_ok = Level::new(vec![table_d.clone()], true);
        let level1_exceeds_limit =
            Level::new(vec![table_d, table_e, table_f, table_g, table_h], false);
        let storage_multi_level_needs_compact =
            LevelStorege::new(vec![level0_ok, level1_exceeds_limit], config);
        assert!(
            storage_multi_level_needs_compact.need_compact(),
            "Multi-level storage with L1 exceeding limit should need compaction"
        );

        // Case 6: Multiple levels, all within limits
        // Level 0 (1 table, limit 2) - OK
        // Level 1 (limit: 4) with 3 tables - OK
        let table_i = Arc::new(create_test_table_with_id(80..90, 9u64));
        let table_j = Arc::new(create_test_table_with_id(90..100, 10u64));
        let table_k = Arc::new(create_test_table_with_id(100..110, 11u64));

        let level0_ok_2 = Level::new(vec![table_i.clone()], true);
        let level1_ok = Level::new(vec![table_i, table_j, table_k], false);
        let storage_multi_level_ok = LevelStorege::new(vec![level0_ok_2, level1_ok], config);
        assert!(
            !storage_multi_level_ok.need_compact(),
            "Multi-level storage with all levels within limits should not need compaction"
        );

        // Case 7: Level 0 does not exist, but subsequent levels might.
        // `levels` vector might be `[empty_level1, empty_level2]` if they were initialized.
        // The current implementation ensures `levels.len()` means the highest depth.
        let storage_no_level0 = LevelStorege::<Memstore>::new(
            vec![
                Level::new(vec![], true), // Level 0 exists but is empty
                Level::new(
                    vec![Arc::new(create_test_table_with_id(0..10, 100u64))],
                    false,
                ), // Level 1 exists, 1 table, limit 4
            ],
            config,
        );
        assert!(
            !storage_no_level0.need_compact(),
            "Storage with empty L0 and valid L1 should not need compaction"
        );
    }

    #[test]
    fn test_table_change_log_corrupted_checksum() {
        let store_id = 1000;
        let mut memstore = Memstore::open(store_id);

        // Manually create a valid TableChange and its encoded bytes
        let change = TableChange {
            level: 0,
            index: 0,
            id: 1,
            change_type: ChangeType::Add,
        };
        let mut change_buffer = Vec::new();
        change.encode(&mut change_buffer); // This is 25 bytes

        // Calculate correct checksum
        let mut hasher = Hasher::new();
        hasher.update(&change_buffer);
        let correct_checksum = hasher.finalize();

        // Prepare the full batch bytes: count + change_data + checksum
        let mut batch_bytes = Vec::new();
        batch_bytes.extend_from_slice(&(1u64).to_le_bytes()); // num_changes_in_batch = 1
        batch_bytes.extend_from_slice(&change_buffer);
        batch_bytes.extend_from_slice(&correct_checksum.to_le_bytes());

        // Corrupt the checksum byte (e.g., flip a bit in the last byte of the checksum)
        let checksum_start_index = batch_bytes.len() - U32_SIZE; // U32_SIZE is 4
        batch_bytes[checksum_start_index] = batch_bytes[checksum_start_index].wrapping_add(1); // Corrupt one byte

        // Append the corrupted bytes to the memstore
        memstore.append(&batch_bytes);

        // Create a TableChangeLog from the corrupted memstore
        let corrupted_log = super::TableChangeLog::new_with_store(memstore);

        // Attempt to get changes, expecting an error due to checksum mismatch
        let result = corrupted_log.get_all_changes();
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "decode table change checksum"
        );
    }

    #[test]
    fn test_key_range_overlap() {
        let to_kv_vec = |s: &str| KeyVec::from(s.as_bytes());

        // Helper for creating range tuples
        let r = |start: &str, end: &str| (to_kv_vec(start), to_kv_vec(end));

        // Test Case 1: No overlap, target range before sorted_key_ranges
        let k = r("000000", "000005");
        let ranges = vec![r("000010", "000020"), r("000030", "000040")];
        assert_eq!(
            super::LevelStorege::<Memstore>::key_range_overlap(k, &ranges),
            vec![]
        );

        // Test Case 2: No overlap, target range after sorted_key_ranges
        let k = r("000050", "000060");
        let ranges = vec![r("000010", "000020"), r("000030", "000040")];
        assert_eq!(
            super::LevelStorege::<Memstore>::key_range_overlap(k, &ranges),
            vec![]
        );

        // Test Case 3: Exact overlap with one range
        let k = r("000010", "000020");
        let ranges = vec![r("000010", "000020"), r("000030", "000040")];
        assert_eq!(
            super::LevelStorege::<Memstore>::key_range_overlap(k, &ranges),
            vec![0]
        );

        // Test Case 4: Partial overlap at the beginning of a range
        let k = r("000005", "000015");
        let ranges = vec![r("000010", "000020"), r("000030", "000040")];
        assert_eq!(
            super::LevelStorege::<Memstore>::key_range_overlap(k, &ranges),
            vec![0]
        );

        // Test Case 5: Partial overlap at the end of a range
        let k = r("000015", "000025");
        let ranges = vec![r("000010", "000020"), r("000030", "000040")];
        assert_eq!(
            super::LevelStorege::<Memstore>::key_range_overlap(k, &ranges),
            vec![0]
        );

        // Test Case 6: Overlap with multiple consecutive ranges
        let k = r("000015", "000035");
        let ranges = vec![
            r("000010", "000020"),
            r("000025", "000035"),
            r("000040", "000050"),
        ];
        assert_eq!(
            super::LevelStorege::<Memstore>::key_range_overlap(k, &ranges),
            vec![0, 1]
        );

        // Test Case 7: Range completely contains another range
        let k = r("000000", "000050");
        let ranges = vec![r("000010", "000020"), r("000030", "000040")];
        assert_eq!(
            super::LevelStorege::<Memstore>::key_range_overlap(k, &ranges),
            vec![0, 1]
        );

        // Test Case 8: Range contained within another range
        let k = r("000012", "000018");
        let ranges = vec![r("000010", "000020"), r("000030", "000040")];
        assert_eq!(
            super::LevelStorege::<Memstore>::key_range_overlap(k, &ranges),
            vec![0]
        );

        // Test Case 9: Empty sorted_key_ranges
        let k = r("000010", "000020");
        let ranges: Vec<(KeyVec, KeyVec)> = vec![];
        assert_eq!(
            super::LevelStorege::<Memstore>::key_range_overlap(k, &ranges),
            vec![]
        );

        // Test Case 10: Overlap at boundary points with an empty space between
        let k = r("000020", "000030");
        let ranges = vec![r("000010", "000020"), r("000030", "000040")];
        assert_eq!(
            super::LevelStorege::<Memstore>::key_range_overlap(k, &ranges),
            vec![0, 1]
        );

        // Test Case 11: Single range in sorted_key_ranges, no overlap
        let k = r("000000", "000005");
        let ranges = vec![r("000010", "000020")];
        assert_eq!(
            super::LevelStorege::<Memstore>::key_range_overlap(k, &ranges),
            vec![]
        );

        // Test Case 12: Single range in sorted_key_ranges, with overlap
        let k = r("000015", "000025");
        let ranges = vec![r("000010", "000020")];
        assert_eq!(
            super::LevelStorege::<Memstore>::key_range_overlap(k, &ranges),
            vec![0]
        );

        // Test Case 13: With delete keys
        let k = r("000000", "000000"); // A delete key
        let ranges = vec![r("000000", "000000"), r("000010", "000020")]; // A range for the delete key
        assert_eq!(
            super::LevelStorege::<Memstore>::key_range_overlap(k, &ranges),
            vec![0]
        );

        // Test Case 14: Overlap with only the exact min key of a range
        let k = r("000000", "000010");
        let ranges = vec![r("000010", "000020")];
        assert_eq!(
            super::LevelStorege::<Memstore>::key_range_overlap(k, &ranges),
            vec![0]
        );

        // Test Case 15: Overlap with only the exact max key of a range
        let k = r("000020", "000030");
        let ranges = vec![r("000010", "000020")];
        assert_eq!(
            super::LevelStorege::<Memstore>::key_range_overlap(k, &ranges),
            vec![0]
        );
    }

    #[test]
    fn test_compact_level_no_level() {
        // create 3 table for compact input and
    }

    #[test]
    fn test_compact_level_no_overlap() {
        // level 1 table a [0..100] table b [200..300]
        // new table c [120..160] compact it to level 1
        // check by get key 120

        // Create existing tables in level 1 (non-overlapping)
        let table_l1_a = Arc::new(create_test_table_with_id_offset(0..100, 1000)); // keys "000000" to "000099"
        let table_l1_b = Arc::new(create_test_table_with_id_offset(200..300, 2000)); // keys "000200" to "000299"

        let level0_empty = super::Level::new(vec![], true); // Empty level 0
        let level1 = super::Level::new(vec![table_l1_a.clone(), table_l1_b.clone()], false);
        let mut level_storage = super::LevelStorege::new(
            vec![level0_empty, level1],
            LevelStoregeConfig::config_for_test(),
        );

        // Create a new table (table_c) that will be compacted into level 1.
        // Its key range [120..160] falls between table_l1_a and table_l1_b, with no overlap.
        let table_c_to_compact = Arc::new(create_test_table_with_id_offset(120..160, 3000)); // keys "000120" to "000159"

        // Perform compaction. We're simulating compacting a single table into level 1.
        let input_tables = vec![table_c_to_compact.clone()];
        let mut store_id_start = 5000; // Starting ID for new SSTables
        let _table_changes = level_storage.compact_level(&mut store_id_start, input_tables, 0); // level_depth 0 means input is from level 0, target is level 1

        // Get the compacted level (level 1)
        let compacted_level = &level_storage.levels[1]; // Since we only had level 1 initially, it's still levels[0]

        // Assertions:
        // 1. The compacted level should now contain 3 tables.
        assert_eq!(compacted_level.sstables.len(), 3);

        // 2. The tables should be in sorted order by their first key: table_l1_a, table_c_to_compact, table_l1_b
        assert_eq!(
            compacted_level.sstables[0].store_id(),
            table_l1_a.store_id()
        );
        assert_eq!(
            compacted_level.sstables[1].store_id(),
            table_c_to_compact.store_id()
        );
        assert_eq!(
            compacted_level.sstables[2].store_id(),
            table_l1_b.store_id()
        );

        // 3. Verify that the key from the newly inserted table can be found.
        let key_120 = KeySlice::from("000120".as_bytes());
        assert_level_find_and_check(&compacted_level, &key_120, Some("120".as_bytes()));

        // 4. Verify keys from original tables are still present.
        let key_50 = KeySlice::from("000050".as_bytes());
        assert_level_find_and_check(&compacted_level, &key_50, Some("50".as_bytes()));
        let key_250 = KeySlice::from("000250".as_bytes());
        assert_level_find_and_check(&compacted_level, &key_250, Some("250".as_bytes()));

        // 5. Verify a key outside the range is not found.
        let key_180 = KeySlice::from("000180".as_bytes());
        assert_level_find_and_check(&compacted_level, &key_180, None);
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
        let mut level_storage =
            super::LevelStorege::new(vec![level0, level1], LevelStoregeConfig::config_for_test());

        // Perform compaction
        let input_tables = vec![table_l0_a.clone(), table_l0_b.clone()];
        let mut store_id_start = 5000; // Define a mutable variable for the store_id
        let table_changes = level_storage.compact_level(&mut store_id_start, input_tables, 0);

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
        assert_eq!(delete_changes[0].id, table_l1_x.store_id());

        // Should add new compacted tables
        assert!(!add_changes.is_empty());
        for add_change in &add_changes {
            assert_eq!(add_change.level, 1);
            assert_eq!(add_change.change_type, ChangeType::Add);
            assert_eq!(add_change.id, 5000); // Assuming one compacted table for this data volume and using the starting ID
        }

        // Verify the final state of the target level (Level 1)
        let final_level_case1 = &level_storage.levels[1];
        assert_eq!(
            final_level_case1.sstables.len(),
            2,
            "Case 1: Final level 1 table count mismatch"
        );

        // Expected order: new compacted table (0..89) then original non-overlapping table_l1_y (100..149)
        let added_table_id_case1 = add_changes[0].id; // Get the ID of the new compacted table
        let expected_final_ids_case1 = vec![added_table_id_case1, table_l1_y.store_id()];
        let actual_final_ids_case1: Vec<u64> = final_level_case1
            .sstables
            .iter()
            .map(|t| t.store_id())
            .collect();
        assert_eq!(
            actual_final_ids_case1, expected_final_ids_case1,
            "Case 1: Final level 1 table IDs and order mismatch"
        );

        // Verify content by querying keys in Case 1
        assert_level_find_and_check(
            &final_level_case1,
            &KeySlice::from("000010".as_bytes()),
            Some("10".as_bytes()),
        ); // from table_l0_a
        assert_level_find_and_check(
            &final_level_case1,
            &KeySlice::from("000030".as_bytes()),
            Some("30".as_bytes()),
        ); // from table_l0_b
        assert_level_find_and_check(
            &final_level_case1,
            &KeySlice::from("000050".as_bytes()),
            Some("50".as_bytes()),
        ); // from table_l0_b
        assert_level_find_and_check(
            &final_level_case1,
            &KeySlice::from("000080".as_bytes()),
            Some("80".as_bytes()),
        ); // from table_l1_x
           // Check a key from the non-overlapping table_l1_y
        assert_level_find_and_check(
            &final_level_case1,
            &KeySlice::from("000120".as_bytes()),
            Some("120".as_bytes()),
        ); // from table_l1_y
           // Check a key outside the range
        assert_level_find_and_check(
            &final_level_case1,
            &KeySlice::from("000180".as_bytes()),
            None,
        );

        // Case 2: No overlap with target level, input tables' keys < target level min key
        // Create input tables with keys smaller than existing target level
        let table_l0_c = Arc::new(create_test_table_with_id_offset(0..30, 3000)); // keys "000000" to "000029"
        let table_l0_d = Arc::new(create_test_table_with_id_offset(10..40, 4000)); // keys "000010" to "000039"

        // Create target level with tables that have larger keys (no overlap)
        let table_l1_z = Arc::new(create_test_table_with_id_offset(200..250, 200)); // keys "000200" to "000249"
        let table_l1_w = Arc::new(create_test_table_with_id_offset(300..350, 300)); // keys "000300" to "000349"

        let level0_case2 = super::Level::new(vec![table_l0_c.clone(), table_l0_d.clone()], true);
        let level1_case2 = super::Level::new(vec![table_l1_z.clone(), table_l1_w.clone()], false);
        let mut level_storage_case2 = super::LevelStorege::new(
            vec![level0_case2, level1_case2],
            LevelStoregeConfig::config_for_test(),
        );

        // Perform compaction
        let input_tables_case2 = vec![table_l0_c.clone(), table_l0_d.clone()];
        let mut store_id_start_case2 = 6000; // Define a mutable variable for the store_id
        let table_changes_case2 =
            level_storage_case2.compact_level(&mut store_id_start_case2, input_tables_case2, 0);

        // Verify table changes for no overlap case
        let delete_changes_case2: Vec<_> = table_changes_case2
            .iter()
            .filter(|tc| tc.change_type == ChangeType::Delete)
            .collect();
        let add_changes_case2: Vec<_> = table_changes_case2
            .iter()
            .filter(|tc| tc.change_type == ChangeType::Add)
            .collect();

        let final_level = &level_storage_case2.levels[1];
        let expected_added_ids_sorted_by_key: Vec<u64> =
            add_changes_case2.iter().map(|tc| tc.id).collect();
        let mut expected_final_store_ids: Vec<u64> = Vec::new();
        expected_final_store_ids.extend(expected_added_ids_sorted_by_key);
        expected_final_store_ids.push(table_l1_z.store_id());
        expected_final_store_ids.push(table_l1_w.store_id());

        let actual_final_store_ids: Vec<u64> =
            final_level.sstables.iter().map(|t| t.store_id()).collect();

        assert_eq!(
            actual_final_store_ids.len(),
            expected_final_store_ids.len(),
            "Final level table count mismatch"
        );
        assert_eq!(
            actual_final_store_ids, expected_final_store_ids,
            "Final level table IDs and order mismatch"
        );

        // Should have no delete operations (no overlap)
        assert_eq!(delete_changes_case2.len(), 0);

        // Should have add operations for new compacted tables
        assert_eq!(
            add_changes_case2.len(),
            1,
            "Expected exactly one new compacted table to be added in Case 2"
        );
        let add_change = &add_changes_case2[0];
        assert_eq!(add_change.level, 1);
        assert_eq!(add_change.change_type, ChangeType::Add);
        assert_eq!(add_change.id, 6000); // The single new compacted table should have this starting ID
                                         // Since input keys are smaller than target level keys, new tables should be inserted at the beginning
        assert_eq!(add_change.index, 0);

        // Verify that the original tables in target level are still there and shifted
        let final_level = &level_storage_case2.levels[1];
        assert!(final_level.sstables.len() >= 2); // At least the original 2 tables plus compacted ones

        // The last tables should be the original ones (table_l1_z and table_l1_w)
        let last_tables: Vec<u64> = final_level
            .sstables
            .iter()
            .skip(final_level.sstables.len() - 2)
            .map(|t| t.store_id())
            .collect();
        assert!(last_tables.contains(&table_l1_z.store_id()));
        assert!(last_tables.contains(&table_l1_w.store_id()));
    }

    /// Tests the `table_num_in_levels` method to ensure it correctly reports the number of tables in each level.
    #[test]
    fn test_table_len_in_levels() {
        // Case 1: Empty LevelStorege
        let empty_storage =
            LevelStorege::<Memstore>::new(vec![], LevelStoregeConfig::config_for_test());
        assert_eq!(empty_storage.table_num_in_levels(), vec![] as Vec<usize>);

        // Case 2: Single level with tables
        let table1 = Arc::new(create_test_table_with_id(0..10, 1));
        let table2 = Arc::new(create_test_table_with_id(10..20, 2));
        let level0 = Level::new(vec![table1.clone(), table2.clone()], true);
        let storage_single_level =
            LevelStorege::new(vec![level0], LevelStoregeConfig::config_for_test());
        assert_eq!(storage_single_level.table_num_in_levels(), vec![2]);

        // Case 3: Multiple levels with different table counts
        let table3 = Arc::new(create_test_table_with_id(20..30, 3));
        let level1 = Level::new(vec![table3.clone()], false);
        let level2 = Level::new(vec![], false); // Empty level
        let table4 = Arc::new(create_test_table_with_id(30..40, 4));
        let table5 = Arc::new(create_test_table_with_id(40..50, 5));
        let table6 = Arc::new(create_test_table_with_id(50..60, 6));
        let level3 = Level::new(vec![table4, table5, table6], false);

        let storage_multi_level = LevelStorege::new(
            vec![
                Level::new(vec![table1, table2], true), // Level 0: 2 tables
                level1,                                 // Level 1: 1 table
                level2,                                 // Level 2: 0 tables
                level3,                                 // Level 3: 3 tables
            ],
            LevelStoregeConfig::config_for_test(),
        );
        assert_eq!(storage_multi_level.table_num_in_levels(), vec![2, 1, 0, 3]);
    }

    #[test]
    fn test_push_new_table() {
        // Create an empty LevelStorege
        let mut level_storage =
            LevelStorege::<Memstore>::new(vec![], LevelStoregeConfig::config_for_test());
        let mut next_id: StoreId = 100;

        // Case 1: Push data that fits into one table
        let data1 = vec![
            KVOpertion::new(
                1,
                "key001".as_bytes().into(),
                OpType::Write("val001".as_bytes().into()),
            ),
            KVOpertion::new(
                2,
                "key002".as_bytes().into(),
                OpType::Write("val002".as_bytes().into()),
            ),
        ];
        level_storage.push_new_table(data1.into_iter(), &mut next_id);

        assert_eq!(level_storage.levels.len(), 1); // Level 0 should be created
        assert_eq!(level_storage.levels[0].sstables.len(), 1); // One table in level 0
        assert_eq!(level_storage.levels[0].sstables[0].store_id(), 100); // Check table ID
        assert_eq!(next_id, 101); // next_id should be incremented

        // Verify data can be found
        assert_level_storage_find_and_check(
            &level_storage,
            &KeySlice::from("key001".as_bytes()),
            Some("val001".as_bytes()),
            u64::MAX,
        );
        assert_level_storage_find_and_check(
            &level_storage,
            &KeySlice::from("key002".as_bytes()),
            Some("val002".as_bytes()),
            u64::MAX,
        );

        // Case 2: Push more data, creating another table in level 0
        let data2 = vec![
            KVOpertion::new(
                3,
                "key003".as_bytes().into(),
                OpType::Write("val003".as_bytes().into()),
            ),
            KVOpertion::new(
                4,
                "key004".as_bytes().into(),
                OpType::Write("val004".as_bytes().into()),
            ),
        ];
        level_storage.push_new_table(data2.into_iter(), &mut next_id);

        assert_eq!(level_storage.levels.len(), 1);
        assert_eq!(level_storage.levels[0].sstables.len(), 2); // Now two tables in level 0
        assert_eq!(level_storage.levels[0].sstables[0].store_id(), 101); // Newest table first
        assert_eq!(level_storage.levels[0].sstables[1].store_id(), 100);
        assert_eq!(next_id, 102);

        // Verify new data and old data
        assert_level_storage_find_and_check(
            &level_storage,
            &KeySlice::from("key001".as_bytes()),
            Some("val001".as_bytes()),
            u64::MAX,
        );
        assert_level_storage_find_and_check(
            &level_storage,
            &KeySlice::from("key003".as_bytes()),
            Some("val003".as_bytes()),
            u64::MAX,
        );

        // Case 3: Push data that requires multiple tables (simulate large data)
        // We need enough data to force TableBuilder::add to return false.
        // The exact amount depends on DATA_BLOCK_SIZE and KV operation encoding size.
        // Let's create many small KVs. Assume DATA_BLOCK_SIZE is small for testing or create many KVs.
        // For simplicity, let's assume 2 KVs fill a block and force a new table.
        // (This requires adjusting TableBuilder/BlockBuilder logic or creating lots of data)

        // Reset storage for this case
        let mut level_storage_multi =
            LevelStorege::<Memstore>::new(vec![], LevelStoregeConfig::config_for_test());
        let mut next_id_multi: StoreId = 200;

        // Create enough data to likely span multiple tables.
        // The exact number depends on block size and encoding.
        // Let's create 1000 KVs. If block size is ~4KB, this should create multiple tables.
        // Use the existing helper function to create the KVOpertions.
        // Note: create_test_table_with_id_offset returns a TableReader, we need the Vec<KVOpertion>.
        // We need a function like `create_kv_data_with_range_id_offset` from block::test.
        // Let's assume we have access to a similar function or adapt it.
        // For now, we'll use the existing map as the helper isn't directly usable here.
        // Re-using the map logic but ensuring it matches the helper's output format if possible.
        let large_data: Vec<KVOpertion> =
            crate::db::block::test::create_kv_data_with_range_id_offset(1000..200000, 0);

        let initial_table_count = level_storage_multi
            .levels
            .get(0)
            .map_or(0, |l| l.sstables.len());
        level_storage_multi.push_new_table(large_data.into_iter(), &mut next_id_multi);

        assert_eq!(level_storage_multi.levels.len(), 1);
        let final_table_count = level_storage_multi.levels[0].sstables.len();
        // We expect more than one table to be created. The exact number depends on block/table size.
        assert!(
            final_table_count > initial_table_count + 1,
            "Expected multiple tables to be created for large data push"
        );
        assert_eq!(
            next_id_multi,
            200 + (final_table_count - initial_table_count) as u64
        ); // ID incremented for each new table

        // Verify some data points
        assert_level_storage_find_and_check(
            &level_storage_multi,
            &KeySlice::from("001001".as_bytes()),
            Some("1001".as_bytes()),
            u64::MAX,
        );
        assert_level_storage_find_and_check(
            &level_storage_multi,
            &KeySlice::from("001400".as_bytes()),
            Some("1400".as_bytes()),
            u64::MAX,
        );
        assert_level_storage_find_and_check(
            &level_storage_multi,
            &KeySlice::from("001999".as_bytes()),
            Some("1999".as_bytes()),
            u64::MAX,
        );
    }
}
