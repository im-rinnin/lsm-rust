use anyhow::Result;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::common::SearchResult;
use super::key::KeyBytes;
use crate::db::common::{KVOperation, KeyQuery, OpId, OpType};
use crossbeam_skiplist::map::Entry;
use crossbeam_skiplist::SkipMap;

pub struct Memtable {
    table: SkipMap<(KeyBytes, OpId), OpType>,
    capacity_bytes: AtomicUsize,
    current_size_bytes: AtomicUsize,
}

pub struct MemtableIterator<'a> {
    inner_iter: crossbeam_skiplist::map::Iter<'a, (KeyBytes, OpId), OpType>,
    // This field stores the current entry that has been "peeked" from `inner_iter`
    // but not yet processed (i.e., its key group hasn't been fully deduplicated).
    peeked_entry: Option<Entry<'a, (KeyBytes, OpId), OpType>>,
}

impl Memtable {
    pub fn new(capacity_bytes: usize) -> Self {
        Memtable {
            table: SkipMap::new(),
            capacity_bytes: AtomicUsize::new(capacity_bytes),
            current_size_bytes: AtomicUsize::new(0),
        }
    }

    pub fn get_size(&self) -> usize {
        self.current_size_bytes.load(Ordering::Relaxed)
    }
    pub fn get_capacity_bytes(&self) -> usize {
        self.capacity_bytes.load(Ordering::Relaxed)
    }
    pub fn get(&self, q: &KeyQuery) -> SearchResult {
        let range_start_bound = (q.key.clone(), 0);
        // Use an inclusive upper bound to avoid overflow when q.op_id == u64::MAX
        for entry in self
            .table
            .range(range_start_bound..=(q.key.clone(), q.op_id))
            .rev()
        {
            let (_entry_key, entry_op_id) = entry.key();
            let entry_op_type = entry.value();
            return Some((entry_op_type.clone(), *entry_op_id));
        }
        None
    }

    pub fn insert(&self, op: KVOperation) -> Result<()> {
        let op_size = op.encode_size();
        self.table.insert((op.key, op.id), op.op);
        self.current_size_bytes
            .fetch_add(op_size, Ordering::Relaxed);
        Ok(())
    }

    fn len(&self) -> usize {
        self.table.len()
    }

    pub fn to_iter<'a>(&'a self) -> MemtableIterator<'a> {
        let mut iter = self.table.iter();
        let first_entry = iter.next(); // Pre-fetch the very first item
        MemtableIterator {
            inner_iter: iter,
            peeked_entry: first_entry,
        }
    }

    pub fn is_full(&self) -> bool {
        self.current_size_bytes.load(Ordering::Relaxed)
            >= self.capacity_bytes.load(Ordering::Relaxed)
    }
}

impl<'a> Iterator for MemtableIterator<'a> {
    type Item = KVOperation;

    fn next(&mut self) -> Option<Self::Item> {
        // Get the current candidate entry. If `peeked_entry` is `None`, try to fetch from `inner_iter`.
        let mut best_entry = match self.peeked_entry.take() {
            Some(entry) => entry,
            None => {
                // If `peeked_entry` was `None`, try to get the next from the underlying iterator.
                // If that's also `None`, then the iterator is exhausted.
                self.inner_iter.next()?
            }
        };

        // Establish the key we are currently processing.
        // We clone here to own the KeyBytes, so it doesn't borrow `best_entry`.
        // This allows `best_entry` to be reassigned later without borrow conflicts.
        let current_group_key = best_entry.key().0.clone();

        // Loop to find the last (max OpId) entry for this `current_group_key`.
        loop {
            // Try to get the next raw entry from the inner iterator.
            match self.inner_iter.next() {
                Some(next_raw_entry) => {
                    // Compare the key of the `next_raw_entry` with our `current_group_key`.
                    if next_raw_entry.key().0 == current_group_key {
                        // Same key: this `next_raw_entry` is newer (due to SkipMap's OpId sorting),
                        // so it becomes the new `best_entry` for this key group.
                        best_entry = next_raw_entry;
                        // Continue the loop to find even newer versions of this same key.
                    } else {
                        // Different key encountered.
                        // `best_entry` now holds the latest operation for the *previous* key group.
                        // `next_raw_entry` is the first operation of the *new* key group.
                        // Store `next_raw_entry` in `peeked_entry` for the *next* call to `next()`.
                        self.peeked_entry = Some(next_raw_entry);
                        break; // Exit inner loop; we've found our `best_entry` to return.
                    }
                }
                None => {
                    // Inner iterator exhausted. No more items.
                    self.peeked_entry = None; // Explicitly clear, as nothing follows.
                    break; // Exit inner loop; `best_entry` is the final one.
                }
            }
        }

        // Convert the `best_entry` (which is the latest for its key group)
        // into the `KVOperation` to be returned.
        Some(KVOperation {
            id: best_entry.key().1,
            key: best_entry.key().0.clone(), // Clone the KeyBytes for ownership
            op: best_entry.value().clone(),  // Clone the OpType for ownership
        })
    }
}

#[cfg(test)]
mod test {
    use tracing::info;

    use crate::db::common::{KVOperation, KeyQuery, OpId, OpType};
    use crate::db::db_log;
    use crate::db::key::KeyBytes;

    fn get_next_id(id: &mut OpId) -> OpId {
        let old = *id;
        *id += 1;
        old
    }

    use super::Memtable;

    const TEST_MEMTABLE_CAPACITY: usize = 1024 * 1024; // 1MB for tests

    #[test]
    fn test_empty_count() {
        let mut m = Memtable::new(TEST_MEMTABLE_CAPACITY);
        assert_eq!(m.len(), 0);
        let op = KVOperation::new(
            1,
            1.to_string().as_bytes().into(),
            OpType::Write(1.to_string().as_bytes().into()),
        );
        m.insert(op).unwrap();
        assert_eq!(m.len(), 1);
    }

    #[test]
    fn test_get_with_duplicate_key() {
        let mut m = Memtable::new(TEST_MEMTABLE_CAPACITY);
        let mut id = 0;
        let key: KeyBytes = "test_key".as_bytes().into();

        // Insert multiple operations for the same key with increasing op_ids
        // 1. Write "value1" with op_id 0
        let op_id_0 = get_next_id(&mut id);
        m.insert(KVOperation::new(
            op_id_0,
            key.clone(),
            OpType::Write("value1".as_bytes().into()),
        ))
        .unwrap();

        // 2. Write "value2" with op_id 1
        let op_id_1 = get_next_id(&mut id);
        m.insert(KVOperation::new(
            op_id_1,
            key.clone(),
            OpType::Write("value2".as_bytes().into()),
        ))
        .unwrap();

        // 3. Delete with op_id 2
        let op_id_2 = get_next_id(&mut id);
        m.insert(KVOperation::new(op_id_2, key.clone(), OpType::Delete))
            .unwrap();

        // 4. Write "value3" with op_id 3
        let op_id_3 = get_next_id(&mut id);
        m.insert(KVOperation::new(
            op_id_3,
            key.clone(),
            OpType::Write("value3".as_bytes().into()),
        ))
        .unwrap();

        let current_max_op_id = id;

        // Test queries at different op_ids
        // Query at op_id 0: Should see "value1"
        let res = m.get(&KeyQuery {
            op_id: op_id_0,
            key: key.as_ref().into(),
        });
        assert!(res.is_some());
        assert_eq!(
            match res.unwrap().0 {
                OpType::Write(ref v) => v.as_ref(),
                OpType::Delete =>
                    panic!("Expected a Write operation, found Delete for key test_key"),
            },
            "value1".as_bytes()
        );

        // Query at op_id 1: Should see "value2"
        let res = m.get(&KeyQuery {
            op_id: op_id_1,
            key: key.as_ref().into(),
        });
        assert!(res.is_some());
        assert_eq!(
            match res.unwrap().0 {
                OpType::Write(ref v) => v.as_ref(),
                OpType::Delete =>
                    panic!("Expected a Write operation, found Delete for key test_key"),
            },
            "value2".as_bytes()
        );

        // Query at op_id 2: Should see None (deleted)
        let res = m.get(&KeyQuery {
            op_id: op_id_2,
            key: key.as_ref().into(),
        });
        assert!(matches!(res.unwrap().0, OpType::Delete));

        // Query at op_id 3: Should see "value3"
        let res = m.get(&KeyQuery {
            op_id: op_id_3,
            key: KeyBytes::from(key.as_ref()),
        });
        assert!(res.is_some());
        assert_eq!(
            match res.unwrap().0 {
                OpType::Write(ref v) => v.as_ref(),
                OpType::Delete =>
                    panic!("Expected a Write operation, found Delete for key test_key"),
            },
            "value3".as_bytes()
        );

        // Query at an op_id higher than any existing op: Should see "value3" (latest write)
        let res = m.get(&KeyQuery {
            op_id: current_max_op_id,
            key: KeyBytes::from(key.as_ref()),
        });
        assert!(res.is_some());
        assert_eq!(
            match res.unwrap().0 {
                OpType::Write(ref v) => v.as_ref(),
                OpType::Delete =>
                    panic!("Expected a Write operation, found Delete for key test_key"),
            },
            "value3".as_bytes()
        );
    }
    #[test]
    fn test_insert_delete_and_get() {
        let mut m = Memtable::new(TEST_MEMTABLE_CAPACITY);
        let mut id = 0;
        // put 1..20
        for i in 0..20 {
            let op_id = get_next_id(&mut id);
            let op = KVOperation::new(
                op_id,
                i.to_string().as_bytes().into(),
                OpType::Write(i.to_string().as_bytes().into()),
            );
            m.insert(op).unwrap();
        }
        // delete 10
        let op_id_delete_10 = get_next_id(&mut id);
        let op = KVOperation::new(
            op_id_delete_10,
            10.to_string().as_bytes().into(),
            OpType::Delete,
        );
        m.insert(op).unwrap();
        // overwirte  12 to 100
        let op_id_overwrite_12 = get_next_id(&mut id);
        let op = KVOperation::new(
            op_id_overwrite_12,
            12.to_string().as_bytes().into(),
            OpType::Write(100.to_string().as_bytes().into()),
        );
        m.insert(op).unwrap();

        let current_max_op_id = id;

        // check op id and key match in 0..10 (excluding 10)
        for i in 0..10 {
            let res = m.get(&KeyQuery {
                op_id: current_max_op_id,
                key: i.to_string().as_bytes().into(),
            });
            assert!(res.is_some(), "Key {} should be found", i);
            assert_eq!(
                match res.unwrap().0 {
                    OpType::Write(ref v) => v.as_ref(),
                    OpType::Delete => panic!("Expected a Write operation, found Delete"),
                },
                i.to_string().as_bytes(),
                "Value for key {} should be {}",
                i,
                i
            );
        }

        //check delete
        let res = m.get(&KeyQuery {
            op_id: current_max_op_id,
            key: 10.to_string().as_bytes().into(),
        });
        assert!(
            res.is_some(),
            "Key 10 should be found as deleted at current_max_op_id"
        );
        assert!(
            matches!(res.unwrap().0, OpType::Delete),
            "Key 10 should be marked as deleted"
        );

        // 10 is deleted but still can be get by op id before its delete
        let res = m.get(&KeyQuery {
            op_id: 10, // Query at the op_id when key '10' was inserted
            key: 10.to_string().as_bytes().into(),
        });
        assert!(res.is_some(), "Key 10 should be found at op_id 10");
        assert_eq!(
            match res.unwrap().0 {
                OpType::Write(ref v) => v.as_ref(),
                OpType::Delete => panic!("Expected a Write operation, found Delete for key 10"),
            },
            10.to_string().as_bytes()
        );

        // Check overwritten key 12
        let res = m.get(&KeyQuery {
            op_id: current_max_op_id,
            key: 12.to_string().as_bytes().into(),
        });
        assert!(res.is_some(), "Key 12 should be found at current_max_op_id");
        assert_eq!(
            match res.unwrap().0 {
                OpType::Write(ref v) => v.as_ref(),
                OpType::Delete => panic!("Expected a Write operation, found Delete for key 12"),
            },
            100.to_string().as_bytes(),
            "Value for key 12 should be 100"
        );

        // check key not exit
        let res = m.get(&KeyQuery {
            op_id: current_max_op_id,
            key: 100.to_string().as_bytes().into(),
        });
        assert!(res.is_none(), "Key 100 should not exist");

        // check op id not match
        let res = m.get(&KeyQuery {
            op_id: 1,
            key: 5.to_string().as_bytes().into(),
        });
        assert!(res.is_none(), "Key 5 should not be found with op_id 1");
    }

    #[test]
    fn test_iter_order() {
        let m = Memtable::new(TEST_MEMTABLE_CAPACITY);
        let mut id = 0;

        // Insert data in an "unordered" way to test the SkipMap's internal sorting
        // and the iterator's deduplication logic.
        // We expect (key, op_id) to be the sorting order.
        let mut inserted_ops = Vec::new();

        let mut add_op = |m: &Memtable, key: &str, value: &str, op_id_gen: &mut OpId| {
            let op_id = get_next_id(op_id_gen);
            let op_type = OpType::Write(value.as_bytes().into());
            let op = KVOperation::new(op_id, key.as_bytes().into(), op_type.clone());
            m.insert(op).unwrap();
            inserted_ops.push((key.to_string(), op_id, op_type));
        };

        // Key "b", op_id 0
        add_op(&m, "b", "val_b_0", &mut id);
        // Key "a", op_id 1
        add_op(&m, "a", "val_a_1", &mut id);
        // Key "c", op_id 2
        add_op(&m, "c", "val_c_2", &mut id);
        // Key "b", op_id 3 (newer version of "b")
        add_op(&m, "b", "val_b_3", &mut id);
        // Key "a", op_id 4 (newer version of "a")
        add_op(&m, "a", "val_a_4", &mut id);
        // Key "a", op_id 5 (even newer version of "a")
        add_op(&m, "a", "val_a_5", &mut id);
        // Key "d", op_id 6, Delete
        let op_id_delete_d = get_next_id(&mut id);
        let op = KVOperation::new(op_id_delete_d, "d".as_bytes().into(), OpType::Delete);
        m.insert(op).unwrap();
        // Key "d", op_id 7, Write (after delete)
        add_op(&m, "d", "val_d_7", &mut id);
        inserted_ops.push(("d".to_string(), op_id_delete_d, OpType::Delete));

        // Sort the expected data by (key, op_id) to match the SkipMap's internal order
        inserted_ops.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));

        let mut actual_iter_data: Vec<(String, OpId, OpType)> = Vec::new();
        for entry in m.to_iter() {
            actual_iter_data.push((entry.key.to_string(), entry.id, entry.op.clone()));
        }

        // The MemtableIterator deduplicates keys, returning only the latest OpId for each key.
        // So, `actual_iter_data` should contain only the final state for each unique key.
        // We need to process `inserted_ops` to get the 'expected' deduplicated and sorted list.

        let mut expected_deduplicated_data: Vec<KVOperation> = Vec::new();
        for (key_str, op_id, op_type) in inserted_ops {
            let key = KeyBytes::from(key_str.as_bytes());
            let op = KVOperation::new(op_id, key.clone(), op_type);

            // Find if this key already exists in the result, and replace if new op_id is higher
            let mut found = false;
            for existing_op in &mut expected_deduplicated_data {
                if existing_op.key == key {
                    // This relies on `inserted_ops` being sorted by key, then op_id,
                    // so the last one encountered for a key will be the latest.
                    *existing_op = op.clone();
                    found = true;
                    break;
                }
            }
            if !found {
                expected_deduplicated_data.push(op);
            }
        }
        // Ensure final expected data is also sorted by key, then OpId (though OpId should be unique for a key now)
        expected_deduplicated_data.sort_by(|a, b| a.key.cmp(&b.key).then_with(|| a.id.cmp(&b.id)));

        assert_eq!(actual_iter_data.len(), expected_deduplicated_data.len());

        for (i, actual) in actual_iter_data.iter().enumerate() {
            let expected = &expected_deduplicated_data[i];
            assert_eq!(
                actual.0,
                expected.key.to_string(),
                "Key mismatch at index {}",
                i
            );
            assert_eq!(
                actual.1, expected.id,
                "OpId mismatch for key {} at index {}",
                actual.0, i
            );
            match (&actual.2, &expected.op) {
                (OpType::Write(v1), OpType::Write(v2)) => {
                    assert_eq!(v1, v2, "Value mismatch for key {} at index {}", actual.0, i)
                }
                (OpType::Delete, OpType::Delete) => {}
                _ => panic!("OpType mismatch for key {} at index {}", actual.0, i),
            }
        }

        // Additionally, test specific expected final values
        let mut iter = m.to_iter();
        assert_eq!(iter.next().unwrap().key.to_string(), "a");
        assert_eq!(iter.next().unwrap().key.to_string(), "b");
        assert_eq!(iter.next().unwrap().key.to_string(), "c");
        assert_eq!(iter.next().unwrap().key.to_string(), "d");
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_iterator() {
        let mut m = Memtable::new(TEST_MEMTABLE_CAPACITY);
        let mut id = 0;

        let ops_to_insert = vec![
            (3, OpType::Write(3.to_string().as_bytes().into())),
            (2, OpType::Write(2.to_string().as_bytes().into())),
            (1, OpType::Write(1.to_string().as_bytes().into())),
            (4, OpType::Write(4.to_string().as_bytes().into())),
            (0, OpType::Write(0.to_string().as_bytes().into())),
            (2, OpType::Delete),
            (3, OpType::Write(100.to_string().as_bytes().into())),
        ];

        let mut expected_iter_data: Vec<(String, OpId, OpType)> = Vec::new();

        for (key_val, op_type) in ops_to_insert {
            let op_id = get_next_id(&mut id);
            let op = KVOperation::new(op_id, key_val.to_string().as_bytes().into(), op_type);
            expected_iter_data.push((key_val.to_string(), op_id, op.op.clone()));
            m.insert(op).unwrap();
        }

        // remvoe duplicate 2 and 3
        expected_iter_data.remove(0);
        expected_iter_data.remove(0);

        // Sort expected data by (key, op_id) to match SkipMap iteration order
        expected_iter_data.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));

        let mut actual_iter_data: Vec<(String, OpId, OpType)> = Vec::new();
        for entry in m.to_iter() {
            actual_iter_data.push((entry.key.to_string(), entry.id, entry.op.clone()));
        }

        assert_eq!(actual_iter_data.len(), expected_iter_data.len());
        for (actual, expected) in actual_iter_data.iter().zip(expected_iter_data.iter()) {
            assert_eq!(actual.0, expected.0, "Key mismatch");
            assert_eq!(actual.1, expected.1, "OpId mismatch for key {}", actual.0);
            match (&actual.2, &expected.2) {
                (OpType::Write(v1), OpType::Write(v2)) => {
                    assert_eq!(v1, v2, "Value mismatch for key {}", actual.0)
                }
                (OpType::Delete, OpType::Delete) => {}
                _ => panic!("OpType mismatch for key {}", actual.0),
            }
        }
    }

    #[test]
    fn test_memtable_capacity() {
        let small_capacity = 100; // A small capacity for testing
        let m = Memtable::new(small_capacity); // No longer mutable
        let mut id = 0;

        // Insert operations until full
        // Note: current_size is now managed by the atomic counter within Memtable
        // We can't track it externally in the same way for precise comparison
        // but we can check the Memtable's reported size.

        while !m.is_full() {
            let op_id = get_next_id(&mut id);
            let key_str = format!("key_{}", op_id);
            let value_str = format!("value_{}", op_id);
            let op = KVOperation::new(
                op_id,
                key_str.as_bytes().into(),
                OpType::Write(value_str.as_bytes().into()),
            );
            let op_size = op.encode_size();

            // Check if adding the current operation would exceed the capacity
            // This check is now more about predicting the state after the atomic operation
            // For a single-threaded test, this logic is fine.
            if m.get_size() + op_size > small_capacity && !m.is_full() {
                m.insert(op).unwrap();
                assert!(m.is_full());
                break;
            }
            m.insert(op).unwrap();
            assert!(!m.is_full());
        }

        assert!(m.get_size() >= small_capacity);
        assert!(m.is_full());

        // Try to insert one more operation, it should still be full or exceed capacity
        let op_id = get_next_id(&mut id);
        let key_str = format!("key_final_{}", op_id);
        let value_str = format!("value_final_{}", op_id);
        let op = KVOperation::new(
            op_id,
            key_str.as_bytes().into(),
            OpType::Write(value_str.as_bytes().into()),
        );
        m.insert(op).unwrap();
        assert!(m.is_full()); // It should remain full or exceed capacity
    }
}
