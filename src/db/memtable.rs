use std::fmt::Result;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::common::SearchResult;
use super::key::{KeyBytes, KeyVec};
use crate::db::common::{KVOpertion, KeyQuery, OpId, OpType};
use crossbeam_skiplist::map::Entry;
use crossbeam_skiplist::SkipMap;

pub struct Memtable {
    table: SkipMap<(KeyBytes, OpId), OpType>,
    capacity_bytes: AtomicUsize,
    current_size_bytes: AtomicUsize,
}

pub struct MemtableIterator<'a> {
    inner_iter: crossbeam_skiplist::map::Iter<'a, (KeyBytes, OpId), OpType>,
}

impl Memtable {
    pub fn get_capacity_bytes(&self) -> usize {
        self.capacity_bytes.load(Ordering::Relaxed)
    }
    pub fn new(capacity_bytes: usize) -> Self {
        Memtable {
            table: SkipMap::new(),
            capacity_bytes: AtomicUsize::new(capacity_bytes),
            current_size_bytes: AtomicUsize::new(0),
        }
    }

    pub fn get<'a>(&'a self, q: &KeyQuery) -> SearchResult {
        let range_start_bound = (q.key.clone(), 0);
        let range_end_bound = (q.key.clone(), q.op_id + 1);

        for entry in self.table.range(range_start_bound..range_end_bound).rev() {
            let (_entry_key, entry_op_id) = entry.key();
            let entry_op_type = entry.value();
            return Some((entry_op_type.clone(), *entry_op_id));
        }
        None
    }

    pub fn insert(&self, op: KVOpertion) -> Result {
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
        MemtableIterator {
            inner_iter: self.table.iter(),
        }
    }

    pub fn estimated_size_bytes(&self) -> usize {
        self.current_size_bytes.load(Ordering::Relaxed)
    }

    pub fn is_full(&self) -> bool {
        self.current_size_bytes.load(Ordering::Relaxed)
            >= self.capacity_bytes.load(Ordering::Relaxed)
    }
}

impl<'a> Iterator for MemtableIterator<'a> {
    type Item = KVOpertion;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner_iter.next().map(|entry| {
            let (key_bytes, op_id) = entry.key();
            let op_type = entry.value();
            // Clone the key and op_type as KVOpertion takes ownership
            KVOpertion {
                id: *op_id,
                key: key_bytes.clone(),
                op: op_type.clone(),
            }
        })
    }
}

#[cfg(test)]
mod test {
    use crate::db::common::{KVOpertion, KeyQuery, OpId, OpType};
    use crate::db::key::{KeyBytes, KeyVec};

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
        let op = KVOpertion::new(
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
        let key: KeyVec = "test_key".as_bytes().into();

        // Insert multiple operations for the same key with increasing op_ids
        // 1. Write "value1" with op_id 0
        let op_id_0 = get_next_id(&mut id);
        m.insert(KVOpertion::new(
            op_id_0,
            key.clone(),
            OpType::Write("value1".as_bytes().into()),
        ))
        .unwrap();

        // 2. Write "value2" with op_id 1
        let op_id_1 = get_next_id(&mut id);
        m.insert(KVOpertion::new(
            op_id_1,
            key.clone(),
            OpType::Write("value2".as_bytes().into()),
        ))
        .unwrap();

        // 3. Delete with op_id 2
        let op_id_2 = get_next_id(&mut id);
        m.insert(KVOpertion::new(op_id_2, key.clone(), OpType::Delete))
            .unwrap();

        // 4. Write "value3" with op_id 3
        let op_id_3 = get_next_id(&mut id);
        m.insert(KVOpertion::new(
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
            let op = KVOpertion::new(
                op_id,
                i.to_string().as_bytes().into(),
                OpType::Write(i.to_string().as_bytes().into()),
            );
            m.insert(op).unwrap();
        }
        // delete 10
        let op_id_delete_10 = get_next_id(&mut id);
        let op = KVOpertion::new(
            op_id_delete_10,
            10.to_string().as_bytes().into(),
            OpType::Delete,
        );
        m.insert(op).unwrap();
        // overwirte  12 to 100
        let op_id_overwrite_12 = get_next_id(&mut id);
        let op = KVOpertion::new(
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
    fn test_iterator() {
        let mut m = Memtable::new(TEST_MEMTABLE_CAPACITY);
        let mut id = 0;

        let ops_to_insert = vec![
            (3, OpType::Write(3.to_string().as_bytes().into())),
            (2, OpType::Write(2.to_string().as_bytes().into())),
            (1, OpType::Write(1.to_string().as_bytes().into())),
            (4, OpType::Write(4.to_string().as_bytes().into())),
            (0, OpType::Write(0.to_string().as_bytes().into())),
        ];

        let mut expected_iter_data: Vec<(String, OpId, OpType)> = Vec::new();

        for (key_val, op_type) in ops_to_insert {
            let op_id = get_next_id(&mut id);
            let op = KVOpertion::new(op_id, key_val.to_string().as_bytes().into(), op_type);
            expected_iter_data.push((key_val.to_string(), op_id, op.op.clone()));
            m.insert(op).unwrap();
        }

        // delete 2
        let op_id_delete_2 = get_next_id(&mut id);
        let op = KVOpertion::new(
            op_id_delete_2,
            2.to_string().as_bytes().into(),
            OpType::Delete,
        );
        expected_iter_data.push((2.to_string(), op_id_delete_2, op.op.clone()));
        m.insert(op).unwrap();

        // overwirte 3 to 100
        let op_id_overwrite_3 = get_next_id(&mut id);
        let op = KVOpertion::new(
            op_id_overwrite_3,
            3.to_string().as_bytes().into(),
            OpType::Write(100.to_string().as_bytes().into()),
        );
        expected_iter_data.push((3.to_string(), op_id_overwrite_3, op.op.clone()));
        m.insert(op).unwrap();

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
            let op = KVOpertion::new(
                op_id,
                key_str.as_bytes().into(),
                OpType::Write(value_str.as_bytes().into()),
            );
            let op_size = op.encode_size();

            // Check if adding the current operation would exceed the capacity
            // This check is now more about predicting the state after the atomic operation
            // For a single-threaded test, this logic is fine.
            if m.estimated_size_bytes() + op_size > small_capacity && !m.is_full() {
                m.insert(op).unwrap();
                assert!(m.is_full());
                break;
            }
            m.insert(op).unwrap();
            assert!(!m.is_full());
        }

        assert!(m.estimated_size_bytes() >= small_capacity);
        assert!(m.is_full());

        // Try to insert one more operation, it should still be full or exceed capacity
        let op_id = get_next_id(&mut id);
        let key_str = format!("key_final_{}", op_id);
        let value_str = format!("value_final_{}", op_id);
        let op = KVOpertion::new(
            op_id,
            key_str.as_bytes().into(),
            OpType::Write(value_str.as_bytes().into()),
        );
        m.insert(op).unwrap();
        assert!(m.is_full()); // It should remain full or exceed capacity
    }
}
