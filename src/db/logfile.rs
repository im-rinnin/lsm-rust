use std::fs::File; // Add import for File

use super::{key::KeyBytes, store::*};
use crate::db::common::KVOperation;
const LOG_FILE_NAME: &str = "logfile";
// log data struct
// [[KVOperation 1],[KVOperation 2]....]
pub struct LogFile<T: Store> {
    s: T,
}

impl<T: Store> LogFile<T> {
    pub fn open(store_id: StoreId) -> Self {
        LogFile {
            s: T::open_with(store_id, "log", "data"),
        }
    }
    pub fn open_with(t: T) -> Self {
        LogFile { s: t }
    }
    pub fn append(&mut self, ops: &Vec<KVOperation>) {
        for op in ops {
            let mut buffer = Vec::new();
            op.encode(&mut buffer);
            let len = buffer.len() as u64;
            self.s.append(&len.to_le_bytes());
            self.s.append(&buffer);
        }
        self.s.flush();
    }
}

use byteorder::{LittleEndian, ReadBytesExt};
use std::io::Cursor;

pub struct LogFileIter<T: Store> {
    store: T,
    current_offset: usize,
}

impl<T: Store> LogFileIter<T> {
    pub fn new(store: T) -> Self {
        LogFileIter {
            store,
            current_offset: 0,
        }
    }
}

impl<T: Store> Iterator for LogFileIter<T> {
    type Item = KVOperation;
    fn next(&mut self) -> Option<Self::Item> {
        // Check if there are enough bytes for the length (u64)
        if self.current_offset + 8 > self.store.len() {
            return None;
        }

        let mut len_buf = [0u8; 8];
        self.store.read_at(&mut len_buf, self.current_offset);
        let kv_op_len = Cursor::new(len_buf).read_u64::<LittleEndian>().unwrap() as usize;
        self.current_offset += 8;

        // Check if there are enough bytes for the KVOperation itself
        if self.current_offset + kv_op_len > self.store.len() {
            // This indicates a corrupted log file or incomplete write
            return None;
        }

        let mut kv_op_buf = vec![0u8; kv_op_len];
        self.store.read_at(&mut kv_op_buf, self.current_offset);
        self.current_offset += kv_op_len;

        // KVOperation::decode expects Bytes, so convert Vec<u8> to Bytes
        let (kv_op, _offset) = KVOperation::decode(bytes::Bytes::from(kv_op_buf));
        Some(kv_op)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::db::common::{KVOperation, OpId, OpType};
    use crate::db::key::KeyBytes;
    use std::env;
    use std::fs::OpenOptions; // Add this import
    use tempfile::tempdir;

    #[test]
    fn test_log_file_iter() {
        let store_id = 1u64;

        // Create a temporary directory for the test
        let dir = tempdir().expect("Failed to create temp dir");

        // Use Filestore instead of Memstore
        let filename = format!("{}.data", store_id);
        let file_path = dir.path().join(&filename); // Create full path
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&file_path) // Open file using the full path
            .expect(&format!("Failed to open file: {}", file_path.display()));
        let filestore_write = Filestore::open_with_file(file, store_id);
        let mut log_file = LogFile::open_with(filestore_write);

        let ops = vec![
            KVOperation::new(
                1,
                KeyBytes::from_vec(b"key1".to_vec()),
                OpType::Write(KeyBytes::from_vec(b"value1".to_vec())),
            ),
            KVOperation::new(
                2,
                KeyBytes::from_vec(b"key2".to_vec()),
                OpType::Write(KeyBytes::from_vec(b"value2".to_vec())),
            ),
            KVOperation::new(3, KeyBytes::from_vec(b"key3".to_vec()), OpType::Delete),
        ];

        log_file.append(&ops);

        // Re-open the log file to get a fresh store for iteration
        let read_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&file_path) // Use file_path here
            .expect(&format!(
                "Failed to open file for reading: {}",
                file_path.display()
            ));
        let read_store = Filestore::open_with_file(read_file, store_id);
        let mut iter = LogFileIter::new(read_store);

        let mut retrieved_ops = Vec::new();
        while let Some(op) = iter.next() {
            retrieved_ops.push(op);
        }

        assert_eq!(ops.len(), retrieved_ops.len());
        for i in 0..ops.len() {
            assert_eq!(ops[i].id, retrieved_ops[i].id);
            assert_eq!(ops[i].key, retrieved_ops[i].key);
            assert_eq!(ops[i].op, retrieved_ops[i].op);
        }

        // Test with an empty log file (using a different store_id to ensure it's empty)
        let empty_store_id = store_id + 1;
        let empty_filename = format!("{}.data", empty_store_id);
        let empty_file_path = dir.path().join(&empty_filename);
        let empty_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&empty_file_path)
            .expect(&format!(
                "Failed to open empty file: {}",
                empty_file_path.display()
            ));
        let empty_store = Filestore::open_with_file(empty_file, empty_store_id);
        let mut empty_iter = LogFileIter::new(empty_store);
        assert!(empty_iter.next().is_none());
    }
}
