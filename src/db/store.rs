use std::{
    cell::RefCell,
    collections::HashMap,
    fs::File,
    io::{Cursor, Read, Write},
    usize,
};

use super::common::Result;

pub type StoreId = u64;
// append only data store
pub trait Store {
    fn flush(&mut self);
    fn append(&mut self, data: &[u8]);
    fn read_at(&self, buf: &mut [u8], offset: usize);
    // store is append only, so seek positon should >= current positon
    // pad the space with zero in [current postion,position)
    fn seek(&mut self, position: usize);
    fn len(&self) -> usize;
    fn close(self);
    fn open(id: StoreId) -> Self;
    fn id(&self) -> StoreId;
}

pub struct Memstore {
    store: Vec<u8>,
    id: StoreId,
}
pub struct Filestore {
    f: File,
    id: StoreId,
}

impl Memstore {}

impl Store for Memstore {
    fn close(self) {}
    fn open(id: StoreId) -> Self {
        Memstore {
            store: Vec::new(),
            id,
        }
    }
    fn id(&self) -> StoreId {
        self.id
    }

    fn len(&self) -> usize {
        self.store.len()
    }
    fn flush(&mut self) {}
    fn append(&mut self, data: &[u8]) {
        self.store.extend_from_slice(data);
    }
    fn read_at(&self, buf: &mut [u8], offset: usize) {
        let end_offset = offset + buf.len();
        if offset >= self.store.len() {
            // If offset is beyond current data, fill buffer with zeros
            buf.fill(0);
            return;
        }
        let bytes_to_read = (self.store.len() - offset).min(buf.len());
        buf[..bytes_to_read].copy_from_slice(&self.store[offset..offset + bytes_to_read]);
        // If the buffer is larger than available data, pad the rest with zeros
        if bytes_to_read < buf.len() {
            buf[bytes_to_read..].fill(0);
        }
    }
    fn seek(&mut self, position: usize) {
        let current_len = self.store.len();
        assert!(
            current_len <= position,
            "Seeking backwards is not allowed in append-only store logic"
        );
        if position > current_len {
            // Pad with zeros to reach the desired position
            self.store.resize(position, 0);
        }
        // For Vec<u8>, 'seeking' just means ensuring its length is at least 'position'.
        // Subsequent appends will start from 'position'.
    }
}
impl Filestore{
    pub fn open_with(f: File, id: StoreId) -> Self {
        Filestore { f, id }
    }
}

impl Store for Filestore {
    fn flush(&mut self) {
        self.f.sync_data();
    }
    fn id(&self) -> StoreId {
        self.id
    }
    fn open(id: StoreId) -> Self {
        use std::fs::OpenOptions;
        let filename = format!("{}.data", id); // Using .data as a generic extension for store files
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true) // Create the file if it doesn't exist
            .open(&filename)
            .expect(&format!("Failed to open file: {}", filename));
        Filestore { f, id }
    }
    fn len(&self) -> usize {
        let meta = self.f.metadata().unwrap();
        meta.len() as usize
    }
    fn close(self) {
        self.f.sync_all();
    }
    fn append(&mut self, data: &[u8]) {
        self.f.write_all(data).unwrap()
    }
    fn read_at(&self, buf: &mut [u8], offset: usize) {
        use std::os::unix::fs::FileExt;
        let bytes_read = self.f.read_at(buf, offset as u64).unwrap();
        // pad zero
        if bytes_read < buf.len() {
            buf[bytes_read..].fill(0);
        }
    }

    fn seek(&mut self, position: usize) {
        use std::io::{Seek, SeekFrom};

        let current_pos = self.f.seek(SeekFrom::Current(0)).unwrap() as usize;
        assert!(
            current_pos <= position,
            "Seeking backwards is not allowed in append-only store logic"
        );

        let file_len = self.len();

        if position > file_len {
            // Need to pad
            let padding_size = position - file_len;
            // Seek to the end to append padding
            self.f.seek(SeekFrom::End(0)).unwrap();
            // Write zeros
            // Consider writing in chunks for large padding? For now, a single write.
            let padding = vec![0u8; padding_size];
            self.f.write_all(&padding).unwrap();
        }
        // Ensure the cursor is at the desired final position
        self.f.seek(SeekFrom::Start(position as u64)).unwrap();
    }
}

#[cfg(test)]
mod test {
    use super::Filestore;
    use std::{
        fs::File,
        io::{Read, Write},
        str::FromStr,
    };
    use tempfile::NamedTempFile;

    use super::{Memstore, Store};

    #[test]
    fn test_read_at_memstore() {
        let id = 2u64; // Using a unique ID for this test
        let mut m = Memstore::open(id);
        let data = b"0123456789abcdef";
        m.append(data);
        let original_len = m.store.len();
        assert_eq!(original_len, data.len());

        // Read from offset 5, length 4
        let mut read_buf = [0u8; 4];
        m.read_at(&mut read_buf, 5);
        assert_eq!(&read_buf, b"5678");
        // Check length is not changed by read_at
        assert_eq!(m.store.len(), original_len);

        // Read from offset 0, length 10
        let mut read_buf_2 = [0u8; 10];
        m.read_at(&mut read_buf_2, 0);
        assert_eq!(&read_buf_2, b"0123456789");
        assert_eq!(m.store.len(), original_len);
    }
    #[test]
    fn test_write_at_memstore() {
        let id = 3u64; // Using a unique ID for this test
        let mut m = Memstore::open(id);
        let initial_data = b"initial";
        m.append(initial_data);
        let initial_len = m.store.len();
        assert_eq!(initial_len, initial_data.len());

        // Test seeking forward
        let seek_pos = initial_len + 10;
        m.seek(seek_pos);
        assert_eq!(m.store.len(), seek_pos);

        // Test seeking to current position (should work)
        m.seek(seek_pos);
        assert_eq!(m.store.len(), seek_pos);
    }

    #[test]
    #[should_panic]
    fn test_write_at_memstore_panic() {
        let mut m = Memstore::open(4u64); // Using a unique ID for this test
        m.append(b"some data");
        m.seek(1); // Seek backwards, should panic due to assert
    }
    #[test]
    fn test_filestore_len() {
        use std::fs::{self, File};
        use tempfile::NamedTempFile;

        let tmp_file = NamedTempFile::new().unwrap();
        let path = tmp_file.path();

        // Test empty file
        let filestore = Filestore {
            f: File::open(path).unwrap(),
            id: 5u64, // Assigning a dummy u64 ID for test
        };
        assert_eq!(filestore.len(), 0);

        // Test with some data
        fs::write(path, b"test data").unwrap();
        let filestore = Filestore {
            f: File::open(path).unwrap(),
            id: 5u64, // Assigning a dummy u64 ID for test
        };
        assert_eq!(filestore.len(), 9);
    }

    #[test]
    fn test_filestore_append() {
        use std::fs::OpenOptions;

        let tmp_file = NamedTempFile::new().unwrap();
        let path = tmp_file.path().to_path_buf();

        // Need to open with write and create permissions for append
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .unwrap();

        let mut filestore = Filestore {
            f: file,
            id: 6u64, // Assigning a dummy u64 ID for test
        };

        // Append first part
        let data1 = b"hello";
        filestore.append(data1);
        filestore.flush(); // Ensure data is written for len() and read_at()
        assert_eq!(filestore.len(), data1.len());

        // Append second part
        let data2 = b" world";
        filestore.append(data2);
        filestore.flush();
        let total_len = data1.len() + data2.len();
        assert_eq!(filestore.len(), total_len);

        // Check content with read_at
        let mut read_buf = vec![0u8; total_len];
        filestore.read_at(&mut read_buf, 0);
        assert_eq!(read_buf.as_slice(), b"hello world");
    }

    #[test]
    fn test_filestore_seek() {
        use std::fs::OpenOptions;

        let tmp_file = NamedTempFile::new().unwrap();
        let path = tmp_file.path().to_path_buf();

        // Open with read, write, create
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .unwrap();

        let mut filestore = Filestore {
            f: file,
            id: 7u64, // Assigning a dummy u64 ID for test
        };

        // Write 10 bytes
        let initial_data = b"0123456789";
        filestore.append(initial_data);
        filestore.flush();
        assert_eq!(filestore.len(), 10);

        // Seek to position 15 (pads bytes 10-14 with 0)
        let seek_pos = 15;
        filestore.seek(seek_pos);
        filestore.flush(); // Ensure padding is written
        assert_eq!(filestore.len(), seek_pos); // Length should now be 15

        // Append 5 bytes
        let appended_data = b"abcde";
        filestore.append(appended_data);
        filestore.flush();
        let final_len = seek_pos + appended_data.len(); // 15 + 5 = 20
        assert_eq!(filestore.len(), final_len);

        // Read all data and check
        let mut read_buf = vec![0u8; final_len];
        filestore.read_at(&mut read_buf, 0);

        // Check initial data
        assert_eq!(&read_buf[0..initial_data.len()], initial_data);
        // Check padding
        assert_eq!(&read_buf[initial_data.len()..seek_pos], &[0u8; 5]);
        // Check appended data
        assert_eq!(&read_buf[seek_pos..final_len], appended_data);
    }

    #[test]
    fn test_filestore_read_at() {
        use std::fs::OpenOptions;

        let tmp_file = NamedTempFile::new().unwrap();
        let path = tmp_file.path().to_path_buf();

        // Open with read, write, create
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .unwrap();

        let mut filestore = Filestore {
            f: file,
            id: 8u64, // Assigning a dummy u64 ID for test
        };

        // Append 10 bytes
        let data = b"0123456789";
        filestore.append(data);
        filestore.flush();
        assert_eq!(filestore.len(), 10);

        // Read range [5, 10) -> 5 bytes at offset 5
        let mut read_buf1 = [0u8; 5];
        filestore.read_at(&mut read_buf1, 5);
        assert_eq!(&read_buf1, b"56789");

        // Read range [5, 20) -> 15 bytes at offset 5
        // Expect "56789" followed by 10 zero bytes padding
        let mut read_buf2 = [0u8; 15];
        filestore.read_at(&mut read_buf2, 5);
        assert_eq!(&read_buf2[0..5], b"56789");
        assert_eq!(&read_buf2[5..15], &[0u8; 10]);
    }

    #[test]
    fn test_memstore_pad_in_write() {
        let id = 9u64; // Using a unique ID for this test
        let mut m = Memstore::open(id);

        // Write initial data
        m.append(b"initial");
        assert_eq!(m.store.len(), 7);

        // Seek forward with padding
        m.seek(10);
        assert_eq!(m.store.len(), 10);

        // Verify padding was written
        let mut buf = vec![0; 10];
        m.read_at(&mut buf, 0);
        assert_eq!(&buf[0..7], b"initial");
        assert_eq!(&buf[7..10], &[0u8; 3]);

        // Verify length is maintained
        assert_eq!(m.store.len(), 10);
    }

    #[test]
    fn test_filestore_open() {
        use std::env;
        use std::path::Path;
        use tempfile::tempdir;

        let test_id = 100u64; // Unique ID for this test

        // Create a temporary directory
        let dir = tempdir().expect("Failed to create temp dir");
        let original_dir = env::current_dir().expect("Failed to get current dir");

        // Change to the temporary directory
        env::set_current_dir(&dir).expect("Failed to change to temp dir");

        // Open the filestore
        let filestore = Filestore::open(test_id);

        // Verify the file exists
        let expected_filename = format!("{}.data", test_id);
        let file_path = Path::new(&expected_filename);
        assert!(file_path.exists(), "File should exist after opening");

        // Verify the ID
        assert_eq!(
            filestore.id(),
            test_id,
            "Filestore ID should match the opened ID"
        );

        // Clean up: The `dir` object will automatically delete the directory and its contents
        // when it goes out of scope.
        // Change back to the original directory
        env::set_current_dir(&original_dir).expect("Failed to change back to original dir");
    }
}
