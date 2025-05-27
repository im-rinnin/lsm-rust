use std::{
    cell::RefCell,
    collections::HashMap,
    fs::File,
    io::{Cursor, Read, Write},
    usize,
};

use super::common::Result;
pub type StoreId = String;
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
    fn create(id: StoreId) -> Self;
}

pub struct Memstore {
    store: RefCell<Cursor<Vec<u8>>>,
}
pub struct Filestore {
    f: File,
}

impl Memstore {
    pub fn new(id: &StoreId) -> Self {
        Memstore {
            store: RefCell::new(Cursor::new(Vec::new())),
        }
    }
}

impl Store for Memstore {
    fn close(self) {}
    fn create(id: StoreId) -> Self {
        unimplemented!()
    }

    fn len(&self) -> usize {
        self.store.borrow().position() as usize
    }
    fn flush(&mut self) {}
    fn append(&mut self, data: &[u8]) {
        self.store.borrow_mut().write(data);
    }
    fn read_at(&self, buf: &mut [u8], offset: usize) {
        let position = self.store.borrow().position();
        self.store.borrow_mut().set_position(offset as u64);
        let res = self.store.borrow_mut().read(buf).unwrap();
        self.store.borrow_mut().set_position(position);
    }
    fn seek(&mut self, position: usize) {
        assert!(self.store.borrow().position() as usize <= position);
        let current_pos = self.store.borrow().position() as usize;
        if current_pos < position {
            let padding_size = position - current_pos;
            self.store
                .borrow_mut()
                .write_all(&vec![0u8; padding_size])
                .unwrap();
        }
        self.store.borrow_mut().set_position(position as u64);
    }
}

impl Filestore {
    fn open(dir_path: String) -> Self {
        let res = File::open(dir_path);
        Filestore { f: res.unwrap() }
    }
}
impl Store for Filestore {
    fn flush(&mut self) {
        self.f.sync_data();
    }
    fn create(id: StoreId) -> Self {
        unimplemented!()
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

    use serde::{Deserialize, Serialize};

    use super::{Memstore, Store};
    #[derive(Serialize, Deserialize, Debug)]
    struct TestSerde {
        pub a: i32,
        pub b: String,
        pub c: Vec<i32>,
    }

    #[test]
    fn test_read_write_memsotre() {
        let id = "1".to_string();
        let mut m = Memstore::new(&id);
        let id = String::from("1");
        let t = TestSerde {
            a: 1,
            b: "sdfs".to_string(),
            c: vec![1, 2, 3],
        };
        let mut a = Vec::new();
        bincode::serialize_into(&mut a, &t);
        bincode::serialize_into(&mut a, &t);
        m.append(&a);
        let mut data = vec![0; a.len()];
        m.read_at(data.as_mut_slice(), 0);
        let mut s = data.as_slice();
        let d: TestSerde = bincode::deserialize_from(&mut s).unwrap();
        let t: TestSerde = bincode::deserialize_from(&mut s).unwrap();
    }

    #[test]
    fn test_read_at_memstore() {
        let id = "test_read_at".to_string();
        let mut m = Memstore::new(&id);
        let data = b"0123456789abcdef";
        m.append(data);
        let original_pos = m.store.borrow().position();
        assert_eq!(original_pos, data.len() as u64);

        // Read from offset 5, length 4
        let mut read_buf = [0u8; 4];
        m.read_at(&mut read_buf, 5);
        assert_eq!(&read_buf, b"5678");
        // Check position is restored
        assert_eq!(m.store.borrow().position(), original_pos);

        // Read from offset 0, length 10
        let mut read_buf_2 = [0u8; 10];
        m.read_at(&mut read_buf_2, 0);
        assert_eq!(&read_buf_2, b"0123456789");
        assert_eq!(m.store.borrow().position(), original_pos);
    }
    #[test]
    fn test_write_at_memstore() {
        let id = "test_write_at".to_string();
        let mut m = Memstore::new(&id);
        let initial_data = b"initial";
        m.append(initial_data);
        let initial_pos = m.store.borrow().position();
        assert_eq!(initial_pos, initial_data.len() as u64);

        // Test seeking forward
        let seek_pos = initial_pos as usize + 10;
        m.seek(seek_pos);
        assert_eq!(m.store.borrow().position(), seek_pos as u64);

        // Test seeking to current position (should work)
        m.seek(seek_pos);
        assert_eq!(m.store.borrow().position(), seek_pos as u64);
    }

    #[test]
    #[should_panic]
    fn test_write_at_memstore_panic() {
        let mut m = Memstore::new(&"panic_test".to_string());
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
        };
        assert_eq!(filestore.len(), 0);

        // Test with some data
        fs::write(path, b"test data").unwrap();
        let filestore = Filestore {
            f: File::open(path).unwrap(),
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

        let mut filestore = Filestore { f: file };

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

        let mut filestore = Filestore { f: file };

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

        let mut filestore = Filestore { f: file };

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
        let id = "test_pad".to_string();
        let mut m = Memstore::new(&id);

        // Write initial data
        m.append(b"initial");
        assert_eq!(m.store.borrow().position(), 7);

        // Seek forward with padding
        m.seek(10);
        assert_eq!(m.store.borrow().position(), 10);

        // Verify padding was written
        let mut buf = vec![0; 10];
        m.read_at(&mut buf, 0);
        assert_eq!(&buf[0..7], b"initial");
        assert_eq!(&buf[7..10], &[0u8; 3]);

        // Verify position is maintained
        assert_eq!(m.store.borrow().position(), 10);
    }
}
