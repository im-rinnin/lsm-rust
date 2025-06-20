use crate::db::key::KeySlice;
use crate::db::key::ValueSlice;
use std::{
    cmp::Ordering,
    io::{Cursor, Read, Write},
    rc::Rc,
    thread::sleep,
    usize,
};

use byteorder::WriteBytesExt;
use byteorder::{LittleEndian, ReadBytesExt};
use bytes::Bytes;
use std::mem::size_of;

use super::key::KeyBytes;
use super::key::ValueByte;
use super::key::{KeyVec, ValueVec}; // Added for kv_opertion_len // Added for kv_opertion_len

#[derive(Clone)] // Add Clone derive
pub struct KeyQuery {
    pub key: KeyBytes,
    pub op_id: OpId,
}
pub type SearchResult = Option<(OpType, OpId)>;

pub type Buffer = Cursor<Vec<u8>>;
pub fn new_buffer(size: usize) -> Buffer {
    Cursor::new(vec![0; size])
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct KVOpertion {
    pub id: OpId,
    pub key: KeyBytes,
    pub op: OpType,
}
impl KVOpertion {
    pub fn new(id: OpId, key: KeyVec, op: OpType) -> Self {
        KVOpertion {
            id: id,
            key: KeyBytes::from_vec(key.into_inner()),
            op: op,
        }
    }

    pub fn encode_size(&self) -> usize {
        // id (u64) + key_len (u64) + key_data
        let mut size = size_of::<u64>() + size_of::<u64>() + self.key.len();
        // op_type (u8)
        size += size_of::<u8>();
        if let OpType::Write(v) = &self.op {
            // value_len (u64) + value_data
            size += size_of::<u64>() + v.len();
        }
        size
    }
    pub fn decode(r: Bytes) -> (Self, usize) {
        let mut cursor = Cursor::new(r.as_ref());

        let id = cursor.read_u64::<LittleEndian>().unwrap();
        let key_len = cursor.read_u64::<LittleEndian>().unwrap() as usize;
        let key_start_offset = cursor.position() as usize;
        let key_end_offset = key_start_offset + key_len;
        let key_data_bytes = r.slice(key_start_offset..key_end_offset);
        let key = KeyBytes::from_bytes(key_data_bytes);

        cursor.set_position(key_end_offset as u64);

        let op_type_byte = cursor.read_u8().unwrap();
        let op = match op_type_byte {
            0 => OpType::Delete,
            1 => {
                let value_len = cursor.read_u64::<LittleEndian>().unwrap() as usize;
                let value_start_offset = cursor.position() as usize;
                let value_end_offset = value_start_offset + value_len;
                let value_data_bytes = r.slice(value_start_offset..value_end_offset);
                cursor.set_position(value_end_offset as u64);
                OpType::Write(ValueByte::from_bytes(value_data_bytes))
            }
            _ => panic!("Unknown OpType byte: {}", op_type_byte),
        };
        let end_offset = cursor.position() as usize;
        (KVOpertion { id, key, op }, end_offset)
    }
    pub fn encode<W: Write>(&self, mut w: &mut W) {
        w.write_u64::<LittleEndian>(self.id).unwrap();
        let key_len = self.key.len() as u64;
        w.write_u64::<LittleEndian>(key_len).unwrap();
        w.write_all(self.key.as_ref()).unwrap();
        match &self.op {
            OpType::Delete => {
                w.write_u8(0).unwrap();
            }
            OpType::Write(v) => {
                w.write_u8(1).unwrap();
                let v_len = v.len() as u64;
                w.write_u64::<LittleEndian>(v_len).unwrap();
                w.write_all(v.as_ref()).unwrap();
            }
        }
    }
}
// every Key in db has a unique id
pub type OpId = u64;
pub const MAX_OP_ID: OpId = u64::MAX; // Represents the maximum possible OpId
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)] // Added Clone
pub enum OpType {
    Write(ValueByte),
    Delete,
}

pub struct KViterAgg<'a> {
    iters: Vec<&'a mut dyn Iterator<Item = KVOpertion>>,
    iters_next: Vec<Option<KVOpertion>>,
}

impl<'a> KViterAgg<'a> {
    pub fn new(iters: Vec<&'a mut dyn Iterator<Item = KVOpertion>>) -> Self {
        let mut res = KViterAgg {
            iters,
            iters_next: Vec::new(),
        };
        for it in res.iters.iter_mut() {
            res.iters_next.push(it.next());
        }
        res
    }
}

impl<'a> Iterator for KViterAgg<'a> {
    type Item = KVOpertion;
    fn next(&mut self) -> Option<Self::Item> {
        let mut has_next = false;
        for (i, n) in self.iters_next.iter_mut().enumerate() {
            if n.is_some() {
                has_next = true;
            } else {
                *n = self.iters.get_mut(i).unwrap().next();
                if n.is_some() {
                    has_next = true;
                }
            }
        }

        if !has_next {
            return None;
        }

        let mut smallest_key_index: Option<usize> = None;

        for (index, current_kv_option) in self.iters_next.iter().enumerate() {
            if let Some(current_kv) = current_kv_option {
                match smallest_key_index {
                    None => {
                        smallest_key_index = Some(index);
                    }
                    Some(s_idx) => {
                        let smallest_kv = self.iters_next[s_idx].as_ref().unwrap();
                        let cmp_res = current_kv.key.cmp(&smallest_kv.key);
                        match cmp_res {
                            Ordering::Less => {
                                smallest_key_index = Some(index);
                            }
                            Ordering::Equal => {
                                if current_kv.id > smallest_kv.id {
                                    smallest_key_index = Some(index);
                                }
                            }
                            Ordering::Greater => {}
                        }
                    }
                }
            }
        }

        if let Some(s_idx) = smallest_key_index {
            let result_kv = self.iters_next[s_idx].take(); // Take the value out
            for (index, current_kv_option) in self.iters_next.iter_mut().enumerate() {
                if index != s_idx {
                    if let Some(current_kv) = current_kv_option {
                        if current_kv.key == result_kv.as_ref().unwrap().key {
                            *current_kv_option = None; // Invalidate other KVs with the same key
                        }
                    }
                }
            }
            result_kv
        } else {
            None
        }
    }
}

pub mod test {
    use crate::db::common::{new_buffer, KVOpertion, KViterAgg};

    use super::OpType;

    #[test]
    fn test_kv_iter_agg() {
        // input iter :diff length ,diff order ,no duplicte key in same iter but in diff
        // iter,ordered in same iter
        // [(id,key)]
        let a = vec![(1, 1), (3, 3), (4, 4), (7, 7), (10, 99)];
        let b = vec![(2, 2), (5, 3), (6, 6), (28, 8), (9, 9)];
        let c = vec![(11, 3), (18, 8)];

        let f = |a: &(u64, u64)| -> KVOpertion {
            let id = a.0;
            let key = a.1.to_string();
            KVOpertion::new(
                id,
                key.as_bytes().into(),
                OpType::Write(key.to_string().as_bytes().into()),
            )
        };

        let a_ops: Vec<KVOpertion> = a.iter().map(|x| f(x)).collect();
        let mut a_iter_ref = a_ops.into_iter();
        let b_ops: Vec<_> = b.iter().map(|x| f(x)).collect();
        let mut b_iter_ref = b_ops.into_iter();
        let c_ops: Vec<_> = c.iter().map(|x| f(x)).collect();
        let mut c_iter_ref = c_ops.into_iter();

        let kv_iter = KViterAgg::new(vec![&mut a_iter_ref, &mut b_iter_ref, &mut c_iter_ref]);
        let expect = vec![
            (1, 1),
            (2, 2),
            (11, 3),
            (4, 4),
            (6, 6),
            (7, 7),
            (28, 8),
            (9, 9),
            (10, 99),
        ];
        let output_ids: Vec<(u64, u64)> = kv_iter
            // Convert KeyVec to String before parsing
            .map(|i| {
                (
                    i.id,
                    String::from_utf8_lossy(i.key.inner())
                        .parse::<u64>()
                        .unwrap(),
                )
            })
            .collect();
        assert_eq!(output_ids, expect);
    }

    #[test]
    fn test_kv_operation_size() {
        let op = KVOpertion::new(1, "123".as_bytes().into(), OpType::Delete);

        assert_eq!(20, op.encode_size());

        let op = KVOpertion::new(
            1,
            "123".as_bytes().into(),
            OpType::Write("234".as_bytes().into()),
        );
        assert_eq!(31, op.encode_size());
    }
    #[test]
    fn test_kv_operation_encode() {
        let op = KVOpertion::new(1, "123".to_string().as_bytes().into(), OpType::Delete);
        let mut v = new_buffer(1024);
        op.encode(&mut v);
        assert_eq!(v.position() as usize, op.encode_size());
        v.set_position(0);
        let (op_res, offset) = KVOpertion::decode(v.into_inner().into());
        assert_eq!(offset, op_res.encode_size());
        assert_eq!(op_res, op);

        let op = KVOpertion::new(
            1,
            "123".to_string().as_bytes().into(),
            OpType::Write("234".as_bytes().into()),
        );
        let mut v = new_buffer(1024);
        op.encode(&mut v);
        assert_eq!(v.position() as usize, op.encode_size());
        let (op_res, offset) = KVOpertion::decode(v.into_inner().into());
        assert_eq!(offset, op_res.encode_size());
        assert_eq!(op_res, op);
    }
}
