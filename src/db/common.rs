use serde::{Deserialize, Serialize};

pub type Key = String;
pub type Value = String;



pub struct KeyQuery {
    pub key: Key,
    pub op_id: OpId,
}

pub type KVOpRef<'a> = (&'a OpId, &'a Key, &'a OpType);
pub enum Error {}

#[derive(Serialize, Deserialize)]
pub struct KVOpertion {
    pub id: OpId,
    pub key: String,
    pub op: OpType,
}
impl KVOpertion {
    pub fn new(id: OpId, key: String, op: OpType) -> Self {
        KVOpertion {
            id: id,
            key: key.to_string(),
            op: op,
        }
    }
}
pub type Result<T> = std::result::Result<T, Error>;
// every Key in db has a unique id
pub type OpId = i64;
#[derive(Debug,Serialize,Deserialize)]
pub enum OpType {
    Write(String),
    Delete,
}

mod test {
    use super::OpType;

    fn test() {
        let t = OpType::Write("234".to_string());
    }
}
