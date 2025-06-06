use bytes::Bytes;
use std::usize;
#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone)]
pub struct Key<T: AsRef<[u8]>> {
    data: T,
}
pub type KeySlice<'a> = Key<&'a [u8]>;
pub type ValueSlice<'a> = Key<&'a [u8]>;
pub type KeyBytes = Key<Bytes>;
pub type KeyVec = Key<Vec<u8>>;
pub type ValueVec = Key<Vec<u8>>;
pub type ValueByte = Key<Bytes>;

impl KeyBytes {
    pub fn from_bytes(data: Bytes) -> Self {
        KeyBytes { data }
    }
    pub fn from_vec(data: Vec<u8>) -> Self {
        KeyBytes {
            data: Bytes::from(data),
        }
    }
}

impl<T: AsRef<[u8]>> Key<T> {
    pub fn as_ref(&self) -> &[u8] {
        self.data.as_ref()
    }
    pub fn inner(&self) -> &T {
        &self.data
    }
    pub fn into_inner(self) -> T {
        self.data
    }
    pub fn len(&self) -> usize {
        self.data.as_ref().len()
    }
    pub fn to_string(&self) -> String {
        String::from_utf8_lossy(self.data.as_ref()).into_owned()
    }
}

impl From<&'_ [u8]> for KeyBytes {
    fn from(data: &'_ [u8]) -> Self {
        KeyBytes {
            data: bytes::Bytes::copy_from_slice(data),
        }
    }
}

impl KeyVec {
    pub fn new() -> Self {
        Self { data: Vec::new() }
    }

    pub fn from_vec(data: Vec<u8>) -> Self {
        Self { data }
    }

    pub fn append(&mut self, data: &[u8]) {
        self.data.extend_from_slice(data);
    }
}
impl<'a> From<&'a [u8]> for KeyVec {
    fn from(s: &'a [u8]) -> Self {
        KeyVec { data: s.to_vec() }
    }
}

impl<'a> From<&'a [u8]> for KeySlice<'a> {
    fn from(value: &'a [u8]) -> Self {
        KeySlice { data: value }
    }
}

#[cfg(test)]
mod test {
    use super::KeySlice;
    use super::KeyVec;

    fn test() {
        let k = String::from("sdf");
        let d = k.as_str();
    }
}
