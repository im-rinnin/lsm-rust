#![allow(unused)]
use std::collections::HashMap;

mod db;
fn main() {
    let mut a =Vec::with_capacity(1024);
    bincode::serialized_size(&3);
    bincode::serialize_into(&mut a,&3).unwrap();
    bincode::serialize_into(&mut a,&3).unwrap();
    println!("{:?}",a);
}
