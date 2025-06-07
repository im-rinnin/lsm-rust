mod db;

#[cfg(test)]
mod test {

    #[test]
    fn do_test_cov() {
        // test_cov(0);
    }
    #[test]
    fn test_binary_search() {

        // Test cases where the target is found
        // Test with single element vector
    }
}

fn main() {
    use crossbeam_skiplist::SkipMap;

    let map = SkipMap::new();

    // Insert key-value pairs
    map.insert("apple", 1);
    map.insert("banana", 2);
    map.insert("cherry", 3);

    // Retrieve values
    if let Some(value) = map.get("banana") {
        println!("Value for banana: {}", value.value());
    }

    // Iterate over the map
    println!("All elements in the map:");
    for entry in map.iter() {
        println!("Key: {}, Value: {}", entry.key(), entry.value());
    }

    // Remove a key-value pair
    map.remove("apple");

    println!("After removing apple:");
    for entry in map.iter() {
        println!("Key: {}, Value: {}", entry.key(), entry.value());
    }
}
