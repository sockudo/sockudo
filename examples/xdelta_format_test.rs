// Example to understand external xdelta3 crate's output format
// Run with: cargo run --example xdelta_format_test

fn main() {
    println!("=== Testing External xdelta3 Format ===\n");

    // Test 1: Simple example
    let original = &[1, 2, 3, 4, 5, 6, 7];
    let modified = &[1, 2, 4, 4, 7, 6, 7];

    let delta = xdelta3::encode(modified, original).unwrap();

    println!("Simple test:");
    println!("Original: {:?}", original);
    println!("Modified: {:?}", modified);
    println!("Delta bytes: {:?}", delta);
    println!(
        "Delta hex: {}",
        delta
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect::<Vec<_>>()
            .join(" ")
    );
    println!("Delta length: {}", delta.len());

    let reconstructed = xdelta3::decode(&delta, original).unwrap();
    println!("Reconstructed: {:?}", reconstructed);
    assert_eq!(reconstructed, modified);
    println!("✓ Decode successful\n");

    // Test 2: Identical files
    let data = b"Same data";
    let delta = xdelta3::encode(data, data).unwrap();

    println!("Identical files:");
    println!("Data: {:?}", data);
    println!("Delta bytes: {:?}", delta);
    println!(
        "Delta hex: {}",
        delta
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect::<Vec<_>>()
            .join(" ")
    );
    println!("Delta length: {}", delta.len());

    let reconstructed = xdelta3::decode(&delta, data).unwrap();
    assert_eq!(reconstructed, data);
    println!("✓ Decode successful\n");

    // Test 3: Hello World
    let original = b"Hello, World!";
    let modified = b"Hello, Rust!";

    let delta = xdelta3::encode(modified, original).unwrap();

    println!("Hello World test:");
    println!("Original: {:?}", String::from_utf8_lossy(original));
    println!("Modified: {:?}", String::from_utf8_lossy(modified));
    println!("Delta bytes: {:?}", delta);
    println!(
        "Delta hex: {}",
        delta
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect::<Vec<_>>()
            .join(" ")
    );
    println!("Delta length: {}", delta.len());

    let reconstructed = xdelta3::decode(&delta, original).unwrap();
    println!(
        "Reconstructed: {:?}",
        String::from_utf8_lossy(&reconstructed)
    );
    assert_eq!(reconstructed, modified);
    println!("✓ Decode successful\n");

    println!("=== All tests passed! ===");
}
