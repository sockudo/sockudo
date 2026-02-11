// Example to understand oxidelta output format
// Run with: cargo run --example xdelta_format_test

fn main() {
    println!("=== Testing Oxidelta Format ===\n");

    // Test 1: Simple example
    let original = &[1, 2, 3, 4, 5, 6, 7];
    let modified = &[1, 2, 4, 4, 7, 6, 7];

    let mut delta = Vec::new();
    oxidelta::compress::encoder::encode_all(
        &mut delta,
        original,
        modified,
        oxidelta::compress::encoder::CompressOptions::default(),
    )
    .unwrap();

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

    let reconstructed = oxidelta::compress::decoder::decode_all(original, &delta).unwrap();
    println!("Reconstructed: {:?}", reconstructed);
    assert_eq!(reconstructed, modified);
    println!("✓ Decode successful\n");

    // Test 2: Identical files
    let data = b"Same data";
    let mut delta = Vec::new();
    oxidelta::compress::encoder::encode_all(
        &mut delta,
        data,
        data,
        oxidelta::compress::encoder::CompressOptions::default(),
    )
    .unwrap();

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

    let reconstructed = oxidelta::compress::decoder::decode_all(data, &delta).unwrap();
    assert_eq!(reconstructed, data);
    println!("✓ Decode successful\n");

    // Test 3: Hello World
    let original = b"Hello, World!";
    let modified = b"Hello, Rust!";

    let mut delta = Vec::new();
    oxidelta::compress::encoder::encode_all(
        &mut delta,
        original,
        modified,
        oxidelta::compress::encoder::CompressOptions::default(),
    )
    .unwrap();

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

    let reconstructed = oxidelta::compress::decoder::decode_all(original, &delta).unwrap();
    println!(
        "Reconstructed: {:?}",
        String::from_utf8_lossy(&reconstructed)
    );
    assert_eq!(reconstructed, modified);
    println!("✓ Decode successful\n");

    println!("=== All tests passed! ===");
}
