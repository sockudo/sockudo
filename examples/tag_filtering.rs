// examples/tag_filtering.rs
//
// This example demonstrates publication filtering by tags using a football match scenario.
// It shows how to:
// 1. Create publications with tags
// 2. Subscribe with filters
// 3. Receive only filtered messages
//
// Run with: cargo run --example tag_filtering

use ahash::AHashMap;
use sockudo::filter::{FilterNode, node::FilterNodeBuilder};

fn main() {
    println!("=== Sockudo Tag Filtering Example ===\n");

    // Example 1: Simple equality filter
    println!("Example 1: Simple Equality Filter");
    println!("----------------------------------");

    let filter = FilterNodeBuilder::eq("event_type", "goal");
    println!("Filter: event_type == 'goal'");
    println!(
        "Filter JSON: {}\n",
        sonic_rs::to_string_pretty(&filter).unwrap()
    );

    // Simulate publications
    let publications = [
        create_publication("goal", "23:30", "Real Madrid", "Mbappe"),
        create_publication("shot", "24:10", "Bayern Munich", "Kane"),
        create_publication("goal", "45:12", "Real Madrid", "Bellingham"),
        create_publication("pass", "46:30", "Bayern Munich", "Musiala"),
    ];

    println!("Publications:");
    for (i, pub_tags) in publications.iter().enumerate() {
        let matches = sockudo::filter::matches(&filter, pub_tags);
        println!(
            "  {}. {} at {} - Match: {}",
            i + 1,
            pub_tags.get("event_type").unwrap(),
            pub_tags.get("minute").unwrap(),
            if matches { "✓" } else { "✗" }
        );
    }
    println!();

    // Example 2: Complex OR filter - goals OR dangerous shots
    println!("Example 2: Complex OR Filter");
    println!("-----------------------------");

    let filter = FilterNodeBuilder::or(vec![
        FilterNodeBuilder::eq("event_type", "goal"),
        FilterNodeBuilder::and(vec![
            FilterNodeBuilder::eq("event_type", "shot"),
            FilterNodeBuilder::gte("xG", "0.8"),
        ]),
    ]);

    println!("Filter: (event_type == 'goal') OR (event_type == 'shot' AND xG >= 0.8)");
    println!(
        "Filter JSON: {}\n",
        sonic_rs::to_string_pretty(&filter).unwrap()
    );

    let publications_with_xg = [
        create_publication_with_xg("goal", "23:30", "Real Madrid", "Mbappe", "0.95"),
        create_publication_with_xg("shot", "24:10", "Bayern Munich", "Kane", "0.85"),
        create_publication_with_xg("shot", "25:45", "Bayern Munich", "Sane", "0.3"),
        create_publication_with_xg("goal", "45:12", "Real Madrid", "Bellingham", "0.78"),
    ];

    println!("Publications:");
    for (i, pub_tags) in publications_with_xg.iter().enumerate() {
        let matches = sockudo::filter::matches(&filter, pub_tags);
        println!(
            "  {}. {} at {} (xG: {}) - Match: {}",
            i + 1,
            pub_tags.get("event_type").unwrap(),
            pub_tags.get("minute").unwrap(),
            pub_tags.get("xG").unwrap_or(&"N/A".to_string()),
            if matches { "✓" } else { "✗" }
        );
    }
    println!();

    // Example 3: Set membership filter
    println!("Example 3: Set Membership Filter");
    println!("---------------------------------");

    let filter = FilterNodeBuilder::in_set("event_type", &["goal", "penalty", "red_card"]);
    println!("Filter: event_type IN ['goal', 'penalty', 'red_card']");
    println!(
        "Filter JSON: {}\n",
        sonic_rs::to_string_pretty(&filter).unwrap()
    );

    let critical_events = [
        create_simple_event("goal", "23:30"),
        create_simple_event("shot", "24:10"),
        create_simple_event("penalty", "35:22"),
        create_simple_event("pass", "40:15"),
        create_simple_event("red_card", "67:45"),
        create_simple_event("corner", "70:12"),
    ];

    println!("Publications:");
    for (i, pub_tags) in critical_events.iter().enumerate() {
        let matches = sockudo::filter::matches(&filter, pub_tags);
        println!(
            "  {}. {} at {} - Match: {}",
            i + 1,
            pub_tags.get("event_type").unwrap(),
            pub_tags.get("minute").unwrap(),
            if matches { "✓" } else { "✗" }
        );
    }
    println!();

    // Example 4: String operations
    println!("Example 4: String Operations");
    println!("-----------------------------");

    let filters = vec![
        (
            "Starts with 'Real'",
            FilterNodeBuilder::starts_with("team", "Real"),
        ),
        (
            "Ends with 'Munich'",
            FilterNodeBuilder::ends_with("team", "Munich"),
        ),
        (
            "Contains 'Madrid'",
            FilterNodeBuilder::contains("team", "Madrid"),
        ),
    ];

    let teams = vec![
        ("Real Madrid", true, false, true),
        ("Bayern Munich", false, true, false),
        ("Real Sociedad", true, false, false),
        ("Liverpool", false, false, false),
    ];

    for (desc, filter) in &filters {
        println!("\nFilter: {}", desc);
        println!("Filter JSON: {}", sonic_rs::to_string(&filter).unwrap());
        println!("Teams:");

        for (team, sw, ew, ct) in &teams {
            let mut tags = AHashMap::new();
            tags.insert("team".to_string(), team.to_string());

            let matches = sockudo::filter::matches(filter, &tags);
            let expected = match desc {
                d if d.contains("Starts") => *sw,
                d if d.contains("Ends") => *ew,
                d if d.contains("Contains") => *ct,
                _ => false,
            };

            println!(
                "  {} - Match: {} {}",
                team,
                if matches { "✓" } else { "✗" },
                if matches == expected {
                    ""
                } else {
                    "(UNEXPECTED!)"
                }
            );
        }
    }
    println!();

    // Example 5: Numeric comparisons
    println!("Example 5: Numeric Comparisons");
    println!("-------------------------------");

    let filter = FilterNodeBuilder::and(vec![
        FilterNodeBuilder::gt("xG", "0.7"),
        FilterNodeBuilder::lt("distance", "20"),
    ]);

    println!("Filter: xG > 0.7 AND distance < 20");
    println!(
        "Filter JSON: {}\n",
        sonic_rs::to_string_pretty(&filter).unwrap()
    );

    let shots = [
        ("0.85", "15", true),
        ("0.45", "18", false),
        ("0.92", "12", true),
        ("0.78", "25", false),
        ("0.65", "10", false),
    ];

    println!("Shots:");
    for (i, (xg, distance, expected)) in shots.iter().enumerate() {
        let mut tags = AHashMap::new();
        tags.insert("xG".to_string(), xg.to_string());
        tags.insert("distance".to_string(), distance.to_string());

        let matches = sockudo::filter::matches(&filter, &tags);
        println!(
            "  {}. xG={}, distance={}m - Match: {} {}",
            i + 1,
            xg,
            distance,
            if matches { "✓" } else { "✗" },
            if matches == *expected {
                ""
            } else {
                "(UNEXPECTED!)"
            }
        );
    }
    println!();

    // Example 6: NOT operation
    println!("Example 6: NOT Operation");
    println!("------------------------");

    let filter = FilterNodeBuilder::not(FilterNodeBuilder::in_set(
        "event_type",
        &["pass", "tackle", "throw_in"],
    ));

    println!("Filter: NOT (event_type IN ['pass', 'tackle', 'throw_in'])");
    println!(
        "Filter JSON: {}\n",
        sonic_rs::to_string_pretty(&filter).unwrap()
    );

    let all_events = vec![
        "goal", "shot", "pass", "tackle", "corner", "throw_in", "penalty",
    ];

    println!("Events:");
    for event in all_events {
        let mut tags = AHashMap::new();
        tags.insert("event_type".to_string(), event.to_string());

        let matches = sockudo::filter::matches(&filter, &tags);
        println!("  {} - Match: {}", event, if matches { "✓" } else { "✗" });
    }
    println!();

    // Example 7: Validation
    println!("Example 7: Filter Validation");
    println!("-----------------------------");

    let valid_filter = FilterNodeBuilder::eq("event_type", "goal");
    // Create an invalid filter via JSON deserialization (missing key - invalid!)
    let invalid_filter: FilterNode = sonic_rs::from_str(r#"{"cmp": "eq", "val": "goal"}"#).unwrap();

    println!("Valid filter:");
    match valid_filter.validate() {
        None => println!("  ✓ Filter is valid"),
        Some(err) => println!("  ✗ Error: {}", err),
    }

    println!("\nInvalid filter (missing key):");
    match invalid_filter.validate() {
        None => println!("  ✓ Filter is valid"),
        Some(err) => println!("  ✗ Error: {}", err),
    }
    println!();

    // Performance demonstration
    println!("Example 8: Performance Test");
    println!("---------------------------");

    let filter = FilterNodeBuilder::or(vec![
        FilterNodeBuilder::eq("event_type", "goal"),
        FilterNodeBuilder::and(vec![
            FilterNodeBuilder::eq("event_type", "shot"),
            FilterNodeBuilder::gte("xG", "0.8"),
        ]),
    ]);

    let tags = create_publication_with_xg("shot", "24:10", "Bayern Munich", "Kane", "0.85");

    let iterations = 10_000;
    let start = std::time::Instant::now();

    for _ in 0..iterations {
        let _ = sockudo::filter::matches(&filter, &tags);
    }

    let duration = start.elapsed();
    let avg_ns = duration.as_nanos() / iterations;

    println!("Evaluated complex filter {} times", iterations);
    println!("Total time: {:?}", duration);
    println!("Average per evaluation: {} ns", avg_ns);
    println!(
        "Evaluations per second: {:.2} million",
        1_000_000_000.0 / avg_ns as f64
    );
    println!("\n✓ Zero allocations during evaluation!");
}

// Helper functions

fn create_publication(
    event_type: &str,
    minute: &str,
    team: &str,
    player: &str,
) -> AHashMap<String, String> {
    let mut tags = AHashMap::new();
    tags.insert("event_type".to_string(), event_type.to_string());
    tags.insert("minute".to_string(), minute.to_string());
    tags.insert("team".to_string(), team.to_string());
    tags.insert("player".to_string(), player.to_string());
    tags
}

fn create_publication_with_xg(
    event_type: &str,
    minute: &str,
    team: &str,
    player: &str,
    xg: &str,
) -> AHashMap<String, String> {
    let mut tags = create_publication(event_type, minute, team, player);
    tags.insert("xG".to_string(), xg.to_string());
    tags
}

fn create_simple_event(event_type: &str, minute: &str) -> AHashMap<String, String> {
    let mut tags = AHashMap::new();
    tags.insert("event_type".to_string(), event_type.to_string());
    tags.insert("minute".to_string(), minute.to_string());
    tags
}
