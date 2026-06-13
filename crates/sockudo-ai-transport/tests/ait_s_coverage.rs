use std::collections::BTreeSet;

#[derive(Debug)]
struct Entry<'a> {
    range: &'a str,
    status: &'a str,
    rationale: &'a str,
}

const COVERAGE: &[Entry<'_>] = &[
    Entry {
        range: "AIT-S1..AIT-S12",
        status: "implemented",
        rationale: "covered by ait_s_conformance.rs protocol/core versioned-message tests",
    },
    Entry {
        range: "AIT-S13..AIT-S18",
        status: "existing-server-tests",
        rationale: "covered by crates/sockudo-server/src/http_handler.rs mutation authz and response tests; server crate owns private HTTP handler helpers",
    },
    Entry {
        range: "AIT-S19..AIT-S25",
        status: "existing-adapter-server-tests",
        rationale: "covered by protocol_compliance, local_adapter, and http_handler idempotency tests",
    },
    Entry {
        range: "AIT-S26..AIT-S30",
        status: "implemented",
        rationale: "covered by ait_s_conformance.rs history cursor and request validation test",
    },
    Entry {
        range: "AIT-S31..AIT-S52",
        status: "existing-server-adapter-push-tests",
        rationale: "covered by existing history, rewind, recovery, annotation, and push tests in server/adapter/push crates",
    },
    Entry {
        range: "AIT-S53..AIT-S64",
        status: "implemented",
        rationale: "covered by ait_s_conformance.rs AI gate/event/header registry tests",
    },
    Entry {
        range: "AIT-S65..AIT-S68",
        status: "existing-server-tests",
        rationale: "covered by http_handler AI idempotency, op_id, and mutation ack tests",
    },
    Entry {
        range: "AIT-S69..AIT-S73",
        status: "implemented",
        rationale: "covered by ait_s_conformance.rs and sockudo-ai-transport rollup unit tests",
    },
    Entry {
        range: "AIT-S74..AIT-S77",
        status: "existing-adapter-tests",
        rationale: "covered by handler history_frames and subscription attach_serial tests",
    },
    Entry {
        range: "AIT-S78..AIT-S82",
        status: "existing-core-adapter-tests",
        rationale: "covered by capability_token and authentication/signin handler tests",
    },
    Entry {
        range: "AIT-S83..AIT-S88",
        status: "implemented-and-existing-tests",
        rationale: "presence defaults covered here; push-rule AIT-S84..AIT-S88 duplicate IDs are covered by push rules tests",
    },
    Entry {
        range: "AIT-S89..AIT-S90",
        status: "runtime-harness",
        rationale: "covered by Node raw-wire conformance plus cross-SDK harness when a real server profile is supplied",
    },
    Entry {
        range: "AIT-S91..AIT-S98",
        status: "existing-server-tests",
        rationale: "covered by http_handler AI limit, terminal persistence, serial ack, channel state, and batch ack tests",
    },
];

#[test]
fn every_unique_ait_s_assertion_has_coverage_or_explicit_runtime_owner() {
    let covered = COVERAGE
        .iter()
        .flat_map(|entry| {
            assert!(
                !entry.status.trim().is_empty() && !entry.rationale.trim().is_empty(),
                "coverage entries must include status and rationale"
            );
            expand_range(entry.range)
        })
        .collect::<BTreeSet<_>>();

    let expected = (1..=98)
        .map(|index| format!("AIT-S{index}"))
        .collect::<BTreeSet<_>>();

    let missing = expected.difference(&covered).cloned().collect::<Vec<_>>();
    let extra = covered.difference(&expected).cloned().collect::<Vec<_>>();
    assert!(missing.is_empty(), "missing AIT-S coverage: {missing:?}");
    assert!(
        extra.is_empty(),
        "unknown AIT-S coverage entries: {extra:?}"
    );
}

fn expand_range(input: &str) -> Vec<String> {
    if let Some((start, end)) = input.split_once("..") {
        let start = parse_id(start);
        let end = parse_id(end);
        return (start..=end).map(|index| format!("AIT-S{index}")).collect();
    }
    vec![format!("AIT-S{}", parse_id(input))]
}

fn parse_id(input: &str) -> u32 {
    input
        .trim()
        .strip_prefix("AIT-S")
        .expect("AIT-S prefix")
        .parse()
        .expect("numeric AIT-S id")
}
