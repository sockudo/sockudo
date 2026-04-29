# Release 4.4 Release Engineering Notes

## Adoption Gates

- Global annotations remain disabled by default through `[annotations].enabled = false`.
- Channel policy must opt in with `policy.channels.annotations_enabled = true` or a namespace override at `policy.channels.channel_namespaces[].annotations_enabled = true`.
- Annotation publish, delete, and read paths reject non-opted-in channels. HTTP annotation publish returns 403 with a channel-policy message.
- Enabling annotations forces message retention for matching channels because summary projections depend on retained message and annotation state. Operators should account for the added storage and billing impact.

## Verification Checklist

- Five summarizers have deterministic unit coverage: `total.v1`, `flag.v1`, `distinct.v1`, `unique.v1`, and `multiple.v1` are covered in `crates/sockudo-core/src/annotations.rs`.
- Cluster convergence under concurrent annotation churn is covered by the memory annotation store convergence tests, including concurrent unique summarizer churn.
- Client ID clipping is tested at the exact configured threshold and above threshold.
- Identified-client rejection is tested for `flag.v1`, `distinct.v1`, and `unique.v1`.
- Summary delivery runs for every annotation create/delete path through `deliver_annotation_change`, and the delete-to-zero projection case is covered by HTTP annotation tests.
- Deleting a non-existent annotation returns 404.
- Annotation projection snapshots are sent on V2 subscription and raw annotation replay remains serial ordered for authorized `ANNOTATION_SUBSCRIBE` consumers.
- SDK examples document that explicitly setting `modes` replaces default modes and that `ANNOTATION_SUBSCRIBE` is opt-in for raw events.

## Release Note Draft Input

- New feature: five annotation summarizers: `total.v1`, `flag.v1`, `distinct.v1`, `unique.v1`, and `multiple.v1`.
- New event: `message.summary` is delivered to ordinary V2 subscribers when annotation state changes.
- New mode: `ANNOTATION_SUBSCRIBE` enables raw `annotation.create` and `annotation.delete` event streams.
- New capabilities: `annotation-publish`, `annotation-delete-own`, and `annotation-delete-any`.
- Breaking changes: none. Annotations are opt-in globally and per channel.
- Dependency: requires Release 4.3 Versioned Durable Messages.
