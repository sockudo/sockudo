# Ably Compatibility Gaps

This file records gaps and release blockers for Sockudo's opt-in `ably-compat` facade. The selected
scope is the pinned Ably REST/WebSocket manifest, excluding Live Objects and all non-WebSocket
realtime transports.

## Not Claimed

| Gap | Status | Notes |
| --- | --- | --- |
| Full Ably platform API parity | Intentionally out of scope | Sockudo is not claiming to replace every Ably service or endpoint. |
| LiveObjects/object modes | Intentionally out of scope | Not required for the current AI Transport target. |
| Non-WebSocket realtime transports | Intentionally out of scope | No Comet, XHR, SSE, long-polling, or fallback transport will be added. |
| Behavior outside the 41-file manifest | Not claimed | Only pinned assertions and native conformance evidence support compatibility statements. |

## Current Release Blockers

| Blocker | Fresh evidence |
| --- | --- |
| Strict upstream-pending lane | Four `resume_lost_continuity` expansions fail inside the unchanged upstream body before the protocol assertion. Portability-only patch rules prohibit changing that body or expected value. |
| Browser default lanes | The latest complete matrix remains red because browser page/console diagnostics are fatal: Chromium has 233 page and 21 console errors, Firefox has 232 page and 390 console errors, and WebKit has 56 page and 438 console errors. The pinned browser crypto tests call private `Message.decode`/`Message.fromWireProtocol` exports that are absent from the pinned browser build; intentional connection-failure cases also produce engine-level WebSocket diagnostics. These errors are retained, not filtered. |
| Browser strict lane | The latest Chromium strict report has four unchanged resume-body failures, 45 page errors, and one runner error caused by the pinned auth test settling 71 results from 70 definitions after `done()` is called twice. |
| Release load/soak guard | One two-node 10,000-publish/1,000,000-delivery burst is correctness-clean, but publish p99 is 2032.031 ms and delivery p99 is 2008 ms against 100 ms and 250 ms budgets. The required three independent runs, both topologies, and every release scenario are therefore not complete. |

## Closed Release Findings

| Finding | Fresh evidence |
| --- | --- |
| Dashboard fallback-secret authentication bypass | Startup now fails closed without an explicit session secret. Dashboard API tests cover missing/placeholder secrets and pass 10/10 with TypeScript checking. |
| Stale dashboard JWT privileges | Authenticated requests reload the user and current role/disabled state instead of trusting stale token authorization. Focused middleware/session tests cover role, password-version, and disabled-user changes. |
| Unbounded local revocation state | Local and shared revocation records are app-scoped, capacity-bounded, and expiry-pruned. Focused tests cover bounds and cross-app rejection. |
| Embedded/JWE JWT coverage | The local compatibility signer now honors the requested embedded/JWE variants and runtime verification rejects unsupported or confused algorithms. These variants have direct behavior assertions rather than title-only coverage. |
| Fuzz boundary inventory | Fourteen checked-in `cargo-fuzz` targets and corpora cover the protocol/REST codecs, filters, auth/capabilities, continuity, state transitions, message data, mutations, versioned wire projection, push mapping, and AI headers. Each target completed a 20-second fresh campaign without a crash; longer campaigns remain prudent release hardening. |
| Fresh AI chaos matrix | Eleven five-node scenarios passed under `target/ai-chaos/release-blocker-fix-postfix2`, including node loss, partitions, Redis restart, readiness, clock skew, and slow-subscriber pressure. Slow-subscriber delivery p99 was 8.704 ms. |
| Firefox realtime assertion failures | The orderly fatal WebSocket close path now flushes Ably `ERROR`, permits the peer close handshake for a bounded 250 ms, and then closes. Focused current-tree Firefox reruns pass `failure.test.js` 19/19 and `init.test.js` 14/14; a complete Firefox rerun is still required before replacing the full-matrix counts above. |

## Required Before Broader Claims

- Resolve the strict and browser gates without changing upstream bodies, assertions, or expected
  values and without introducing a non-WebSocket transport. Pinned upstream defects remain release
  blockers rather than local test patches or xfails.
- Meet the release latency budgets and retain three independent one-node and two-node load/soak
  runs for every required scenario.
- Retain longer fuzz campaigns and the fresh security/chaos evidence with the release artifacts.
- Keep the `ably-compat` feature disabled in native-only AI Transport builds.
- Keep Pusher V1 and Sockudo V2 conformance green with and without `ably-compat`.
- Expand docs only when supported by the pinned manifests or native conformance suites.
