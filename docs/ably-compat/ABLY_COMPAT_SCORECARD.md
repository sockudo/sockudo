# Ably REST, WebSocket, and AI Transport Compatibility Scorecard

Evidence refreshed: 2026-07-15

Status: the opt-in `ably-compat` feature exposes the selected Ably REST and WebSocket surface while
the native Sockudo/Pusher `/app/{appKey}` route remains unchanged. The pinned Node default lane and
AI Transport lane are green. Strict upstream-pending and browser lanes are separate release gates
and are currently red; their failures are listed below. This is therefore implementation and test
evidence, not a release-ready compatibility claim.

## Target Versions

| Surface | Target | Evidence |
| --- | --- | --- |
| Ably AI Transport package | `@ably/ai-transport@0.4.0`, source `e817858c3e66931c7847cd08bab88384288c9bea` | Four-file pinned AIT manifest. |
| Ably JS upstream suite | `ably@2.21.0`, source `400c6a42ff4903cd3bba3c556f75dfbea1b74448` | 41-file REST/WebSocket manifest under `sockudo-compatibility/`. |
| Browser runtimes | Playwright `1.61.1`: Chromium `149.0.7827.55`, Firefox `151.0`, WebKit `26.5` | Revisions and every selected file are pinned in `scope/browser-manifest.json`. |
| Sockudo server workspace | `4.7.0` | Root Cargo workspace package version. |

## Compatibility Summary

| Area | Status | Notes |
| --- | --- | --- |
| Pusher Protocol V1 compatibility | Supported | Ably compatibility is additive and must not alter V1 wire behavior. |
| Sockudo-native AI Transport | Supported | Uses Cargo feature `ai-transport` plus runtime `[ai_transport]`; it does not require `ably-compat`. |
| Ably facade feature gate | Supported | Root Ably WebSocket route, Ably REST routes, and Ably realtime egress tap are compiled only with `ably-compat`. |
| Ably Realtime JSON/MsgPack protocol | Node-default evidence green; browser gate red | The selected WebSocket-only Node matrix passes in both formats. Browser page/console failures remain release blockers. No fallback realtime transport is implemented. |
| Ably REST auth, time, publish, history, stats, status, batch, and push-recipient surfaces | Node-default evidence green; browser gate red | The selected Node assertions pass. Browser CORS coverage includes the required methods and headers; independent page/console diagnostics still fail the browser gate. |
| Mutable messages and annotations | Node-default evidence green; browser gate red | They use Sockudo's native version and annotation services. Browser assertions pass in the latest complete reports, while fatal browser diagnostics remain. |
| History, rewind, recovery, presence, and SYNC | Node-default evidence green; strict/browser gates red | The strict resume body fails before its wire assertion because of a pinned upstream fixture error. Chromium also reports fatal page errors. |
| Channel modes/capabilities | Supported only where selected assertions prove behavior | Broader Ably platform semantics are not claimed. |
| LiveObjects/object modes | Intentionally out of scope | Not part of the current compatibility target. |
| Ably Push recipient/channel REST subset | Node-default evidence green; browser gate red | Requests use the native push registration, subscription, publish, and status pipeline. Provider credentials remain external and secret. |
| Binary MsgPack Ably protocol | Node-default evidence green | The same selected REST/WebSocket manifest runs JSON and MsgPack where upstream supports them. |
| Non-WebSocket Ably realtime transports | Intentionally out of scope | Comet, XHR polling/streaming, SSE, long polling, and transport fallback are not implemented. |
| Full Ably platform API parity | Not claimed | The evidence-backed scope is Ably REST and WebSocket compatibility, excluding Live Objects. |

## Stable Commands

Start Sockudo with:

```bash
cargo run -p sockudo --features "v2,ai-transport,ably-compat,redis,postgres,push" -- \
  --config config/config.toml
```

Then run:

```bash
cd sockudo-compatibility
make conformance
make strict-completeness
make browser-conformance
make browser-matrix
make browser-strict
make ait-conformance
```

The default, strict-completeness, browser, and AIT reports remain separate. A pass in one lane is
never used to report another lane as green.

## Fresh pinned results

| Lane | Result | Release status |
| --- | --- | --- |
| Node upstream defaults | 41 files; 575 passed; 0 failed; 27 upstream-pending; 0 runner/stale-xfail errors | Green for this lane only |
| Node strict completeness | 6 files; 246 passed; 4 failed; 0 pending | Blocked by the unchanged `resume_lost_continuity` upstream body calling an undefined helper return before its wire assertion |
| Chromium defaults | 574 passed; 0 assertion failures; 27 pending; 233 page errors; 21 console errors | Red because browser errors are fatal |
| Chromium strict | 176 recorded passes; 4 failed; 45 page errors; 1 runner error | Red; the auth file's 70 passes are discarded after its pinned body settles 71 results from 70 definitions |
| Firefox defaults | 572 passed; 2 failed; 27 pending; 232 page errors; 390 console errors | Red; focused post-fix reruns pass the formerly failing `failure.test.js` 19/19 and `init.test.js` 14/14, but the full matrix has not been rerun |
| WebKit defaults | 574 passed; 0 assertion failures; 27 pending; 56 page errors; 438 console errors | Red because browser errors are fatal |
| AI Transport | 4 files; 50 passed; 0 failed; 0 pending | Green for this lane only |

## Hardening and Distributed Evidence

| Gate | Fresh result | Release status |
| --- | --- | --- |
| Dashboard authentication | 10/10 dashboard API tests and TypeScript checking pass; explicit-secret startup, current-user authorization, bounded revocation, unsigned-token rejection, and login-rate limits are covered | The three recorded dashboard security findings are closed |
| JWT/JWE behavior | Embedded and encrypted token variants are produced and verified by direct assertions; unsigned and algorithm-confused tokens are rejected | Green for the implemented variants |
| Fuzz campaigns | 14 checked-in targets/corpora, 20 seconds per target, no crashes | Fresh short campaigns are green; retain longer campaigns before release |
| Five-node AI chaos | 11/11 scenarios passed under `target/ai-chaos/release-blocker-fix-postfix2`; slow-subscriber delivery p99 8.704 ms | Green for this fresh matrix |
| Two-node Ably burst correctness | 10,000/10,000 publishes and 1,000,000/1,000,000 deliveries; zero loss, duplicates, reordering, unexpected messages, or gaps | Correctness green for this run |
| Release load/soak performance | Publish p99 2032.031 ms and delivery p99 2008 ms versus 100 ms and 250 ms budgets; only one topology/scenario run is retained | Red; three-run release matrix remains incomplete |

## Compatibility Claim

Current status: **release evidence is incomplete**. The implemented and selected surface is Ably
REST and WebSocket compatibility, excluding Live Objects, but the strict and browser gates above
must be green before publishing that wording as an unqualified release claim.

Preferred public wording:

- **Ably REST and WebSocket compatibility, excluding Live Objects** — only when accompanied by the
  pinned lane results and explicit WebSocket-only transport scope.
- **AI Transport compatibility** — supported by its separate 50/50 pinned lane.

Avoid "complete Ably support" or "full Ably compatibility" until the scorecard proves it.
