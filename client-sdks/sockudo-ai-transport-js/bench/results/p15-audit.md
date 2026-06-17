# P15 Performance Audit

Budgets are enforced by `pnpm bench` through Vitest tests under `bench/`.

## Profile Notes

Initial inspection found the same hot-path risks listed in the prompt:

- Inbound message handling can allocate when it joins decoded input/output arrays.
- Header access must stay lazy through `InboundMessage.getTransportHeaders()`.
- View materialization must keep cached message arrays stable for token-tail updates.
- Router queues must fail closed under hostile slow-consumer input.

The committed bench suite exercises those paths directly and records runtime evidence in
`bench/results/latest.json` when it runs locally or in CI.

## Current Evidence

Run:

```bash
pnpm bench
```

This performs a production build, bundle/tree-shake checks, throughput and latency assertions,
bounded-memory checks, router cap checks, and React commit-count assertions. The default regression
job uses a 100k-token flat-memory stream so CI remains fast. The same test accepts
`BENCH_EXTENDED=1` for the manual 1M-token pass.

Extended memory pass verified during this audit:

```bash
BENCH_EXTENDED=1 node --expose-gc ./node_modules/vitest/vitest.mjs run --config bench/vitest.config.ts -t "high-volume"
```

Result: 1 test passed, 8 skipped, 1.23s duration.
