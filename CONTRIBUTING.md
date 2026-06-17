# Contributing to Sockudo 🚀

Welcome! 🎉 We're thrilled that you'd like to contribute to **Sockudo**, a high-performance WebSocket server built in Rust. Your support helps improve the real-time ecosystem and benefits the community.

---

## 🧠 Before You Start

- **Read the [README](./README.md)** for project setup and usage.
- **Check existing issues and discussions** before opening a new one.
- Contributions of *all kinds* are welcome — code, docs, tests, issues, etc.

---

## 🛠️ Getting Started

### 1. Clone the Repository

```bash
git clone https://github.com/sockudo/sockudo
cd sockudo
```

### 2. Set Up Your Environment

* Copy and edit the example `.env` file:

  ```bash
  cp .env.example .env
  ```

* Run locally (Docker preferred):

  ```bash
  make quick-start
  ```

* Or using Cargo:

  ```bash
  cargo build
  cargo run
  ```

---

## 🤝 How to Contribute

### ✅ Good First Tasks

* Fix typos or improve documentation
* Tidy up logs or improve CLI feedback
* Add test coverage to critical modules
* Enhance monitoring or error handling

### 💡 Feature Contributions

1. Open a discussion or issue before implementing large changes.
2. Write clean, idiomatic Rust code.
3. Add tests for new features and ensure existing tests pass.

---

## 🎯 Code Style & Tools

* Run `cargo fmt` before committing.
* Use `cargo clippy` to lint your code.
* Follow Rust best practices and Sockudo’s architecture (channels, adapters, services).
* Ensure your feature fits into the modular structure: `core`, `adapters`, `config`, etc.

---

## 🧪 Running Tests

```bash
cargo test
```

To run integration tests (requires Redis, etc.):

```bash
make test
```

---

## Protocol and SDK Compatibility Policy

Sockudo treats protocol compatibility as a release gate, not a best-effort guideline.

### Wire protocol rules

- Wire-protocol v1 is additive-only. New event names, optional fields, enum/action values, webhook event types, and HTTP response fields must not break existing V1 clients or SDKs.
- Protocol V1/Pusher-compatible output must remain byte-identical unless a major-version protocol is explicitly introduced.
- Protocol-visible PRs must update or explicitly confirm unchanged:
  - `docs/specs/ai-transport-wire-protocol.md`
  - forward-compat fixtures and golden transcripts
  - `docs/specs/compat-matrix.json`
  - the generated compatibility table in `docs/content/docs/reference/compatibility.mdx`
- `node scripts/generate-compat-matrix.mjs --check` is the doc-drift gate for the living compatibility matrix.

### SDK versioning rules

- SDKs use SemVer for public API changes.
- Patch and minor SDK releases must be additive and must preserve existing valid traffic behavior.
- Release workflows for SDK repositories must run their E1 compatibility lane against both the latest released Sockudo server and current server main before publish.
- Per-language API-diff jobs block non-additive public API changes unless the SDK release is intentionally major.

### Release order

Protocol and AI Transport releases must ship in this order:

1. Sockudo server with new behavior default-off.
2. `@sockudo/client`.
3. `@sockudo/ai-transport`.
4. Other client SDKs.
5. Server HTTP SDKs.

Do not skip ahead in the order. A later layer can release only after the earlier layer's compatibility gate is green.

### Support window and deprecations

- Sockudo supports the current and previous minor server release.
- Each SDK supports its current and previous minor release against supported server minors.
- AI Transport ships with zero deprecations in this plan. Any future deprecation requires a separate policy update, matrix update, migration note, and release announcement.

### Rollback guidance

- Server rollback: disable the new feature flag first, then roll back the binary if needed. V1 behavior must remain compatible throughout.
- Client SDK rollback: pin the SDK package to the previous minor while keeping the server feature default-off for affected clients.
- `@sockudo/ai-transport` rollback: disable AI channel prefixes and revert the package independently of the realtime SDK when possible.
- Server HTTP SDK rollback: remove usage of additive helper methods; existing trigger, batch, channel-info, and webhook validation APIs must remain compatible.

---

## 🚀 Submitting a Pull Request

1. Fork this repo and create your branch:

   ```bash
   git checkout -b feature/your-change
   ```

2. Make your changes with descriptive commit messages.

3. Push and open a PR to `main`:

   ```bash
   git push origin feature/your-change
   ```

4. Link related issues (e.g., `Closes #123`).

---

### 🔍 Code Review Process

All contributions go through a **code review**.
Please **do not merge your own pull requests**, even if all checks pass.
One of the maintainers will review your changes, suggest improvements if needed, and handle the merge. ✅

---

## 💬 Community & Conduct

* Be kind, inclusive, and respectful.
* All contributions are governed by our [Code of Conduct](./CODE_OF_CONDUCT.md).

* 💬 **Join us on [Discord](https://discord.gg/BSdkg2JR)** — we discuss new features, ideas, and help new contributors get onboarded!  
  If you're not sure where to start, just post in the `#contributions` channel and we'll figure something out together. 💡

---


## 📜 License

By contributing, you agree that your contributions will be licensed under the [AGPL-3.0](./LICENSE).

Thank you for making Sockudo better! 🦀💬
