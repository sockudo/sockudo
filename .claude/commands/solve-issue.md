Solve a GitHub issue end-to-end: fetch details, create a branch, implement the fix, and open a PR.

## Steps

1. **Fetch the issue** using `gh issue view $ARGUMENTS --json number,title,body,labels` to understand what needs to be done.

2. **Derive a branch name** from the issue title: lowercase, replace spaces/special chars with hyphens, prefix with `fix/` for bugs or `feat/` for features, append the issue number. Example: `fix/connection-drops-on-reconnect-42`.

3. **Create the branch from master**:
   ```
   git fetch origin master
   git checkout -b <branch-name> origin/master
   ```

4. **Read and understand** the relevant code before making any changes. Use Grep and Read to locate the affected code paths. Do not guess — read first.

5. **Implement the fix** following the project's code conventions (see CLAUDE.md):
   - Use `tracing` for logging, not `println!`
   - Use `sockudo_core::error::Result` for errors
   - Feature-gate optional code with `#[cfg(feature = "...")]`
   - Do not add placeholder implementations
   - Do not add unnecessary comments

6. **Run checks locally**:
   ```
   cargo clippy --workspace
   cargo fmt --check --workspace
   cargo test --workspace
   ```
   Fix any clippy warnings or test failures before proceeding.

7. **Commit the changes** with a clear message referencing the issue:
   ```
   git add <specific files>
   git commit -m "<type>: <short description>

   Closes #<issue-number>
   ```

8. **Push the branch**:
   ```
   git push -u origin <branch-name>
   ```

9. **Open a PR** targeting `master`:
   ```
   gh pr create \
     --title "<short title>" \
     --base master \
     --body "..."
   ```
   PR body must include:
   - `## Summary` — 2–4 bullet points describing what changed and why
   - `## Test plan` — checklist of how to verify the fix
   - `Closes #<issue-number>` — so the issue auto-closes on merge


Return the PR URL at the end.
