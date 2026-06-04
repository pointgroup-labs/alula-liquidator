# Contributing

Thanks for taking the time. A few short conventions keep the project consistent.

## Local checks

CI runs four jobs. Match them locally before pushing:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo audit                   # honours .cargo/audit.toml
```

Run `cargo fmt --all` (without `--check`) to apply formatting. The `--locked` flag is mandatory — CI refuses to update `Cargo.lock`.

## Commit and PR conventions

Commit messages and PR titles follow [Conventional Commits](https://www.conventionalcommits.org/):

```
feat: add new strategy hook
fix(reactor): handle empty event batch
chore(deps): bump tokio
```

Allowed verbs: `feat`, `fix`, `chore`, `build`, `ci`, `docs`, `style`, `refactor`, `perf`, `test`, `revert`. Optional scope in parentheses; `!` after the scope signals a breaking change.

The PR title check is enforced by `.github/workflows/conventional-commits.yml`. Individual commits inside a PR are not enforced (PRs are squash-merged), but matching the convention there too makes `git log` more useful.

## Dependency bumps

Dependabot opens weekly PRs (`Monday 08:00 UTC`) for `cargo` and `github-actions`. Patch bumps are grouped into a single PR per ecosystem; minor bumps land individually. Review the changelog before merging minors.

If you add a new direct dependency, prefer the existing `[workspace.dependencies]` table so all crates pick the same version. Default to `--no-default-features` for crates with optional TLS / runtime backends.

## Security advisories

`cargo audit` is part of CI. When `cargo audit` reports a finding we cannot fix from this side of the dependency tree, add an entry to [`.cargo/audit.toml`](./.cargo/audit.toml) with:

1. the fixed-version range cited from the advisory,
2. the specific upstream blocker (which crate's bump is gated),
3. an honest residual-risk note for the keeper's deployment shape — no rationalising.

Re-evaluate the entire ignore list on every `stellar-rpc-client` or `soroban-sdk` dependabot bump; most current ignores will clear when those move to `rustls 0.22+`.

## Bigger changes

Open an issue first for anything that touches the strategy contract, the engine/keeper boundary, or the metric vocabulary. These cost more to undo than they cost to discuss up front. Small fixes, doc edits, and dependency bumps can go straight to PR.
