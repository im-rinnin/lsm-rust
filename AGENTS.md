# Repository Guidelines

## Project Structure & Module Organization
- Source lives in `src/`. Binary entry is `src/main.rs`.
- Core LSM engine resides in `src/db.rs` and `src/db/*` (e.g., `lsm_storage.rs`, `memtable.rs`, `level.rs`, `table.rs`, `block.rs`, `store.rs`, `logfile.rs`, `snapshot.rs`, `common.rs`, `db_meta.rs`).
- Design notes: `design.md`, `ut-design.md`. Coverage helper: `code_coverage.sh`.
- Unit tests live alongside modules via `#[cfg(test)]`. No `tests/` integration folder yet.

## Build, Test, and Development Commands
- Build: `cargo build` (debug), `cargo build --release` (optimized).
- Check: `cargo check` (fast type/borrow checking).
- Run: `cargo run` (executes the binary in `src/main.rs`).
- Test: `cargo test`, `cargo test path::to::test`, `cargo test --lib`.
- Lint/Format: `cargo clippy`, `cargo fmt`, `cargo fmt --check`.
- Coverage: `./code_coverage.sh` (requires `grcov`).

## Coding Style & Naming Conventions
- Rust 2021, 4-space indent, `snake_case` functions/vars, `PascalCase` types, `SCREAMING_SNAKE_CASE` consts.
- Imports grouped by crate; prefer explicit imports; avoid glob `*`.
- Errors: use `Result<T, E>`/`anyhow::Result<T>`, prefer `?`, avoid `unwrap()`.
- Concurrency: `Arc<RwLock<T>>` for shared state, `crossbeam_channel` for queues/signals.
- Memory/Buffers: `Arc<T>` for sharing, `Bytes` for zero-copy buffers, `Vec<u8>` when owned.
- Logging: `tracing`; enable via `db::db_log::enable_log_with_level(tracing::Level::INFO)`.
- Serialization: `serde` with `#[derive(Serialize, Deserialize)]`.

## Testing Guidelines
- Write small, deterministic tests; use `assert!`/`assert_eq!` and fixed seeds.
- Name tests descriptively (e.g., `fn compaction_merges_latest()`).
- Run a single test: `cargo test module::tests::name -- --nocapture`.
- Use `./code_coverage.sh` for line/branch coverage locally.

## Commit & Pull Request Guidelines
- Prefer Conventional Commits: `feat:`, `fix:`, `refactor:`, `test:`, `docs:`.
- PRs: clear description, rationale, and scope; link issues; include behavior/perf notes when relevant; add/adjust tests.
- Ensure green `cargo test`, `cargo fmt --check`, and `cargo clippy` before pushing.

## Security & Configuration Tips
- Do not commit real datasets or secrets.
- Avoid `unsafe` unless essential and reviewed; document any usage.
