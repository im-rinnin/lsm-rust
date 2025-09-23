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
- Keys/Values: use `KeyBytes`/`ValueByte`; `KeyVec`/`ValueVec` are removed.
 - Iterators: aggregate multiple ordered KV streams with `KViterAgg`, which accepts `Vec<Box<dyn Iterator<Item = KVOpertion>>>`.

## Testing Guidelines
- write unit test first (TDD)
- run `cargo check && cargo test ` after code change  
- Write small, deterministic tests; use `assert!`/`assert_eq!` and fixed seeds.
- Name tests descriptively (e.g., `fn compaction_merges_latest()`).
- Run a single test: `cargo test module::tests::name -- --nocapture`.
- Use `./code_coverage.sh` for line/branch coverage locally.

## Commit & Pull Request Guidelines
- Prefer Conventional Commits: `feat:`, `fix:`, `refactor:`, `test:`, `docs:`.
- PRs: clear description, rationale, and scope; link issues; include behavior/perf notes when relevant; add/adjust tests.
- Ensure green: `cargo test`, `cargo fmt --check`, and `cargo clippy -- -D warnings` before pushing.

## Security & Configuration Tips
- Do not commit real datasets or secrets.
- Avoid `unsafe` unless essential and reviewed; document any usage.

## Roadmap & TODOs
- Source of truth is `todo.md`. Update `todo.md` first, then mirror a concise summary here.
- In progress
  - Open DB from existing files: load table-change log; load SSTables and build levels
- High priority
  - Read mini LSM and refactor; validate running DB from files
- Normal
  - Write worker: loop and fetch requests (no sleeps)
  - Disk perf: reads and sequential writes
  - File-backed store improvements; range query API support
  - Persist compaction changes in table-change log
- Low
  - Benchmarking suite; recovery hardening
- Done (highlights)
  - Shutdown dumps memtables/imm to L0
  - Freeze memtable; backpressure when many imm tables
  - Post-compact check with immediate rerun if needed
  - Key/Value refactor to `KeyBytes`/`ValueByte`; removed `KeyVec`/`ValueVec`
  - Unit tests across memtable/levels/log/table; zero-copy KV read
  - Block format aligned with mini LSM; configs plumbed; tracing logs
