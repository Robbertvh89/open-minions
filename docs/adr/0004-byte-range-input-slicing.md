# ADR-0004: Byte-range input slicing instead of pre-split objects

- Status: **Accepted**
- Date: 2026-10-15
- Deciders: project owner
- Deviates from paper: **no** — ARCHITECTURE §5.2 marks it **[ours]**; the paper does not fix how the
  input is partitioned. SPEC Q1 (§11) asked this question; this ADR answers it.

## Context

A task's input is a JSONL object in S3 with many records. The server splits it into subtasks of
`records_per_subtask` records (default 100). Two options:

1. Pre-split: the server rewrites the input into one S3 object per subtask.
2. Byte ranges: the server records `(object_key, byte_start, byte_end, record_count)` per subtask and
   the executor fetches only that range.

Pre-splitting copies all input data through the server or a server-side copy per subtask, which costs
S3 requests and storage and doubles the data footprint. The server must also stay off the executor
data path (invariant 4), so any rewrite is server-side S3 work on the whole input.

## Decision

- `minion_server.splitting.slice_jsonl` scans the input once, streaming, and yields slices that start
  and end just past a newline (or at EOF). Slices are contiguous: each starts where the previous ended.
- Blank (whitespace-only) lines are skipped and not counted. A CRLF line keeps its `\r` inside the line.
- Memory is bounded by `max_line_bytes`; a longer line raises `LineTooLongError` and the task is
  rejected instead of buffering unbounded input.
- The splitter persists slices in `minion_subtask` with `byte_start`, `byte_end` (check constraint
  `byte_end > byte_start`) and `record_count`. Ids are deterministic, so a re-split is idempotent
  (`ON CONFLICT DO NOTHING`).
- The executor sends `Range: bytes=start-(end-1)` on a presigned GET (`minion_executor.io`), then
  parses exactly `record_count` records and fails with `BAD_INPUT` (non-retryable) on a mismatch.

## Consequences

- No input rewrite and no extra S3 objects per subtask; splitting cost is one scan of the input.
- Subtask inputs depend on the original object. Replacing or editing the object after task submission
  invalidates the slices; the executor detects this only via `BAD_INPUT` (size/record mismatch).
  Inputs must be immutable for the life of a task (documented requirement, not enforced).
- Blank lines and a trailing unterminated line are handled by the splitter; executors must not assume
  every line in the range is a record.
- Presigned GET is scoped to one object, so an executor can read a range of a single input object only.
