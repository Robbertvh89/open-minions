---
id: TASK-19
title: >-
  Presigned POST with content-length-range for output uploads (upload-time size
  cap)
status: Done
assignee: []
created_date: '2026-10-17 12:59'
updated_date: '2026-10-18 19:47'
labels:
  - server
  - executor
  - security
dependencies: []
ordinal: 46000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Presigned PUT URLs accept any size until TTL; switch outputs to presigned POST with content-length-range so S3 rejects oversize uploads. Wire-contract change for executors. From TASK-6.3 security review.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Output uploads above the cap are rejected by S3
- [x] #2 Executors handle the POST form upload; back-compat plan documented
<!-- AC:END -->



## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
## Goal
Output uploads switch from a presigned PUT (no size limit until the URL expires) to a presigned POST whose signed policy includes `["content-length-range", 0, max_output_bytes]`, so S3 itself refuses an object above the server cap. This is a wire-contract change for executors and minion-scale sims.
- AC#1 -> SPEC S-2 (presigned URLs, output cap), §6.2 acquire/heartbeat rows, invariant 4, NF-2 (unchanged).
- AC#2 -> FR-11 (executor upload step), §6.2 contract, ARCH §2.2 error table. Back-compat plan below.
Not a deviation from the paper (the paper only says executors write through presigned object-store URLs; S-2 is [ours]). A short ADR-0017 is still recommended because the executor wire contract breaks (no dual-field period) and the rollout gate is new.

## Verified facts
- SeaweedFS `chrislusf/seaweedfs:4.05` (deploy/compose.yml:54, conftest.py:112) supports POST policy uploads. Probe on 127.0.0.1:59000 using boto3 `generate_presigned_post` and httpx:
  - file of 100 B or 1000 B (cap 1000): 204, object stored. So the range is inclusive and counts the file part only.
  - 1001 B, and 5 MiB against a 1 MiB cap: 400 `EntityTooLarge`, no object. Same with a hand-built streaming multipart body, both with Content-Length and chunked.
  - tampered `key` field: **307** (Location /minion, empty body), no object at either key. Expired policy: also 307. Bad signature: 403 `SignatureDoesNotMatch`.
  - an extra form field outside the policy (Content-Type) is accepted (AWS would reject it). This is harmless here: the key, size and expiry are still enforced.
  - `httpx.Response.raise_for_status()` raises on 307, so the executor's current "non-2xx = UPLOAD_FAILED" path covers it (keep follow_redirects off, which is the httpx default).
  - The first write on a fresh SeaweedFS container can time out with "assign volume" (seen once, then fine). The conftest `s3` fixture already warms it with the bucket, but watch for flakes.
- TASK-9 status in main:
  - AC#1 (revision pin) is done: ModelEntry requires a 40-hex/64-hex revision (packages/minion-common/src/minion_common/schemas.py:84-108), delivered by TASK-4.1.
  - AC#3 (params allow-list) is done: CompletionParams/EmbeddingParams with extra=forbid and `parse_params` (schemas.py:111-137), delivered by TASK-4.1.
  - AC#2, first half (Range-signed GET), is done: packages/minion-server/src/minion_server/storage.py:75-79, test `test_signed_range_get_enforced_by_s3` (packages/minion-server/tests/test_storage.py:82). Enforcement depends on the backend: on SeaweedFS that test only warns.
  - AC#2, second half ("oversize PUT rejected"), is closed by this task. Once TASK-19 merges, all TASK-9 ACs are met. The remaining TASK-9 notes are follow-ups, not ACs: create separate tasks for them, then close TASK-9.

## Touchpoints
- Server presign: packages/minion-server/src/minion_server/storage.py:70-90 (`_presign`, `presign_put`)
- Assignment build: packages/minion-server/src/minion_server/services/acquisition.py:116-144 (`_assignment`; PUT at :126)
- Heartbeat refresh: packages/minion-server/src/minion_server/services/heartbeat.py:62-70
- Acquire route: packages/minion-server/src/minion_server/api/executor.py:130-160 (gate goes before the inflight/slots block)
- Server cap setting: packages/minion-server/src/minion_server/settings.py:69. Promotion HEAD check: services/completion.py:108 (stays as defence in depth)
- Schemas: packages/minion-common/src/minion_common/schemas.py:258-260 (AcquireRequest), :263-287 (Assignment.output_url :277), :337-342 (HeartbeatResponse.output_url :340), :345-346 (caps)
- Executor upload: packages/minion-executor/src/minion_executor/io.py:91-108 (`upload_file`); loop.py:769-780 (Heartbeater init), :1288 (`_check_url(a.output_url)`), :1316-1330 (cap check, refresh, upload), :598 (AcquireRequest)
- Heartbeater: packages/minion-executor/src/minion_executor/heartbeat.py:44-47 (`_same_object`), :64/:79/:82 (initial URL), :150-158 (refresh)
- Scale sim: packages/minion-scale/src/minion_scale/sim.py:252 (AcquireRequest), :406-415 (`_upload` PUT). Metric name `output_put` (sim.py:412/414, metrics.py:106, docs/bench/scale-report-schema.md:74)
- Tests referencing output_url / PUT: minion-common tests/test_client.py:54,250,262,331 and test_schemas.py; minion-server tests/test_acquire.py, test_heartbeat.py, test_storage.py, test_protocol_races.py:673; minion-executor tests/test_loop.py:68,78,743,971-997,2505, test_heartbeater.py, test_governor_reaction.py, test_mlx_runners_gpu.py:366,387; minion-scale tests/test_fleet.py:186, test_sim.py:181
- Docs: SPEC.md:395-396 (§6.2), :443-457 (S-2); ARCHITECTURE.md:24, :159, :171-172, :179, :252, :415, :427-439; README.md:533

## Design
- **Schema (minion-common):**
  ```python
  class PresignedPost(_ResponseModel):
      url: str
      fields: dict[
          str, str
      ]  # validator: 1..16 entries; names ^[A-Za-z0-9_-]{1,64}$; values <= 8 KiB, no CR/LF/'"'; must contain "key" and "policy"
  ```
  - `Assignment.output_post: PresignedPost` replaces `output_url`.
  - `HeartbeatResponse.output_post: PresignedPost` replaces `output_url`.
  - `AcquireRequest.output_upload: Literal["post"] | None = None`: the capability marker, which new executors always send.
- **Encoder (minion-common, new `minion_common/s3post.py`):** pure, shared by the executor and the sim, no httpx or boto.
  ```python
  @dataclass(frozen=True)
  class FormParts:
      preamble: bytes
      epilogue: bytes
      content_type: str

  def encode_post_form(fields: Mapping[str, str], *, filename: str = "output.jsonl", boundary: str | None = None) -> FormParts
  ```
  - Policy fields go first in the order given. The `file` part goes last (S3 requires that). The boundary is `secrets.token_hex(16)` and is checked to occur in no field value.
  - The caller sends `preamble + <file bytes> + epilogue` with `Content-Length = len(preamble) + size + len(epilogue)`.
  - A hand-built stream instead of `httpx files=`: httpx multipart reads file objects synchronously in the event loop, and this keeps the existing to_thread chunked reads (io.py:91-97).
- **Server storage:**
  - `async def presign_post(self, key: str, max_bytes: int, ttl_s: int | None = None) -> PresignedPost` calls `self._signer.generate_presigned_post(Bucket, key, Conditions=[["content-length-range", 0, max_bytes]], ExpiresIn=ttl)`. boto adds the exact `bucket` and `key` conditions itself.
  - Delete `presign_put`; its only callers are acquisition and heartbeat.
- **Assignment and heartbeat:** sign the attempt key with `max_bytes=settings.max_output_bytes`. `Assignment.max_output_bytes` stays, because the executor still pre-checks so it never uploads in vain.
- **Rollout gate:** in api/executor.py `acquire`, before the inflight/slots block and before any DB access:
  - if `body.output_upload is None`, raise `api_error(426, "EXECUTOR_OUTDATED", "upgrade the executor: output uploads use presigned POST")`.
  - It must sit before the claim. Otherwise an old executor would acquire, fail to parse the assignment (missing `output_url` is a ValueError, loop.py:608) and orphan the subtask until the ADR-0014 expiry, again and again.
  - With the gate, an old executor gets a non-shed 4xx, which loop.py:610-611 re-raises: the run exits, launchd restarts it with throttling, and the log names EXECUTOR_OUTDATED. No acquirement is created.
- **Executor:**
  - `io.upload_post(http, post: PresignedPost, path) -> None`: streaming, exact Content-Length, `raise_for_status()` (any 2xx is OK; AWS default is 204).
  - On 400 whose body contains `<Code>EntityTooLarge</Code>` (read at most 4 KiB, never logged), raise `OutputTooLargeError`. The loop maps it to the non-retryable `OUTPUT_TOO_LARGE` failure, because a deterministic rejection should not be retried. All other status errors keep `UPLOAD_FAILED` retryable; transport errors keep release.
  - Heartbeater holds `output_post: PresignedPost`. `_same_object` compares scheme+netloc+path of `url` **and** `fields["key"]`: the POST URL is the bucket URL, so the path alone no longer pins the object.
  - `_check_url(post.url)`.
  - The loop sends `output_upload="post"`.
- **Sim:** sends `output_upload="post"` and uploads `body` via `encode_post_form`. Keep the metric name `output_put` so the report schema stays stable; just update its description to "output upload (presigned POST)".
- **Back-compat (AC#2):** hard cut in one release, no dual `output_url` period. Keeping a PUT URL would leave AC#1's hole open to any executor that uses it. Server and executor ship together from this repo. Operator procedure (ADR-0017 + SPEC):
  1. upgrade the executors, or accept that old ones exit with EXECUTOR_OUTDATED until upgraded;
  2. subtasks in flight across the server upgrade still hold a pre-upgrade PUT URL (valid up to TTL <= 1 h). Their heartbeat responses no longer parse (the Heartbeater logs `heartbeat_error`, but the beat counted server-side), so they finish with the old PUT URL or fail/retry as usual. This is bounded by `max_attempts`.

## Steps
1. **common, additive (~180 lines):**
   - Changes: add `PresignedPost` and `s3post.encode_post_form`. Add `output_post: PresignedPost | None = None` to Assignment and HeartbeatResponse (temporarily optional; `output_url` kept). Add `AcquireRequest.output_upload`. Export from `__init__` if applicable.
   - Unit tests: PresignedPost validation (missing key/policy, CRLF, too many fields); the encoder round-trips through stdlib `email.parser.BytesParser` (fields in order, file last, exact length); the boundary is not in the values; unknown fields are still ignored on old-style payloads.
2. **server (~120 lines + tests):**
   - Changes: `S3Storage.presign_post`. `_assignment` and `heartbeat` fill `output_post` (and still `output_url`, until step 4). No gate yet.
   - Unit tests (test_acquire, test_heartbeat): `fields["key"]` equals the attempt key; the decoded base64 policy contains `["content-length-range", 0, max_output_bytes]` and `{"key": ...}`; expiration is about now + ttl.
   - **Integration** test_storage `test_presigned_post_enforces_size_cap(s3)`, with hard asserts (no warning) because 4.05 is verified to enforce:
     - (a) a file exactly at the cap gives 2xx and HEAD shows the size;
     - (b) cap+1 gives 4xx and the key is absent;
     - (c) an oversize POST onto an existing key gives 4xx and the old content is unchanged;
     - (d) a tampered `key` field is not 2xx and the tampered key is absent;
     - (e) a bad signature gives 403.
3. **executor + sim (~250 lines):**
   - Changes: `io.upload_post` + `OutputTooLargeError`; Heartbeater on PresignedPost (`initial_output`, `output_post`, the key-aware `_same_object`); loop.py wiring (`_check_url`, upload, `OUTPUT_TOO_LARGE` mapping, `output_upload="post"` at :598); sim `_upload` uses POST plus the acquire field.
   - Unit tests (MockTransport):
     - the parsed multipart body matches the fields, with the file last;
     - Content-Length is exact and the file is streamed in chunks;
     - 307 gives UPLOAD_FAILED fail; 400 EntityTooLarge gives OUTPUT_TOO_LARGE non-retryable; ConnectError gives release;
     - the refreshed POST is used for the upload (port test_loop.py:971);
     - a heartbeat POST with a different key is ignored (ObjectMismatch), same key with a newer expiry is accepted;
     - sim test_fleet/test_sim.
4. **cutover (~200 lines, mostly tests):**
   - Changes: remove `output_url` from Assignment and HeartbeatResponse; make `output_post` required; delete `presign_put` and the server's dual fill; enable the 426 gate.
   - Update every test listed under Touchpoints.
   - **Integration:**
     - (i) acquire without `output_upload` gives 426 EXECUTOR_OUTDATED and `minion_subtask_acquirement` has no row (gate before claim);
     - (ii) an evil-executor test in test_protocol_races (raw httpx): server `max_output_bytes=64`; acquire; POST a 65-byte body with `asg.output_post` gives 4xx and no attempt object; `complete` then gives 422 OUTPUT_MISSING; a 64-byte POST then works and complete succeeds;
     - (iii) the existing raw-path round trip (test_protocol_races.py:673) moves to POST;
     - (iv) executor test_e2e / test_loop_integration (real server + SeaweedFS + fake engine) stays green;
     - (v) a minion-scale smoke test with SeaweedFS.
5. **docs (docs-keeper):**
   - SPEC §6.2 rows 395-396 (`output_post {url, fields}`, `output_upload`, 426 EXECUTOR_OUTDATED) and S-2 (cap enforced at upload time by the signed policy; HEAD-at-complete kept as defence in depth; quota/lifecycle advice kept, since attempts x cap still accumulates).
   - ARCH §2.2 table (POST rows, EntityTooLarge -> OUTPUT_TOO_LARGE) and §3.3 lines 415/427-439.
   - README.md:533 limitation removed or reworded; scale-report-schema `output_put` description.
   - New ADR-0017 "Presigned POST output uploads" (contract break, gate, SeaweedFS 307 quirk, no dual period).
   - TASK-9 note.

## Invariants
- 1: unchanged; executors still use only REST and presigned objects.
- 2/3: the gate runs before the claim and adds no check-then-insert. The policy is scoped to one attempt key, as before.
- 4: strengthened; payloads still bypass the server, and S3 now enforces the cap.
- 5: the key is still attempt-scoped and promotion is unchanged (HEAD + CopySourceIfMatch).
- 6/7: untouched (upload runs in the control process, as before; no worker change).

## Risks / open questions
- **Hard cut:** old executors stop working (by design, with a clear code). Confirm 426 vs 409/400 for EXECUTOR_OUTDATED (426 proposed).
- **SeaweedFS quirk:** policy violations return 307, not 403. Tests must assert "not 2xx and no object", not a specific status. AWS/MinIO return 403; the EntityTooLarge 400 is the same across backends.
- **Laxness:** SeaweedFS accepts form fields outside the policy. Harmless for us, but note it in the ADR.
- **Memory:** unverified whether SeaweedFS buffers a multipart upload in memory before rejecting it. The cap is <= 5 GiB. If the M6 scale run or the e2e shows memory spikes, note it for operators.
- **Size:** the assignment/heartbeat response grows by about 1 KB (base64 policy). Signing cost is similar to PUT (local HMAC), so the acquire hot path is unaffected. Re-check with the minion-scale smoke run.
- **Overlap with TASK-9:** once this merges, TASK-9's ACs are all met. Split its follow-up notes into their own tasks before closing it.

Too big? No: 5 steps in one milestone, one PR.
<!-- SECTION:PLAN:END -->
