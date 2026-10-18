# ADR-0017: Output uploads use a presigned POST with a signed size range

- Status: **Accepted** (TASK-19)
- Deviates from paper: no (the paper only says executors write through presigned object-store URLs; S-2 is ours)

## Context

A presigned PUT cannot carry a size limit, so an executor could write an arbitrarily large object
under its attempt key until `complete` HEADed it (SPEC S-2). A presigned POST can: its signed
policy may contain `["content-length-range", 0, max]`, which the object store enforces at upload.

## Decision

- `Assignment` and `HeartbeatResponse` carry `output_post {url, fields}` instead of `output_url`. The
  server signs it with `generate_presigned_post` for the attempt key and `MINION_MAX_OUTPUT_BYTES`
  (inclusive; counts the file part). The executor sends the fields in order and the `file` part last
  (`minion_common.s3post`), streamed with an exact `Content-Length`.
- Hard cut, no dual field: a kept PUT URL would leave the hole open for any executor using it.
  `AcquireRequest.output_upload = "post"` is the capability marker; without it `acquire` answers
  `426 EXECUTOR_OUTDATED`, **before** any claim, so an old executor never owns a subtask whose
  assignment it cannot parse. The old executor exits (non-shed 4xx), the log names the code.
- Executors map `400 EntityTooLarge` to the non-retryable `OUTPUT_TOO_LARGE`; any other non-2xx
  (redirects are not followed) stays a retryable `UPLOAD_FAILED`; transport errors release.
- The refreshed POST from a heartbeat is accepted only when URL and `key` field are unchanged (the POST
  URL is the bucket URL, so the path alone does not pin the object).
- The HEAD check at `complete` stays as defence in depth.

## Consequences

- Operator procedure: upgrade executors with the server; old ones get `426` until upgraded. Subtasks in
  flight across the server upgrade hold a pre-upgrade PUT URL (valid up to the presign TTL); their
  heartbeat responses no longer parse, so they finish or fail/retry as usual, bounded by `max_attempts`.
- SeaweedFS 4.05 enforces the range (`400 EntityTooLarge`, nothing stored) but answers a tampered key or
  an expired policy with `307`, not `403`, and accepts form fields outside the policy. Tests therefore
  assert "not 2xx and no object", never a status code. AWS and MinIO answer `403`.
- Responses grow by about 1 KB (base64 policy). Signing is local HMAC, as before.
- Whether SeaweedFS buffers a multipart body in memory before rejecting it is unverified; watch memory
  on large caps.
- Attempts times the cap can still accumulate: quota and lifecycle advice (ARCHITECTURE §3.3) stands.
