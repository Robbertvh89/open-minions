---
id: TASK-50
title: GPU e2e tests read the presigned-POST multipart body as raw JSONL
status: In Progress
assignee: []
created_date: '2026-10-19 23:45'
updated_date: '2026-10-19 23:46'
labels:
  - test
  - executor
dependencies: []
ordinal: 90000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
test_mlx_runners_gpu.py and test_hf_runners_gpu.py test_e2e_{completion,embedding}_through_sandboxed_worker fail with JSONDecodeError since TASK-19 (#55): the S3 stub captures the multipart/form-data POST body and json-decodes it as JSONL. Opt-in gpu/model tests, not run by default, so it went unnoticed. Parse the file part (like test_io_upload.py: email BytesParser, policy HTTP) and also assert the form fields (key, policy) are sent.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 All 4 e2e GPU tests pass on this Mac with the mlx and hf extras
- [ ] #2 Stub asserts multipart form fields and file part
<!-- AC:END -->
