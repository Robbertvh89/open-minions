---
id: TASK-21
title: Fix flaky test_counts_route_templates_not_urls (digit leak check)
status: In Progress
assignee: []
created_date: '2026-10-17 13:26'
updated_date: '2026-10-17 13:26'
labels:
  - server
  - test
dependencies: []
ordinal: 46000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
The metrics leak check asserts the substring '123' is absent, but prometheus_client *_created timestamps can contain it by chance; CI failed once on TASK-6.3. Check the full path segment instead.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Leak check uses the full unique path segment, not a short digit string
- [x] #2 Test passes repeatedly
<!-- AC:END -->
