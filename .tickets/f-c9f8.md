---
id: f-c9f8
status: closed
deps: [f-9ad3]
links: []
created: 2026-01-12T15:02:39Z
type: task
priority: 2
assignee: legout
parent: f-c589
---
# Migrate internal call sites to core.normalize_path

## Acceptance Criteria

All non-dataset internal call sites import and use core/filesystem/paths.normalize_path(); legacy functions remain but have zero duplicated logic.

