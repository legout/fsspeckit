## 1. Define Import Standards
- [x] 1.1 Add "Import Patterns" section to `docs/explanation/concepts.md`
- [x] 1.2 Document the three-level import hierarchy:
  - Top-level: `from fsspeckit import filesystem, AwsStorageOptions`
  - Package-level: `from fsspeckit.datasets import PyarrowDatasetIO`
  - Module-level: `from fsspeckit.sql.filters import sql2pyarrow_filter`

## 2. Standardize How-To Guides
- [x] 2.1 Update imports in `docs/how-to/configure-cloud-storage.md`
- [x] 2.2 Update imports in `docs/how-to/work-with-filesystems.md`
- [x] 2.3 Update imports in `docs/how-to/read-and-write-datasets.md`
- [x] 2.4 Update imports in `docs/how-to/merge-datasets.md`
- [x] 2.5 Update imports in `docs/how-to/sync-and-manage-files.md`
- [x] 2.6 Update imports in `docs/how-to/optimize-performance.md`
- [x] 2.7 Update imports in `docs/how-to/use-sql-filters.md`

## 3. Standardize Tutorials
- [x] 3.1 Update imports in `docs/tutorials/getting-started.md`
- [x] 3.2 Remove "Legacy import (still works but deprecated)" comments

## 4. Standardize Reference and Explanation
- [x] 4.1 Update imports in `docs/reference/api-guide.md`
- [x] 4.2 Update imports in `docs/explanation/concepts.md`
- [x] 4.3 Update imports in `docs/explanation/architecture.md`

## 5. Validate Consistency
- [x] 5.1 Verify all import statements across docs use consistent patterns
- [x] 5.2 Run MkDocs build to ensure no issues
- [x] 5.3 Spot-check that imports work with current package structure
