## 1. Define Import Standards
- [ ] 1.1 Add "Import Patterns" section to `docs/explanation/concepts.md`
- [ ] 1.2 Document the three-level import hierarchy:
  - Top-level: `from fsspeckit import filesystem, AwsStorageOptions`
  - Package-level: `from fsspeckit.datasets import PyarrowDatasetIO`
  - Module-level: `from fsspeckit.sql.filters import sql2pyarrow_filter`

## 2. Standardize How-To Guides
- [ ] 2.1 Update imports in `docs/how-to/configure-cloud-storage.md`
- [ ] 2.2 Update imports in `docs/how-to/work-with-filesystems.md`
- [ ] 2.3 Update imports in `docs/how-to/read-and-write-datasets.md`
- [ ] 2.4 Update imports in `docs/how-to/merge-datasets.md`
- [ ] 2.5 Update imports in `docs/how-to/sync-and-manage-files.md`
- [ ] 2.6 Update imports in `docs/how-to/optimize-performance.md`
- [ ] 2.7 Update imports in `docs/how-to/use-sql-filters.md`

## 3. Standardize Tutorials
- [ ] 3.1 Update imports in `docs/tutorials/getting-started.md`
- [ ] 3.2 Remove "Legacy import (still works but deprecated)" comments

## 4. Standardize Reference and Explanation
- [ ] 4.1 Update imports in `docs/reference/api-guide.md`
- [ ] 4.2 Update imports in `docs/explanation/concepts.md`
- [ ] 4.3 Update imports in `docs/explanation/architecture.md`

## 5. Validate Consistency
- [ ] 5.1 Verify all import statements across docs use consistent patterns
- [ ] 5.2 Run MkDocs build to ensure no issues
- [ ] 5.3 Spot-check that imports work with current package structure
