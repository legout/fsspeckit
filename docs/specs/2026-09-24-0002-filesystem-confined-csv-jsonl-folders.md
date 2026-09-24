# Filesystem-confined CSV and JSON(L) folders

**Status:** Approved behavioral design, not implementation authorization. In the 2026-09-24 conversation, the owner chose JSONL part writes including frame/table rows, empty results for empty folders and unmatched globs, and unioned columns across files; the owner then approved the complete folder design (“yes. approved”). This approval covers the behavior below, not an implementation plan or code changes.

## Boundary and scope

This specification covers folder and glob reads through `fs.read_csv`, `fs.read_json`, and `fs.read_files`, plus CSV and JSONL part-file writes through `fs.write_files`. Discovery, reads, writes, and existence checks use the supplied fsspec filesystem and its logical paths. The caller configures that filesystem, including its credentials and root; Polars URL `storage_options` cannot stand in for an arbitrary supplied filesystem such as `DirFileSystem`.

The [filesystem-confined writer specification](2026-09-24-0001-filesystem-confined-format-writes.md) separately owns direct `write_parquet`, `write_csv`, and `write_json` repairs. Direct writes target one file. `write_files(mode="append")` creates a new part file instead of appending bytes to an existing file. Neither specification makes unsupported object-store append work.

## Folder reads

- A named file is read as that file. For an existing directory, discover matching files recursively; for an explicit glob, use its matches. Select `.csv` for CSV, `.json` for ordinary JSON, and `.jsonl` when `jsonlines=True`. Do not implicitly mix JSON documents with JSON Lines. Recognize files by their suffix, not by an extension substring elsewhere in the directory name; a directory literally named `csv` must work.
- Keep the existing Polars DataFrame return type and `diagonal_relaxed` union for CSV and JSONL DataFrame reads. If files have different columns, retain all columns and fill absent values with nulls. Preserve `include_file_path` with logical paths. Raw JSON reads retain their per-file grouping rather than silently flattening records across files.
- `batch_size` counts **files**, not rows. For DataFrame output with `concat=True`, each nonempty batch yields a DataFrame even when it contains just one file; with `concat=False`, return a list of per-file results. A batched read with no matches yields no batches.
- For an existing directory with no matching files or an unmatched explicit glob, `concat=True` DataFrame reads return a zero-column `pl.DataFrame()` (including when `include_file_path=True`); list or raw reads return `[]`. A specifically named missing file or directory still raises `FileNotFoundError`, rather than looking like an empty existing folder.
- `read_files(format="jsonl")` selects the JSON Lines reader and `.jsonl` discovery. Existing `read_files(format="json", jsonlines=True)` remains supported; `format="json"` without that flag selects ordinary JSON.

## Folder writes

- Keep existing CSV part-file writes through `write_files(path=folder, format="csv", mode="append")`. Add `format="jsonl"` for the same folder/append operation. Each invocation creates a distinct `.jsonl` part via the supplied filesystem; it does not append to an existing object or change direct `write_json` behavior. The destination must be writable through that filesystem; automatic directory creation and remote atomicity are not promised.
- For JSONL parts, a dictionary is one record, a list of dictionaries is one record per element, and Polars frames/lazy frames, Pandas frames, and Arrow tables serialize **one row per line**. Each record is compact JSON followed by a newline. An Arrow table need not be converted through Polars; unsupported values surface the serializer's error rather than being silently stringified. This row-oriented contract applies to JSONL parts only; direct plain `write_json` keeps its column-oriented frame/table payload.
- An append to a folder must never replace an existing part: generated names are unique, and a caller-supplied name that collides fails before overwriting. For `format="jsonl"`, non-append `write_files` modes are outside this contract and fail explicitly rather than writing an ordinary JSON document under a `.jsonl` name. Existing CSV/JSON/Parquet mode semantics remain governed by their existing contracts.

## Acceptance examples

Use a `DirFileSystem` rooted separately from the working directory; assert results under its root and no same-named files or deletions in the working directory.

1. `read_csv("csv")` discovers two CSVs in a directory literally named `csv`; files with columns `x` and `y` yield both columns and nulls where missing. With `batch_size=1, concat=True`, each batch is a DataFrame, not a list.
2. `read_json("logs", jsonlines=True)` and `read_files("logs", format="jsonl")` discover `.jsonl` files, union differing record columns for DataFrame output, and do not absorb a neighboring `.json` file. Raw multi-file JSONL output remains grouped by file.
3. An existing empty folder or unmatched glob returns the specified empty result (or zero batches); a named missing file or directory raises `FileNotFoundError`.
4. `write_files(..., path="out", format="jsonl", mode="append")` writes row records from dictionaries, a Polars frame, a Pandas frame, and an Arrow table. A second write creates another part without changing the first; a colliding explicit name cannot overwrite a part. Each part is readable as JSON Lines through the supplied filesystem.
5. Existing `write_files(..., format="csv", mode="append")` still creates a CSV part; direct `write_json(..., append=True)` remains a single-file operation with the payload contract in the writer specification.

## Non-goals and handoff

No URL-native Polars API, new dependency, PyArrow dataset replacement for the Polars reader, row-level streaming guarantee, automatic schema normalization beyond the stated column union, remote credentialed test, object-store atomic publication, or change to Parquet dataset handlers. No new domain vocabulary or qualifying ADR is required for this existing file/folder terminology and reversible API behavior. The approved scope and examples above resolve the material design choices; execution authority and a plan remain separate gates. Planning contract: version 1; installed `legout/skills` source, exact revision unknown.
