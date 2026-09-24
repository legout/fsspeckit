# Filesystem-confined Parquet, CSV, and JSON writes

**Status:** Approved writer design, not implementation authorization. The owner approved the writer contract in the 2026-09-24 conversation (“looks good. record to ../fsspeckit/”) and subsequently confirmed the supplied-fsspec boundary and separate folder design. Implementation and any changed behavior require their own handoff.

## Scope and invariant

Fix the existing filesystem-extension writers (`fs.write_parquet`, `fs.write_csv`, `fs.write_json`) at their source. Every write, append, and deletion performed by these paths must use the supplied fsspec filesystem and its logical path, never a bare local path or its underlying `fs` directly. The caller configures that filesystem, including credentials and root, before invoking these instance methods. Polars `storage_options` on a path cannot substitute for an arbitrary supplied fsspec filesystem such as `DirFileSystem`; pass Polars a file handle from `self.open` for CSV. A separate URL-native API is outside this contract. Keep the current format-specific backends and return types rather than making IDL supply workarounds.

The defects were reproduced in fsspeckit 0.28.1 at `3f5f542` with a credential-free `DirFileSystem` rooted separately from the working directory: an Arrow-table Parquet write raises `AttributeError: 'list' object has no attribute 'schema'`; an Arrow-table CSV write lands in the working directory instead of the filesystem root; appending JSON to an earlier plain write produces invalid JSON Lines.

## Format contracts

### Parquet

- Keep PyArrow `pq.write_table(data, path, filesystem=self, ...)`, optional `schema`, and the returned `pyarrow.parquet.FileMetaData` with its logical file path set. Preserve the existing `cast_schema` behavior when a schema is supplied.
- Repair `datasets/types.py:to_pyarrow_table(..., concat=True)` so a single `pa.Table` and a nonempty list of Arrow tables produce a **table**, not a list. Keep Arrow inputs on an Arrow-native path; preserve the existing Polars, Pandas, and dictionary conversions and `concat=False` behavior. The writer already requests `concat=True`; fixing only that call would leave the shared converter inconsistent.
- Reject empty or unsupported inputs before attempting a write, with a clear input error instead of an indexing or downstream schema error.

### CSV

- Normalize the currently accepted Polars frame/lazy frame, Arrow table, Pandas frame, dictionary, and list-of-dictionaries inputs to one Polars frame. Write **all** branches through `self.open(path, "wb")` or `self.open(path, "ab")` and `DataFrame.write_csv(file, **kwargs)`. Keep its existing formatting options and `None` return; do not pass a bare path to Polars.
- `append=False` replaces the file with a header according to the writer options. `append=True` to a missing or empty file writes a header by default; appending to a nonempty file does not duplicate it. If the nonempty file lacks a final newline, separate the appended rows before writing them. Conflicting explicit header options must fail before mutation rather than silently producing duplicate headers.
- If a filesystem cannot support the append or required tail check, surface its capability error rather than fall back to local I/O or claim that remote append succeeded.

### JSON

- Keep `orjson`, the existing `None` return, and the current payload shape: dictionaries write as dictionaries, lists of dictionaries as arrays for plain writes, and frames/tables as column-oriented dictionaries. Do **not** switch these to Polars' row-oriented JSON output.
- `append=False` writes one compact JSON document. `append=True` writes JSON Lines: one line per appended dictionary, or one line per element of a list of dictionaries. Before appending to a nonempty file, inspect its final byte through `self.open`; insert one newline if needed, then write newline-terminated record(s). A plain dictionary write followed by append yields separate JSON Lines values, readable with `jsonlines=True`; an earlier array or column-oriented frame/table value is not silently converted into homogeneous row records. Do not introduce a format toggle or silently rewrite earlier content.
- Retain the existing optional-dependency behavior except where it currently prevents supported inputs; no new JSON writer dependency is needed. Unsupported filesystem append/tail operations must fail without local-path fallback.

## Shared dispatch and errors

`write_files(..., format="parquet" | "csv" | "json")` must reach these corrected writers without escaping `DirFileSystem`. Its `mode="append"` creates another output file; it is distinct from direct `write_csv(..., append=True)` and `write_json(..., append=True)`, which append to one file. Do not redesign dispatch or change that mode contract here. The existing `write_files` overwrite branch calls `self.fs.rm(path)`; use the supplied filesystem boundary there too if exercising that branch, and cover the overwrite path with a confinement check. Preserve native serialization errors and filesystem capability errors rather than masking them as success.

## Acceptance examples

Use a `DirFileSystem` rooted under one temporary directory and set the working directory to a different temporary directory; assert **both** that expected files exist under the filesystem root and that no same-named files appear in the working directory.

1. Direct `write_parquet(pa.table({"x": [1]}), "a.parquet")` reads back one row and returns metadata for the logical path. Also cover a list of Arrow tables, a supplied schema, and empty/unsupported data.
2. Direct CSV writes for Arrow, Pandas, Polars, and a dictionary/list-of-dictionaries input all stay inside the root and read back. Appending to a missing, empty, terminated, and unterminated file yields one header and complete rows; unsupported append fails explicitly.
3. `write_json({"x": 1}, "events.jsonl")` followed by `write_json({"x": 2}, "events.jsonl", append=True)` yields two valid JSON Lines and no concatenated JSON tokens. A plain frame/table write retains column-oriented JSON; appending a list of dictionaries yields one line per record.
4. `write_files` dispatch for each format is confined; overwrite does not remove a same-named file outside the filesystem root. These are local, credential-free tests only.

## Boundaries and handoff

No live IDL tests, credentialed reads/writes, or remote deletion. This does not make fsspeckit a drop-in IDL replacement, change readers or `sync_folder`, redesign dataset modes, or promise append support from object stores lacking it. Folder discovery, file-batch reads, and segmented CSV/JSONL writes belong to the separate [folder specification](2026-09-24-0002-filesystem-confined-csv-jsonl-folders.md); this repair does not add `format="jsonl"` to `write_files` or make direct append a folder operation. PyArrow remains the Parquet writer; Polars remains the CSV writer because PyArrow CSV does not closely replace its formatting options; `orjson` remains the JSON writer because PyArrow's JSON interface is read-oriented. No new domain vocabulary or ADR is needed for these repairs to the existing backend choices. Material design decisions are captured above; no unresolved behavior decision is required for this specification. Planning contract: version 1; installed skill revision: unknown.
