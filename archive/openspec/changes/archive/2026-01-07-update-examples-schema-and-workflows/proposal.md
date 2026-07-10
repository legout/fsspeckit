# Change: Update Schema and Workflow Examples to Current Dependencies

## Why
The intermediate examples under `examples/datasets/schema/*` and `examples/datasets/workflows/*` are currently broken and/or unsafe:

- PyArrow API drift: `pa.table(list_of_dicts)` failures on current PyArrow
- Removed helpers: `pa.types.is_numeric` no longer exists
- Incorrect path operations: `Path.mkdir(parents_only=True)` is not a valid API
- Unsafe cleanup: broad `shutil.rmtree(dataset1_path.parent.parent)` can attempt to delete unrelated `/tmp` contents
- Workflow examples include outdated storage option fields and unrealistic cloud simulations

These scripts should be corrected to run offline, be safe, and match current APIs.

## What Changes
- Update schema examples to:
  - Use current PyArrow APIs and type checks (`is_integer`/`is_floating` etc.)
  - Fix path creation and safe cleanup
  - Keep datasets small enough for quick runs
- Update workflow examples to:
  - Use current PyArrow dataset creation methods
  - Avoid requiring real cloud credentials by default (local-first)
  - Remove or clearly gate any “real cloud” sections behind explicit configuration/flags

## Impact
- Restores mid-level learning path scripts and makes them reliable regression checks.
- Prevents accidental deletion of unrelated filesystem paths during example cleanup.

