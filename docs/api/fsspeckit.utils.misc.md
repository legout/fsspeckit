# `fsspeckit.utils.misc` (compatibility façade)

The `fsspeckit.utils.misc` module is a **backwards-compatibility façade**.  
It re-exports a small set of helpers from `fsspeckit.common.misc` and `rich.progress` so that
existing code continues to work after the domain refactor.

New code SHOULD import from the canonical modules instead of this façade.

## Exposed helpers

| Compat name                  | Canonical helper          | Canonical module                 |
|------------------------------|---------------------------|----------------------------------|
| `get_partitions_from_path`   | `get_partitions_from_path`| `fsspeckit.common.misc`         |
| `run_parallel`               | `run_parallel`            | `fsspeckit.common.misc`         |
| `sync_dir`                   | `sync_dir`                | `fsspeckit.common.misc`         |
| `sync_files`                 | `sync_files`              | `fsspeckit.common.misc`         |
| `Progress`                   | `Progress`                | `rich.progress.Progress`        |

> The full, up-to-date API documentation for these helpers lives under
> `fsspeckit.common` in the domain package reference.

## Recommended imports

**Preferred (domain packages):**

```python
from fsspeckit.common.misc import run_parallel, sync_dir
from rich.progress import Progress
```

**Backwards-compatible (façade):**

```python
from fsspeckit.utils import run_parallel, sync_dir
from fsspeckit.utils.misc import Progress
```

Use the façade only when maintaining legacy code; for new development, always
prefer the canonical imports from `fsspeckit.common.misc`.
