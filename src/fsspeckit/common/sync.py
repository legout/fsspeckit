"""Filesystem synchronization utilities for fsspeckit."""

import posixpath

from fsspec import AbstractFileSystem

from fsspeckit.common.logging import get_logger
from fsspeckit.common.parallel import run_parallel

logger = get_logger(__name__)


def _get_root_fs(fs: AbstractFileSystem) -> AbstractFileSystem:
    """Get the root filesystem by unwrapping nested filesystems."""
    while hasattr(fs, "fs"):
        fs = fs.fs
    return fs


def check_fs_identical(fs1: AbstractFileSystem, fs2: AbstractFileSystem) -> bool:
    """Check if two fsspec filesystems are identical.

    Args:
        fs1: First filesystem (fsspec AbstractFileSystem)
        fs2: Second filesystem (fsspec AbstractFileSystem)

    Returns:
        bool: True if filesystems are identical, False otherwise
    """
    fs1 = _get_root_fs(fs1)
    fs2 = _get_root_fs(fs2)
    return fs1 == fs2


def server_side_copy_file(key, src_mapper, dst_mapper, RETRIES):
    """Copy a single file using server-side copy."""
    for attempt in range(1, RETRIES + 1):
        try:
            dst_mapper[key] = src_mapper[key]
            break
        except (OSError, IOError) as e:
            if attempt == RETRIES:
                logger.error(
                    "Failed to copy file %s after %d attempts: %s",
                    key,
                    RETRIES,
                    str(e),
                    exc_info=True,
                )
                raise RuntimeError(
                    f"Failed to copy file {key} after {RETRIES} attempts"
                ) from e
        except Exception as e:
            if attempt == RETRIES:
                logger.error(
                    "Unexpected error copying file %s: %s",
                    key,
                    str(e),
                    exc_info=True,
                )
                raise RuntimeError(f"Unexpected error copying file {key}: {e}") from e


def copy_file(key, src_fs, dst_fs, src_path, dst_path, CHUNK, RETRIES):
    """Copy a single file between filesystems through the client."""
    for attempt in range(1, RETRIES + 1):
        try:
            with (
                src_fs.open(posixpath.join(src_path, key), "rb") as r,
                dst_fs.open(posixpath.join(dst_path, key), "wb") as w,
            ):
                while True:
                    chunk = r.read(CHUNK)
                    if not chunk:
                        break
                    w.write(chunk)
            break
        except (OSError, IOError) as e:
            if attempt == RETRIES:
                logger.error(
                    "Failed to copy file %s after %d attempts: %s",
                    key,
                    RETRIES,
                    str(e),
                    exc_info=True,
                )
                raise RuntimeError(
                    f"Failed to copy file {key} after {RETRIES} attempts"
                ) from e
        except Exception as e:
            if attempt == RETRIES:
                logger.error(
                    "Unexpected error copying file %s: %s",
                    key,
                    str(e),
                    exc_info=True,
                )
                raise RuntimeError(f"Unexpected error copying file {key}: {e}") from e


def delete_file(key, dst_fs, dst_path, RETRIES):
    """Delete a single file from a filesystem."""
    for attempt in range(1, RETRIES + 1):
        try:
            dst_fs.rm(posixpath.join(dst_path, key))
            break
        except (OSError, IOError) as e:
            if attempt == RETRIES:
                logger.error(
                    "Failed to delete file %s after %d attempts: %s",
                    key,
                    RETRIES,
                    str(e),
                    exc_info=True,
                )
                raise RuntimeError(
                    f"Failed to delete file {key} after {RETRIES} attempts"
                ) from e
        except Exception as e:
            if attempt == RETRIES:
                logger.error(
                    "Unexpected error deleting file %s: %s",
                    key,
                    str(e),
                    exc_info=True,
                )
                raise RuntimeError(f"Unexpected error deleting file {key}: {e}") from e


def sync_files(
    add_files: list[str],
    delete_files: list[str],
    src_fs: AbstractFileSystem,
    dst_fs: AbstractFileSystem,
    src_path: str = "",
    dst_path: str = "",
    server_side: bool = False,
    chunk_size: int = 8 * 1024 * 1024,
    parallel: bool = False,
    n_jobs: int = -1,
    verbose: bool = True,
) -> dict[str, list[str]]:
    """Sync files between two filesystems by copying new files and deleting old ones.

    Args:
        add_files: List of file paths to add (copy from source to destination)
        delete_files: List of file paths to delete from destination
        src_fs: Source filesystem (fsspec AbstractFileSystem)
        dst_fs: Destination filesystem (fsspec AbstractFileSystem)
        src_path: Base path in source filesystem. Default is root ('').
        dst_path: Base path in destination filesystem. Default is root ('').
        server_side: Whether to use server-side copy if supported. Default is False.
        chunk_size: Size of chunks to read/write files (in bytes). Default is 8MB.
        parallel: Whether to perform copy/delete operations in parallel. Default is False.
        n_jobs: Number of parallel jobs if parallel=True. Default is -1 (all cores).
        verbose: Whether to show progress bars. Default is True.

    Returns:
        dict: Summary of added and deleted files
    """
    CHUNK = chunk_size
    RETRIES = 3

    server_side = check_fs_identical(src_fs, dst_fs) and server_side

    src_mapper = src_fs.get_mapper(src_path)
    dst_mapper = dst_fs.get_mapper(dst_path)

    if len(add_files):
        # Copy new files
        if parallel:
            if server_side:
                try:
                    run_parallel(
                        server_side_copy_file,
                        add_files,
                        src_mapper=src_mapper,
                        dst_mapper=dst_mapper,
                        RETRIES=RETRIES,
                        n_jobs=n_jobs,
                        verbose=verbose,
                    )
                except (RuntimeError, OSError) as e:
                    logger.warning(
                        "Server-side copy failed for some files, falling back to client-side: %s",
                        str(e),
                    )
                    # Fallback to client-side copy if server-side fails
                    run_parallel(
                        copy_file,
                        add_files,
                        src_fs=src_fs,
                        dst_fs=dst_fs,
                        src_path=src_path,
                        dst_path=dst_path,
                        CHUNK=CHUNK,
                        RETRIES=RETRIES,
                        n_jobs=n_jobs,
                        verbose=verbose,
                    )

            else:
                run_parallel(
                    copy_file,
                    add_files,
                    src_fs=src_fs,
                    dst_fs=dst_fs,
                    src_path=src_path,
                    dst_path=dst_path,
                    CHUNK=CHUNK,
                    RETRIES=RETRIES,
                    n_jobs=n_jobs,
                    verbose=verbose,
                )
        else:
            if verbose:
                from rich.progress import track

                for key in track(
                    add_files,
                    description="Copying new files...",
                    total=len(add_files),
                ):
                    if server_side:
                        try:
                            server_side_copy_file(
                                key, src_mapper, dst_mapper, RETRIES
                            )
                        except (RuntimeError, OSError):
                            copy_file(
                                key, src_fs, dst_fs, src_path, dst_path, CHUNK, RETRIES
                            )
                    else:
                        copy_file(
                            key, src_fs, dst_fs, src_path, dst_path, CHUNK, RETRIES
                        )
            else:
                for key in add_files:
                    if server_side:
                        try:
                            server_side_copy_file(
                                key, src_mapper, dst_mapper, RETRIES
                            )
                        except (RuntimeError, OSError):
                            copy_file(
                                key, src_fs, dst_fs, src_path, dst_path, CHUNK, RETRIES
                            )
                    else:
                        copy_file(
                            key, src_fs, dst_fs, src_path, dst_path, CHUNK, RETRIES
                        )

    if len(delete_files):
        # Delete old files from destination
        if parallel:
            run_parallel(
                delete_file,
                delete_files,
                dst_fs=dst_fs,
                dst_path=dst_path,
                RETRIES=RETRIES,
                n_jobs=n_jobs,
                verbose=verbose,
            )
        else:
            if verbose:
                from rich.progress import track

                for key in track(
                    delete_files,
                    description="Deleting stale files...",
                    total=len(delete_files),
                ):
                    delete_file(key, dst_fs, dst_path, RETRIES)
            else:
                for key in delete_files:
                    delete_file(key, dst_fs, dst_path, RETRIES)

    return {"added_files": add_files, "deleted_files": delete_files}


def sync_dir(
    src_fs: AbstractFileSystem,
    dst_fs: AbstractFileSystem,
    src_path: str = "",
    dst_path: str = "",
    server_side: bool = True,
    chunk_size: int = 8 * 1024 * 1024,
    parallel: bool = False,
    n_jobs: int = -1,
    verbose: bool = True,
) -> dict[str, list[str]]:
    """Sync two directories between different filesystems.

    Compares files in the source and destination directories, copies new or updated files from source to destination,
    and deletes stale files from destination.

    Args:
        src_fs: Source filesystem (fsspec AbstractFileSystem)
        dst_fs: Destination filesystem (fsspec AbstractFileSystem)
        src_path: Path in source filesystem to sync. Default is root ('').
        dst_path: Path in destination filesystem to sync. Default is root ('').
        server_side: Whether to use server-side copy if supported. Default is True.
        chunk_size: Size of chunks to read/write files (in bytes). Default is 8MB.
        parallel: Whether to perform copy/delete operations in parallel. Default is False.
        n_jobs: Number of parallel jobs if parallel=True. Default is -1 (all cores).
        verbose: Whether to show progress bars. Default is True.

    Returns:
        dict: Summary of added and deleted files
    """

    src_mapper = src_fs.get_mapper(src_path)
    dst_mapper = dst_fs.get_mapper(dst_path)

    add_files = sorted(src_mapper.keys() - dst_mapper.keys())
    delete_files = sorted(dst_mapper.keys() - src_mapper.keys())

    return sync_files(
        add_files=add_files,
        delete_files=delete_files,
        src_fs=src_fs,
        dst_fs=dst_fs,
        src_path=src_path,
        dst_path=dst_path,
        chunk_size=chunk_size,
        server_side=server_side,
        parallel=parallel,
        n_jobs=n_jobs,
        verbose=verbose,
    )
