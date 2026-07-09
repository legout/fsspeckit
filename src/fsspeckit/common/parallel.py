"""Parallel execution utilities for fsspeckit."""

import os
from collections.abc import Iterable
from typing import Any, Callable

from rich.progress import BarColumn, Progress, TextColumn, TimeElapsedColumn


def _prepare_parallel_args(
    args: tuple, kwargs: dict
) -> tuple[list, list, dict, dict, int]:
    """Prepare and validate arguments for parallel execution.

    Args:
        args: Positional arguments
        kwargs: Keyword arguments

    Returns:
        tuple: (iterables, fixed_args, iterable_kwargs, fixed_kwargs, first_iterable_len)

    Raises:
        ValueError: If no iterable arguments or length mismatch
    """
    iterables = []
    fixed_args = []
    iterable_kwargs = {}
    fixed_kwargs = {}
    first_iterable_len = None

    # Process positional arguments
    for arg in args:
        # Accept any non-string Iterable (including generators)
        if isinstance(arg, Iterable) and not isinstance(arg, (str, bytes)):
            # Convert to list to materialize generators and get length
            materialized_arg = list(arg)
            iterables.append(materialized_arg)
            if first_iterable_len is None:
                first_iterable_len = len(materialized_arg)
            elif len(materialized_arg) != first_iterable_len:
                raise ValueError("All iterables must have the same length")
        else:
            fixed_args.append(arg)

    # Process keyword arguments
    for key, value in kwargs.items():
        # Accept any non-string Iterable (including generators)
        if isinstance(value, Iterable) and not isinstance(value, (str, bytes)):
            # Convert to list to materialize generators and get length
            materialized_value = list(value)
            if first_iterable_len is None:
                first_iterable_len = len(materialized_value)
            elif len(materialized_value) != first_iterable_len:
                raise ValueError("All iterables must have the same length")
            iterable_kwargs[key] = materialized_value
        else:
            fixed_kwargs[key] = value

    if first_iterable_len is None:
        raise ValueError("At least one iterable argument must be provided")

    return iterables, fixed_args, iterable_kwargs, fixed_kwargs, first_iterable_len


def _execute_parallel_with_progress(
    func: Callable,
    iterables: list,
    fixed_args: list,
    iterable_kwargs: dict,
    fixed_kwargs: dict,
    param_combinations: list,
    parallel_kwargs: dict,
) -> list:
    """Execute parallel tasks with progress tracking.

    Args:
        func: Function to execute
        iterables: List of iterable arguments
        fixed_args: List of fixed arguments
        iterable_kwargs: Dictionary of iterable keyword arguments
        fixed_kwargs: Dictionary of fixed keyword arguments
        param_combinations: List of parameter combinations
        parallel_kwargs: Parallel execution configuration

    Returns:
        list: Results from parallel execution
    """
    from fsspeckit.common.optional import _import_joblib

    joblib = _import_joblib()
    Parallel = joblib.Parallel
    delayed = joblib.delayed

    results = [None] * len(param_combinations)
    with Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        "[progress.percentage]{task.percentage:>3.0f}%",
        TimeElapsedColumn(),
        transient=True,
    ) as progress:
        task = progress.add_task(
            "Running in parallel...", total=len(param_combinations)
        )

        def wrapper(idx, param_tuple):
            res = func(
                *(list(param_tuple[: len(iterables)]) + fixed_args),
                **{
                    k: v
                    for k, v in zip(
                        iterable_kwargs.keys(), param_tuple[len(iterables) :]
                    )
                },
                **fixed_kwargs,
            )
            progress.update(task, advance=1)
            return idx, res

        for idx, result in Parallel(**parallel_kwargs)(
            delayed(wrapper)(i, param_tuple)
            for i, param_tuple in enumerate(param_combinations)
        ):
            results[idx] = result
    return results


def _execute_parallel_without_progress(
    func: Callable,
    iterables: list,
    fixed_args: list,
    iterable_kwargs: dict,
    fixed_kwargs: dict,
    param_combinations: list,
    parallel_kwargs: dict,
) -> list:
    """Execute parallel tasks without progress tracking.

    Args:
        func: Function to execute
        iterables: List of iterable arguments
        fixed_args: List of fixed arguments
        iterable_kwargs: Dictionary of iterable keyword arguments
        fixed_kwargs: Dictionary of fixed keyword arguments
        param_combinations: List of parameter combinations
        parallel_kwargs: Parallel execution configuration

    Returns:
        list: Results from parallel execution
    """
    from fsspeckit.common.optional import _import_joblib

    joblib = _import_joblib()
    Parallel = joblib.Parallel
    delayed = joblib.delayed

    return Parallel(**parallel_kwargs)(
        delayed(func)(
            *(list(param_tuple[: len(iterables)]) + fixed_args),
            **{
                k: v
                for k, v in zip(
                    iterable_kwargs.keys(), param_tuple[len(iterables) :]
                )
            },
            **fixed_kwargs,
        )
        for param_tuple in param_combinations
    )


def run_parallel(
    func: Callable,
    *args,
    n_jobs: int = -1,
    backend: str = "threading",
    verbose: bool = True,
    **kwargs,
) -> list[Any]:
    """Runs a function for a list of parameters in parallel.

    Requires: fsspeckit[datasets] extra for joblib dependency.

    Args:
        func (Callable): function to run in parallel
        *args: Positional arguments. Can be single values or any non-string iterables (including generators)
        n_jobs (int, optional): Number of joblib workers. Defaults to -1
        backend (str, optional): joblib backend. Valid options are
            `loky`,`threading`,`multiprocessing` or `sequential`. Defaults to "threading"
        verbose (bool, optional): Show progress bar. Defaults to True
        **kwargs: Keyword arguments. Can be single values or any non-string iterables (including generators)

    Returns:
        list[any]: Function output

    Raises:
        ImportError: If joblib is not available. Install with: pip install fsspeckit[datasets]
        ValueError: If no iterable arguments are provided or iterables have different lengths

    Examples:
        >>> # Single iterable argument
        >>> run_parallel(func, [1,2,3], fixed_arg=42)

        >>> # Multiple iterables in args and kwargs
        >>> run_parallel(func, [1,2,3], val=[7,8,9], fixed=42)

        >>> # Only kwargs iterables
        >>> run_parallel(func, x=[1,2,3], y=[4,5,6], fixed=42)

        >>> # Generator support
        >>> def gen():
        ...     yield from [1, 2, 3]
        >>> run_parallel(str, gen())  # Returns ['1', '2', '3']
    """
    if backend == "threading" and n_jobs == -1:
        n_jobs = min(256, (os.cpu_count() or 1) + 4)

    parallel_kwargs = {"n_jobs": n_jobs, "backend": backend, "verbose": 0}

    # Prepare and validate arguments
    iterables, fixed_args, iterable_kwargs, fixed_kwargs, first_iterable_len = (
        _prepare_parallel_args(args, kwargs)
    )

    # Create parameter combinations
    all_iterables = iterables + list(iterable_kwargs.values())

    # Handle empty iterables case
    if first_iterable_len == 0:
        return []

    param_combinations = list(zip(*all_iterables))

    # Execute with or without progress tracking
    if not verbose:
        return _execute_parallel_without_progress(
            func,
            iterables,
            fixed_args,
            iterable_kwargs,
            fixed_kwargs,
            param_combinations,
            parallel_kwargs,
        )
    else:
        return _execute_parallel_with_progress(
            func,
            iterables,
            fixed_args,
            iterable_kwargs,
            fixed_kwargs,
            param_combinations,
            parallel_kwargs,
        )
