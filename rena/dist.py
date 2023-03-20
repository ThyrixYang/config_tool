import os
from typing import Any, Callable, Optional, Union
from functools import partial, wraps

_global_lock_file = "s3://tmp/rena_global_lock.lock"

def _get_rank() -> int:
    # SLURM_PROCID can be set even if SLURM is not managing the multiprocessing,
    # therefore LOCAL_RANK needs to be checked first
    rank_keys = ("RANK", "LOCAL_RANK", "SLURM_PROCID", "JSM_NAMESPACE_RANK")
    for key in rank_keys:
        rank = os.environ.get(key)
        if rank is not None:
            return int(rank)
    return 0

local_rank = _get_rank()

def rank_zero_only(fn: Callable) -> Callable:
    """Function that can be used as a decorator to enable a function/method being called only on rank 0."""

    @wraps(fn)
    def wrapped_fn(*args: Any, **kwargs: Any) -> Optional[Any]:
        if local_rank == 0:
            return fn(*args, **kwargs)
        return None

    return wrapped_fn
