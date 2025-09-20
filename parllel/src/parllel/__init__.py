"""Parllel - A high-performance async pipeline processing library for Python."""

from .pipeline import Pipeline
from .stage import Stage, work_pool, mix_pool, as_stage, PathChoice
from .util import (
    Err,
    Result,
    err_as_value,
    get_err,
    is_err,
    is_ok,
    unwrap,
    unwrap_or,
    with_retry,
)
from .async_util import SENTINEL
from .worker_state import WorkerHandler, WorkerState

__version__ = "0.1.0"
__author__ = "Aaron Hough"
__email__ = "aaron@runmodular.com"

__all__ = [
    "Pipeline",
    "Stage", 
    "work_pool",
    "mix_pool", 
    "as_stage",
    "PathChoice",
    "Result",
    "Err",
    "err_as_value",
    "get_err",
    "is_err", 
    "is_ok",
    "unwrap",
    "unwrap_or",
    "with_retry",
    "WorkerHandler",
    "WorkerState",
    "SENTINEL"
]
