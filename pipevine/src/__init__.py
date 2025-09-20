"""
Pipevine - A high-performance async pipeline processing library for Python.

This library provides tools for building concurrent data processing pipelines
with backpressure control, automatic error handling, and support for both
async/await and multiprocessing execution models.
"""

from .pipevine.pipeline import Pipeline
from .pipevine.stage import Stage, work_pool, mix_pool, as_stage, PathChoice
from .pipevine.util import Result, Err, is_err, unwrap, with_retry
from .pipevine.worker_state import WorkerHandler, WorkerState
from .pipevine.async_util import SENTINEL

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
    "is_err", 
    "unwrap",
    "with_retry",
    "WorkerHandler",
    "WorkerState",
    "SENTINEL"
]