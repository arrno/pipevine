"""
Parllel - A high-performance async pipeline processing library for Python.

This library provides tools for building concurrent data processing pipelines
with backpressure control, automatic error handling, and support for both
async/await and multiprocessing execution models.
"""

from .parllel.pipeline import Pipeline
from .parllel.stage import Stage, work_pool, mix_pool, as_stage, PathChoice
from .parllel.util import Result, Err, is_err, unwrap, with_retry
from .parllel.worker_state import WorkerHandler, WorkerState
from .parllel.async_util import SENTINEL

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