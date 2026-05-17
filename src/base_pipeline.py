"""
src/base_pipeline.py – Abstract base class shared by all four pipelines.
"""

import uuid
import time
from abc import ABC, abstractmethod
from src.db_manager import DBManager


class BasePipeline(ABC):
    """Common state and lifecycle helpers for every pipeline."""

    def __init__(self, pipeline_name: str, data_file, batch_size: int,
                 db_type: str = "postgresql", query_name: str = "all"):
        self.pipeline_name  = pipeline_name
        self.data_file      = data_file          # str or list[str]
        self.batch_size     = batch_size
        self.db_type        = "postgresql"       # always PostgreSQL
        self.query_name = query_name

        # Runtime counters
        self.run_id          = str(uuid.uuid4())[:12]
        self.batch_count     = 0
        self.total_records   = 0
        self.malformed_records = 0
        self.start_time      = None
        self.end_time        = None

        self.db_manager = DBManager("postgresql")

    # ── DB lifecycle wrappers ─────────────────────────────────────────────────

    def connect_db(self) -> bool:
        ok = self.db_manager.connect()
        if ok:
            data_label = (
                ", ".join(self.data_file)
                if isinstance(self.data_file, list)
                else self.data_file
            )
            self.db_manager.create_run(
                self.run_id, self.pipeline_name,
                self.batch_size, data_label,
            )
        return ok

    def disconnect_db(self):
        self.db_manager.disconnect()

    def save_metadata(self):
        elapsed = (self.end_time or time.time()) - (self.start_time or time.time())
        avg = self.total_records / self.batch_count if self.batch_count else 0
        self.db_manager.finish_run(
            self.run_id, self.total_records, self.malformed_records,
            self.batch_count, avg, elapsed,
        )

    def get_status_string(self) -> str:
        elapsed = (self.end_time or time.time()) - (self.start_time or time.time())
        return (
            f"\n{'─'*50}\n"
            f"Pipeline  : {self.pipeline_name}\n"
            f"Run ID    : {self.run_id}\n"
            f"Records   : {self.total_records:,}\n"
            f"Malformed : {self.malformed_records:,}\n"
            f"Batches   : {self.batch_count}\n"
            f"Elapsed   : {elapsed:.2f}s\n"
            f"{'─'*50}"
        )

    # ── Subclass contract ─────────────────────────────────────────────────────

    @abstractmethod
    def execute(self) -> bool:
        """Run the full pipeline. Return True on success."""
