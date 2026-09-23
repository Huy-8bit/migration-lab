import os
import json
import logging
from datetime import datetime
from pathlib import Path
from src.common.config import CHECKPOINT_DIR

logger = logging.getLogger(__name__)

class CheckpointManager:
    def __init__(self, checkpoint_dir: str = CHECKPOINT_DIR):
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)

    def _get_file_path(self, table_name: str) -> Path:
        return self.checkpoint_dir / f"{table_name}.json"

    def get_checkpoint(self, table_name: str) -> dict | None:
        file_path = self._get_file_path(table_name)
        if file_path.exists():
            try:
                with open(file_path, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Failed to read checkpoint for {table_name}: {e}")
        return None

    def save_checkpoint(self, table_name: str, last_processed_id: int, records_processed: int, status: str):
        file_path = self._get_file_path(table_name)
        data = {
            "table": table_name,
            "last_processed_id": last_processed_id,
            "records_processed": records_processed,
            "status": status,
            "timestamp": datetime.now().isoformat()
        }
        try:
            with open(file_path, 'w') as f:
                json.dump(data, f)
        except Exception as e:
            logger.error(f"Failed to save checkpoint for {table_name}: {e}")

    def is_table_complete(self, table_name: str) -> bool:
        checkpoint = self.get_checkpoint(table_name)
        return checkpoint is not None and checkpoint.get("status") == "complete"

    def get_resume_id(self, table_name: str) -> int:
        checkpoint = self.get_checkpoint(table_name)
        if checkpoint and checkpoint.get("status") != "complete":
            return checkpoint.get("last_processed_id", 0)
        return 0

    def clear_all(self):
        for file_path in self.checkpoint_dir.glob("*.json"):
            try:
                file_path.unlink()
            except Exception as e:
                logger.error(f"Failed to delete checkpoint file {file_path}: {e}")

    def get_summary(self) -> list[dict]:
        summary = []
        for file_path in self.checkpoint_dir.glob("*.json"):
            try:
                with open(file_path, 'r') as f:
                    summary.append(json.load(f))
            except Exception:
                pass
        return summary
