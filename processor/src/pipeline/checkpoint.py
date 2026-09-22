import json
import os
from pathlib import Path
from threading import Lock


class FileCheckpointStore:
    """Small local checkpoint store used by the Docker demo."""

    def __init__(self, path: str) -> None:
        self.path = Path(path)
        self._lock = Lock()

    def load(self) -> dict[str, str]:
        with self._lock:
            if not self.path.exists():
                return {}
            try:
                value = json.loads(self.path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                raise RuntimeError(f"Could not read checkpoint file {self.path}") from exc
            if not isinstance(value, dict) or not all(
                isinstance(key, str) and isinstance(sequence, str)
                for key, sequence in value.items()
            ):
                raise RuntimeError(f"Checkpoint file {self.path} has an invalid format")
            return value

    def save(self, checkpoints: dict[str, str]) -> None:
        with self._lock:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            temp_path = self.path.with_suffix(self.path.suffix + ".tmp")
            temp_path.write_text(
                json.dumps(checkpoints, indent=2, sort_keys=True),
                encoding="utf-8",
            )
            os.replace(temp_path, self.path)

