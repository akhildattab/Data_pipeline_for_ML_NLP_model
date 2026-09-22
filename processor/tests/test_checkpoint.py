import json

import pytest

from pipeline.checkpoint import FileCheckpointStore


def test_checkpoint_round_trip(tmp_path) -> None:
    path = tmp_path / "nested" / "checkpoints.json"
    store = FileCheckpointStore(str(path))

    assert store.load() == {}
    store.save({"shard-1": "101", "shard-2": "205"})
    assert store.load() == {"shard-1": "101", "shard-2": "205"}


def test_invalid_checkpoint_fails_loudly(tmp_path) -> None:
    path = tmp_path / "checkpoints.json"
    path.write_text(json.dumps(["not", "a", "mapping"]))

    with pytest.raises(RuntimeError, match="invalid format"):
        FileCheckpointStore(str(path)).load()

