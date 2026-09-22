import json
from types import SimpleNamespace

import pytest

from pipeline.consumer import KinesisToS3Pipeline


class MemoryCheckpointStore:
    def __init__(self) -> None:
        self.value = {}
        self.save_calls = 0

    def load(self) -> dict[str, str]:
        return self.value.copy()

    def save(self, checkpoints: dict[str, str]) -> None:
        self.save_calls += 1
        self.value = checkpoints.copy()


class RecordingWriter:
    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.records = []

    def write(self, records: list[dict]) -> list[str]:
        if self.fail:
            raise RuntimeError("simulated S3 failure")
        self.records.extend(records)
        return ["enriched_articles/batch.parquet"]


def settings():
    return SimpleNamespace(
        stream_name="MyStream",
        batch_size=10,
        max_records_per_read=100,
        flush_interval_seconds=5,
        poll_interval_seconds=0.01,
        starting_position="TRIM_HORIZON",
    )


def kinesis_record(sequence: str = "10") -> dict:
    return {
        "SequenceNumber": sequence,
        "Data": json.dumps(
            {"article_id": "article-1", "content": "three word article"}
        ).encode(),
    }


def test_flush_writes_before_committing_checkpoint() -> None:
    checkpoint_store = MemoryCheckpointStore()
    writer = RecordingWriter()
    pipeline = KinesisToS3Pipeline(
        settings=settings(),
        kinesis_client=object(),
        writer=writer,
        checkpoint_store=checkpoint_store,
    )

    pipeline._process_record("shard-1", kinesis_record())
    pipeline._flush()

    assert len(writer.records) == 1
    assert checkpoint_store.value == {"shard-1": "10"}
    assert pipeline.average.value == 3


def test_failed_write_does_not_advance_checkpoint() -> None:
    checkpoint_store = MemoryCheckpointStore()
    pipeline = KinesisToS3Pipeline(
        settings=settings(),
        kinesis_client=object(),
        writer=RecordingWriter(fail=True),
        checkpoint_store=checkpoint_store,
    )
    pipeline._process_record("shard-1", kinesis_record())

    with pytest.raises(RuntimeError, match="simulated S3 failure"):
        pipeline._flush()

    assert checkpoint_store.value == {}
    assert len(pipeline.batch) == 1


def test_invalid_record_is_skipped_but_checkpointed() -> None:
    checkpoint_store = MemoryCheckpointStore()
    writer = RecordingWriter()
    pipeline = KinesisToS3Pipeline(
        settings=settings(),
        kinesis_client=object(),
        writer=writer,
        checkpoint_store=checkpoint_store,
    )

    pipeline._process_record(
        "shard-1",
        {"SequenceNumber": "11", "Data": b"not-json"},
    )
    pipeline._flush()

    assert writer.records == []
    assert pipeline.invalid_records == 1
    assert checkpoint_store.value == {"shard-1": "11"}
