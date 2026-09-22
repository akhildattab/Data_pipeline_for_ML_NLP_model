import json
from datetime import datetime, timezone

import pytest

from pipeline.transform import InvalidRecord, RunningAverage, count_words, enrich_record


def test_count_words_handles_repeated_whitespace() -> None:
    assert count_words("  one\n two\tthree  ") == 3
    assert count_words("") == 0


def test_enrich_record_adds_feature_and_source_metadata() -> None:
    article = {
        "article_id": "article-1",
        "title": "A title",
        "author": "Reporter",
        "publish_date": "2024-02-01T12:00:00",
        "content": "A short test article.",
    }
    result = enrich_record(
        json.dumps(article).encode(),
        stream_name="MyStream",
        shard_id="shardId-000000000000",
        sequence_number="42",
        now=lambda: datetime(2024, 2, 1, 13, 0, tzinfo=timezone.utc),
    )

    assert result["word_count"] == 4
    assert result["ingested_at"] == "2024-02-01T13:00:00Z"
    assert result["source_stream"] == "MyStream"
    assert result["source_sequence_number"] == "42"


@pytest.mark.parametrize(
    "payload",
    [
        b"not-json",
        json.dumps([]).encode(),
        json.dumps({"article_id": "article-1"}).encode(),
        json.dumps({"article_id": "", "content": "text"}).encode(),
        json.dumps({"article_id": "article-1", "content": None}).encode(),
    ],
)
def test_enrich_record_rejects_invalid_input(payload: bytes) -> None:
    with pytest.raises(InvalidRecord):
        enrich_record(
            payload,
            stream_name="MyStream",
            shard_id="shard-1",
            sequence_number="1",
        )


def test_running_average_is_incremental() -> None:
    average = RunningAverage()
    assert average.value == 0
    average.add(10)
    average.add(20)
    average.add(30)
    assert average.count == 3
    assert average.value == 20

