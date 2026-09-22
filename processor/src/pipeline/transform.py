import json
from datetime import datetime, timezone
from typing import Callable


class InvalidRecord(ValueError):
    """Raised when a Kinesis payload cannot be converted to an article."""


def count_words(content: str) -> int:
    """Count whitespace-delimited tokens after trimming the input."""
    return len(content.split())


def enrich_record(
    payload: bytes,
    *,
    stream_name: str,
    shard_id: str,
    sequence_number: str,
    now: Callable[[], datetime] | None = None,
) -> dict:
    try:
        article = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise InvalidRecord("payload is not valid UTF-8 JSON") from exc

    if not isinstance(article, dict):
        raise InvalidRecord("payload must be a JSON object")

    missing = [name for name in ("article_id", "content") if name not in article]
    if missing:
        raise InvalidRecord(f"missing required field(s): {', '.join(missing)}")
    if not isinstance(article["article_id"], str) or not article["article_id"].strip():
        raise InvalidRecord("article_id must be a non-empty string")
    if not isinstance(article["content"], str):
        raise InvalidRecord("content must be a string")

    timestamp = (now or (lambda: datetime.now(timezone.utc)))()
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    timestamp = timestamp.astimezone(timezone.utc)

    return {
        "article_id": article["article_id"],
        "title": article.get("title"),
        "author": article.get("author"),
        "publish_date": article.get("publish_date"),
        "content": article["content"],
        "word_count": count_words(article["content"]),
        "ingested_at": timestamp.isoformat().replace("+00:00", "Z"),
        "source_stream": stream_name,
        "source_shard": shard_id,
        "source_sequence_number": sequence_number,
    }


class RunningAverage:
    def __init__(self) -> None:
        self.count = 0
        self.total = 0

    def add(self, value: int) -> None:
        self.count += 1
        self.total += value

    @property
    def value(self) -> float:
        return self.total / self.count if self.count else 0.0

