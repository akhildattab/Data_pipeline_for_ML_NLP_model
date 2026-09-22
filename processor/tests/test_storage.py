from io import BytesIO

import pyarrow.parquet as pq

from pipeline.storage import ParquetBatchWriter


class CapturingS3Client:
    def __init__(self) -> None:
        self.objects = []

    def put_object(self, **kwargs) -> None:
        self.objects.append(kwargs)


def article(sequence_number: str, hour: str = "13") -> dict:
    return {
        "article_id": f"article-{sequence_number}",
        "title": "Title",
        "author": "Reporter",
        "publish_date": "2024-02-01T12:00:00",
        "content": "three word article",
        "word_count": 3,
        "ingested_at": f"2024-02-01T{hour}:00:00Z",
        "source_stream": "MyStream",
        "source_shard": "shard-1",
        "source_sequence_number": sequence_number,
    }


def test_writer_creates_partitioned_parquet() -> None:
    s3 = CapturingS3Client()
    writer = ParquetBatchWriter(s3, "my-bucket", "enriched_articles")

    keys = writer.write([article("1"), article("2")])

    assert len(keys) == 1
    assert keys[0].startswith(
        "enriched_articles/ingestion_date=2024-02-01/ingestion_hour=13/batch-"
    )
    assert s3.objects[0]["Bucket"] == "my-bucket"
    table = pq.ParquetFile(BytesIO(s3.objects[0]["Body"])).read()
    assert table.num_rows == 2
    assert table.column("word_count").to_pylist() == [3, 3]


def test_writer_splits_records_across_hour_partitions() -> None:
    s3 = CapturingS3Client()
    writer = ParquetBatchWriter(s3, "my-bucket", "enriched_articles")

    keys = writer.write([article("1", "13"), article("2", "14")])

    assert len(keys) == 2
    assert any("ingestion_hour=13" in key for key in keys)
    assert any("ingestion_hour=14" in key for key in keys)


def test_object_key_is_deterministic_for_the_same_batch() -> None:
    s3 = CapturingS3Client()
    writer = ParquetBatchWriter(s3, "my-bucket", "enriched_articles")
    records = [article("1"), article("2")]

    assert writer.write(records) == writer.write(records)
