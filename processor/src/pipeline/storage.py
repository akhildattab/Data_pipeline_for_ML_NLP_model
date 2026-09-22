from collections import defaultdict
from hashlib import sha256
from io import BytesIO
from typing import Iterable

import pyarrow as pa
import pyarrow.parquet as pq


ARTICLE_SCHEMA = pa.schema(
    [
        ("article_id", pa.string()),
        ("title", pa.string()),
        ("author", pa.string()),
        ("publish_date", pa.string()),
        ("content", pa.string()),
        ("word_count", pa.int64()),
        ("ingested_at", pa.string()),
        ("source_stream", pa.string()),
        ("source_shard", pa.string()),
        ("source_sequence_number", pa.string()),
    ]
)


class ParquetBatchWriter:
    def __init__(self, s3_client, bucket: str, prefix: str) -> None:
        self.s3 = s3_client
        self.bucket = bucket
        self.prefix = prefix.strip("/")

    def write(self, records: list[dict]) -> list[str]:
        if not records:
            return []

        partitions: dict[tuple[str, str], list[dict]] = defaultdict(list)
        for record in records:
            partitions[(record["ingested_at"][:10], record["ingested_at"][11:13])].append(
                record
            )

        keys = []
        for (date, hour), partition_records in sorted(partitions.items()):
            key = self._object_key(date, hour, partition_records)
            table = pa.Table.from_pylist(partition_records, schema=ARTICLE_SCHEMA)
            output = BytesIO()
            pq.write_table(table, output, compression="snappy")
            self.s3.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=output.getvalue(),
                ContentType="application/vnd.apache.parquet",
            )
            keys.append(key)
        return keys

    def _object_key(self, date: str, hour: str, records: Iterable[dict]) -> str:
        identities = [
            f'{record["source_shard"]}:{record["source_sequence_number"]}'
            for record in records
        ]
        digest = sha256("|".join(identities).encode("utf-8")).hexdigest()[:16]
        return (
            f"{self.prefix}/ingestion_date={date}/ingestion_hour={hour}/"
            f"batch-{digest}.parquet"
        )
