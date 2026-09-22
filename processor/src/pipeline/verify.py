import os
import sys
import time
from io import BytesIO

import boto3
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

from pipeline.config import Settings
from pipeline.transform import count_words


REQUIRED_COLUMNS = {
    "article_id",
    "content",
    "word_count",
    "ingested_at",
    "source_stream",
    "source_shard",
    "source_sequence_number",
}


def parquet_keys(s3_client, bucket: str, prefix: str) -> list[str]:
    paginator = s3_client.get_paginator("list_objects_v2")
    keys = []
    try:
        for page in paginator.paginate(Bucket=bucket, Prefix=f"{prefix.strip('/')}/"):
            keys.extend(
                item["Key"]
                for item in page.get("Contents", [])
                if item["Key"].endswith(".parquet")
            )
    except ClientError as exc:
        if exc.response["Error"]["Code"] in {"NoSuchBucket", "404"}:
            return []
        raise
    return sorted(keys)


def main() -> None:
    settings = Settings.from_env()
    timeout = int(os.getenv("VERIFY_TIMEOUT_SECONDS", "90"))
    settle_seconds = int(os.getenv("VERIFY_SETTLE_SECONDS", "8"))
    s3_client = boto3.client("s3", **settings.boto_config())

    deadline = time.monotonic() + timeout
    keys: list[str] = []
    last_change = time.monotonic()
    while time.monotonic() < deadline:
        current_keys = parquet_keys(
            s3_client,
            settings.output_bucket,
            settings.output_prefix,
        )
        if current_keys != keys:
            keys = current_keys
            last_change = time.monotonic()
        if keys and time.monotonic() - last_change >= settle_seconds:
            break
        print("Waiting for enriched Parquet output to settle...")
        time.sleep(2)

    if not keys:
        print("Verification failed: no Parquet files were written", file=sys.stderr)
        raise SystemExit(1)

    row_count = 0
    total_words = 0
    seen_sources: set[tuple[str, str]] = set()
    for key in keys:
        body = s3_client.get_object(Bucket=settings.output_bucket, Key=key)["Body"].read()
        table = pq.ParquetFile(BytesIO(body)).read()
        missing_columns = REQUIRED_COLUMNS - set(table.column_names)
        if missing_columns:
            raise AssertionError(f"{key} is missing columns: {sorted(missing_columns)}")

        for record in table.to_pylist():
            expected_word_count = count_words(record["content"])
            if record["word_count"] != expected_word_count:
                raise AssertionError(
                    f"word_count mismatch for article {record['article_id']}"
                )
            source = (record["source_shard"], record["source_sequence_number"])
            if source in seen_sources:
                raise AssertionError(f"duplicate source record detected: {source}")
            seen_sources.add(source)
            row_count += 1
            total_words += record["word_count"]

    average = total_words / row_count
    print(
        f"Verification passed: {row_count} enriched rows across {len(keys)} "
        f"Parquet files; average word count={average:.2f}"
    )


if __name__ == "__main__":
    main()
