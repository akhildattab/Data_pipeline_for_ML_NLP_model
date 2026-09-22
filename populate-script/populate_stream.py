import json
import logging
import os
import time

import boto3
from botocore.exceptions import ClientError, EndpointConnectionError
from faker import Faker


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("article-publisher")

fake = Faker()

AWS_CONFIG = {
    "endpoint_url": os.getenv("AWS_ENDPOINT_URL", "http://localstack:4566"),
    "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID", "test"),
    "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
    "region_name": os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
}


def wait_for_aws(timeout_seconds: int = 60) -> None:
    """Wait until LocalStack is ready to accept AWS API requests."""
    client = boto3.client("s3", **AWS_CONFIG)
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        try:
            client.list_buckets()
            return
        except (ClientError, EndpointConnectionError):
            time.sleep(1)
    raise TimeoutError("AWS endpoint did not become ready in time")


def create_s3_bucket(bucket_name: str) -> None:
    client = boto3.client("s3", **AWS_CONFIG)
    existing = {bucket["Name"] for bucket in client.list_buckets().get("Buckets", [])}
    if bucket_name not in existing:
        client.create_bucket(Bucket=bucket_name)
        logger.info("Created bucket %s", bucket_name)


def create_kinesis_stream(stream_name: str, shard_count: int = 1) -> None:
    client = boto3.client("kinesis", **AWS_CONFIG)
    try:
        client.create_stream(StreamName=stream_name, ShardCount=shard_count)
        logger.info("Created stream %s", stream_name)
    except client.exceptions.ResourceInUseException:
        logger.info("Stream %s already exists", stream_name)

    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        status = client.describe_stream_summary(StreamName=stream_name)[
            "StreamDescriptionSummary"
        ]["StreamStatus"]
        if status == "ACTIVE":
            return
        time.sleep(1)
    raise TimeoutError(f"Stream {stream_name} did not become ACTIVE in time")


def generate_mock_article() -> dict:
    return {
        "article_id": fake.uuid4(),
        "title": fake.sentence(nb_words=6),
        "author": fake.name(),
        "publish_date": fake.date_time_this_year().isoformat(),
        "content": " ".join(fake.paragraphs(nb=10)),
    }


def publish_articles_to_kinesis(stream_name: str, target_size_mb: int) -> int:
    client = boto3.client("kinesis", **AWS_CONFIG)
    target_bytes = target_size_mb * 1024 * 1024
    total_size = 0
    articles_count = 0

    while total_size < target_bytes:
        article = generate_mock_article()
        payload = json.dumps(article).encode("utf-8")
        client.put_record(
            StreamName=stream_name,
            Data=payload,
            PartitionKey=article["article_id"],
        )
        total_size += len(payload)
        articles_count += 1

        if articles_count % 250 == 0:
            logger.info("Published %s articles", articles_count)

    logger.info(
        "Published %s articles (%.2f MiB)",
        articles_count,
        total_size / (1024 * 1024),
    )
    return articles_count


def main() -> None:
    bucket_name = os.getenv("OUTPUT_BUCKET", "my-bucket")
    stream_name = os.getenv("STREAM_NAME", "MyStream")
    dataset_size_mb = int(os.getenv("DATASET_SIZE_MB", "1"))
    num_iterations = int(os.getenv("NUM_ITERATIONS", "1"))
    publish_interval = float(os.getenv("PUBLISH_INTERVAL_SECONDS", "1"))

    wait_for_aws()
    create_s3_bucket(bucket_name)
    create_kinesis_stream(stream_name)

    total_articles = 0
    for iteration in range(num_iterations):
        logger.info("Starting publish iteration %s/%s", iteration + 1, num_iterations)
        total_articles += publish_articles_to_kinesis(stream_name, dataset_size_mb)
        if iteration < num_iterations - 1:
            time.sleep(publish_interval)

    logger.info("Publisher finished after sending %s articles", total_articles)


if __name__ == "__main__":
    main()
