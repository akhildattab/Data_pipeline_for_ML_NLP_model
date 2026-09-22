from dataclasses import dataclass
import os


def _positive_int(name: str, default: int) -> int:
    value = int(os.getenv(name, str(default)))
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return value


def _positive_float(name: str, default: float) -> float:
    value = float(os.getenv(name, str(default)))
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return value


@dataclass(frozen=True)
class Settings:
    endpoint_url: str
    region: str
    access_key: str
    secret_key: str
    stream_name: str
    output_bucket: str
    output_prefix: str
    batch_size: int
    flush_interval_seconds: float
    poll_interval_seconds: float
    starting_position: str
    checkpoint_path: str
    max_records_per_read: int

    @classmethod
    def from_env(cls) -> "Settings":
        starting_position = os.getenv("STARTING_POSITION", "TRIM_HORIZON").upper()
        if starting_position not in {"TRIM_HORIZON", "LATEST"}:
            raise ValueError("STARTING_POSITION must be TRIM_HORIZON or LATEST")

        return cls(
            endpoint_url=os.getenv("AWS_ENDPOINT_URL", "http://localstack:4566"),
            region=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
            access_key=os.getenv("AWS_ACCESS_KEY_ID", "test"),
            secret_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
            stream_name=os.getenv("STREAM_NAME", "MyStream"),
            output_bucket=os.getenv("OUTPUT_BUCKET", "my-bucket"),
            output_prefix=os.getenv("OUTPUT_PREFIX", "enriched_articles").strip("/"),
            batch_size=_positive_int("BATCH_SIZE", 250),
            flush_interval_seconds=_positive_float("FLUSH_INTERVAL_SECONDS", 5),
            poll_interval_seconds=_positive_float("POLL_INTERVAL_SECONDS", 0.5),
            starting_position=starting_position,
            checkpoint_path=os.getenv("CHECKPOINT_PATH", "/state/checkpoints.json"),
            max_records_per_read=_positive_int("MAX_RECORDS_PER_READ", 500),
        )

    def boto_config(self) -> dict:
        return {
            "endpoint_url": self.endpoint_url,
            "region_name": self.region,
            "aws_access_key_id": self.access_key,
            "aws_secret_access_key": self.secret_key,
        }

