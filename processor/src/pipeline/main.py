import logging
import os

import boto3

from pipeline.checkpoint import FileCheckpointStore
from pipeline.config import Settings
from pipeline.consumer import KinesisToS3Pipeline, install_signal_handlers
from pipeline.storage import ParquetBatchWriter


def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    settings = Settings.from_env()
    kinesis_client = boto3.client("kinesis", **settings.boto_config())
    s3_client = boto3.client("s3", **settings.boto_config())

    pipeline = KinesisToS3Pipeline(
        settings=settings,
        kinesis_client=kinesis_client,
        writer=ParquetBatchWriter(
            s3_client,
            settings.output_bucket,
            settings.output_prefix,
        ),
        checkpoint_store=FileCheckpointStore(settings.checkpoint_path),
    )
    install_signal_handlers(pipeline)
    pipeline.run()


if __name__ == "__main__":
    main()

