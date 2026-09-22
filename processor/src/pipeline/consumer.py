import logging
import signal
import time

from botocore.exceptions import ClientError

from pipeline.checkpoint import FileCheckpointStore
from pipeline.config import Settings
from pipeline.storage import ParquetBatchWriter
from pipeline.transform import InvalidRecord, RunningAverage, enrich_record


logger = logging.getLogger(__name__)


class KinesisToS3Pipeline:
    def __init__(
        self,
        *,
        settings: Settings,
        kinesis_client,
        writer: ParquetBatchWriter,
        checkpoint_store: FileCheckpointStore,
    ) -> None:
        self.settings = settings
        self.kinesis = kinesis_client
        self.writer = writer
        self.checkpoint_store = checkpoint_store
        self.checkpoints = checkpoint_store.load()
        self.pending_checkpoints: dict[str, str] = {}
        self.batch: list[dict] = []
        self.average = RunningAverage()
        self.records_read = 0
        self.invalid_records = 0
        self.batches_written = 0
        self.last_flush = time.monotonic()
        self._stop_requested = False

    def request_stop(self, *_args) -> None:
        logger.info("Shutdown requested; finishing the current batch")
        self._stop_requested = True

    def run(self) -> None:
        self._wait_for_stream()
        iterators = self._create_shard_iterators()
        logger.info(
            "Reading stream=%s shards=%s batch_size=%s",
            self.settings.stream_name,
            len(iterators),
            self.settings.batch_size,
        )

        try:
            while not self._stop_requested:
                found_records = False
                for shard_id in list(iterators):
                    response = self.kinesis.get_records(
                        ShardIterator=iterators[shard_id],
                        Limit=self.settings.max_records_per_read,
                    )
                    iterators[shard_id] = response["NextShardIterator"]
                    records = response.get("Records", [])
                    found_records = found_records or bool(records)

                    for record in records:
                        self._process_record(shard_id, record)
                        if len(self.batch) >= self.settings.batch_size:
                            self._flush()

                if time.monotonic() - self.last_flush >= self.settings.flush_interval_seconds:
                    self._flush()
                if not found_records:
                    time.sleep(self.settings.poll_interval_seconds)
        finally:
            self._flush()
            logger.info(
                "Pipeline stopped records_read=%s valid=%s invalid=%s "
                "batches=%s average_word_count=%.2f",
                self.records_read,
                self.average.count,
                self.invalid_records,
                self.batches_written,
                self.average.value,
            )

    def _process_record(self, shard_id: str, record: dict) -> None:
        sequence_number = record["SequenceNumber"]
        self.records_read += 1
        self.pending_checkpoints[shard_id] = sequence_number
        try:
            enriched = enrich_record(
                record["Data"],
                stream_name=self.settings.stream_name,
                shard_id=shard_id,
                sequence_number=sequence_number,
            )
        except InvalidRecord as exc:
            self.invalid_records += 1
            logger.warning(
                "Skipping invalid record shard=%s sequence=%s reason=%s",
                shard_id,
                sequence_number,
                exc,
            )
            return

        self.batch.append(enriched)
        self.average.add(enriched["word_count"])

    def _flush(self) -> None:
        if not self.batch and not self.pending_checkpoints:
            self.last_flush = time.monotonic()
            return

        records_to_write = self.batch
        checkpoints_to_commit = self.pending_checkpoints.copy()

        if records_to_write:
            keys = self.writer.write(records_to_write)
            self.batches_written += len(keys)

        self.checkpoints.update(checkpoints_to_commit)
        self.checkpoint_store.save(self.checkpoints)
        self.batch = []
        self.pending_checkpoints.clear()
        self.last_flush = time.monotonic()

        logger.info(
            "Committed records=%s total_valid=%s average_word_count=%.2f",
            len(records_to_write),
            self.average.count,
            self.average.value,
        )

    def _wait_for_stream(self, timeout_seconds: int = 120) -> None:
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline and not self._stop_requested:
            try:
                summary = self.kinesis.describe_stream_summary(
                    StreamName=self.settings.stream_name
                )["StreamDescriptionSummary"]
                if summary["StreamStatus"] == "ACTIVE":
                    return
            except self.kinesis.exceptions.ResourceNotFoundException:
                pass
            except ClientError as exc:
                logger.warning("Waiting for stream: %s", exc.response["Error"]["Code"])
            time.sleep(1)
        raise TimeoutError(f"Stream {self.settings.stream_name} did not become ACTIVE")

    def _create_shard_iterators(self) -> dict[str, str]:
        shards = self.kinesis.list_shards(StreamName=self.settings.stream_name)["Shards"]
        if not shards:
            raise RuntimeError(f"Stream {self.settings.stream_name} has no shards")

        iterators = {}
        for shard in shards:
            shard_id = shard["ShardId"]
            request = {
                "StreamName": self.settings.stream_name,
                "ShardId": shard_id,
            }
            if shard_id in self.checkpoints:
                request.update(
                    ShardIteratorType="AFTER_SEQUENCE_NUMBER",
                    StartingSequenceNumber=self.checkpoints[shard_id],
                )
            else:
                request["ShardIteratorType"] = self.settings.starting_position

            iterators[shard_id] = self.kinesis.get_shard_iterator(**request)[
                "ShardIterator"
            ]
        return iterators


def install_signal_handlers(pipeline: KinesisToS3Pipeline) -> None:
    signal.signal(signal.SIGTERM, pipeline.request_stop)
    signal.signal(signal.SIGINT, pipeline.request_stop)

