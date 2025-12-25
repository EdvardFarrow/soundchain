import asyncio
import orjson
import structlog
import clickhouse_connect
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from decouple import config
from soundchain.utils.logging import setup_logging
from soundchain.settings import DEBUG
import logging.config
from prometheus_client import start_http_server, Counter


logging.config.dictConfig(setup_logging(debug=DEBUG))
logger = structlog.get_logger("etl_worker")


# Config
BATCH_SIZE = 5000
FLUSH_INTERVAL_MS = 1000 # 1 sec
MAX_WORKERS = 1


# Retry Settings
MAX_RETRIES = 5
INITIAL_BACKOFF = 1.0
MULTIPLIER_BACKOFF = 2.0
DLQ_TOPIC = "raw-listens-dlq"


# Counters
PROCESSED_MESSAGES = Counter('soundchain_processed_messages_total', 'Total messages successfully inserted into ClickHouse')
PROCESSING_ERRORS = Counter('soundchain_processing_errors_total', 'Total errors during parsing or insertion')
DLQ_MESSAGES = Counter('soundchain_dlq_messages_total', 'Total messages sent to DLQ')


class ClickHouseLoader:
    def __init__(self):
        self.client = clickhouse_connect.get_client(
            host=config("CLICKHOUSE_HOST"),
            port=config("CLICKHOUSE_PORT", cast=int),
            username=config("CLICKHOUSE_USER"),
            password=config("CLICKHOUSE_PASSWORD"),
            database=config("CLICKHOUSE_DB"),
            settings={'async_insert': 1, 'wait_end_of_query': 1} 
        )
        # Thread pool to avoid blocking the Event Loop
        self.executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)

    def _insert_sync(self, batch_data: list):
        if not batch_data:
            return

        data_to_insert = []
        columns = ["event_id", "user_id", "track_id", "timestamp", 
                    "duration_played_ms", "country", "platform"]
        
        try:
            for row in batch_data:
                ts = datetime.fromisoformat(row['timestamp'].replace('Z', '+00:00'))
                data_to_insert.append([
                    row['event_id'], row['user_id'], row['track_id'], ts,
                    row['duration_played_ms'], row['country'], row['platform']
                ])
            
            self.client.insert("listens", data_to_insert, column_names=columns)
            
        except Exception as e:
            logger.error("db_insert_error", error=str(e))
            raise e

    async def insert_batch_async(self, batch_data: list):
        """Async wrapper: runs synchronous code in the ThreadPool"""
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(self.executor, self._insert_sync, batch_data)


async def send_to_dlq(producer: AIOKafkaProducer, batch: list, error_reason: str):
    """
    Sending the "broken" batch to the Dead Letter Queue.
    Wrapping the data in an envelope with the cause of the error.
    """
    if not producer:
        logger.critical("dlq_producer_missing")
        return

    try:
        dlq_payload = {
            "error": error_reason,
            "failed_at": datetime.now(timezone.utc).isoformat(),
            "original_batch": batch 
        }
        
        payload_bytes = orjson.dumps(dlq_payload)
        
        await producer.send_and_wait(DLQ_TOPIC, payload_bytes)
        logger.warning("sent_to_dlq", count=len(batch), reason=error_reason)
        
    except Exception as e:
        logger.critical("dlq_send_failed", error=str(e))
        raise e


async def consume():
    start_http_server(8000) 
    logger.info("metrics_server_started_port_8000")
    
    consumer = AIOKafkaConsumer(
        "raw-listens",
        bootstrap_servers=config("REDPANDA_BROKERS"),
        group_id="analytics_group_v1",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        fetch_min_bytes=1024 * 1024, # 1 MB
        fetch_max_wait_ms=500
    )
    
    producer = AIOKafkaProducer(
        bootstrap_servers=config("REDPANDA_BROKERS")
    )
    
    loader = ClickHouseLoader()
    
    await consumer.start()
    await producer.start()
    
    logger.info("worker_started", topic="raw-listens", dlq=DLQ_TOPIC)

    try:
        while True:
            result = await consumer.getmany(timeout_ms=FLUSH_INTERVAL_MS, max_records=BATCH_SIZE)

            raw_batch = []
            for tp, messages in result.items():
                raw_batch.extend(messages)

            if not raw_batch:
                continue

            clean_batch = []
            for msg in raw_batch:
                try:
                    clean_batch.append(orjson.loads(msg.value))
                except Exception as e:
                    logger.error("parse_failed", offset=msg.offset, error=str(e))
                    PROCESSING_ERRORS.inc() 
                    continue
            
            if not clean_batch:
                await consumer.commit()
                continue

            success = False
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    await loader.insert_batch_async(clean_batch)
                    success = True
                    break 
                
                except Exception as e:
                    logger.error("db_insert_failed", attempt=attempt, error=str(e))
                    PROCESSING_ERRORS.inc() 
                    
                    if attempt < MAX_RETRIES:
                        sleep_time = INITIAL_BACKOFF * (MULTIPLIER_BACKOFF ** (attempt - 1))
                        logger.info("retrying", sleep_s=sleep_time)
                        await asyncio.sleep(sleep_time)
            
            if success:
                await consumer.commit()
                PROCESSED_MESSAGES.inc(len(clean_batch)) 
                logger.info("batch_flushed", count=len(clean_batch))
            else:
                logger.error("all_retries_exhausted", action="sending_to_dlq")
                
                DLQ_MESSAGES.inc(len(clean_batch))
                
                await send_to_dlq(producer, clean_batch, error_reason="ClickHouse insert failed after retries")
                await consumer.commit()

    except asyncio.CancelledError:
        logger.info("worker_shutting_down")
    finally:
        await consumer.stop()
        await producer.stop()
        logger.info("worker_stopped")

if __name__ == "__main__":
    try:
        asyncio.run(consume())
    except KeyboardInterrupt:
        pass