import logging.config
import structlog
from soundchain.utils.logging import setup_logging
from soundchain.settings import DEBUG
from contextlib import asynccontextmanager
from decouple import config

from fastapi import FastAPI
from aiokafka import AIOKafkaProducer

from soundchain.api.v1.ingestion import router as ingestion_router


logging.config.dictConfig(setup_logging(debug=DEBUG))

logger = structlog.get_logger()


KAFKA_BOOTSTRAP_SERVERS = config("REDPANDA_BROKERS", default="localhost:19092")

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("redpanda_connecting", servers=KAFKA_BOOTSTRAP_SERVERS)
    
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        acks=1 
    )
    
    try:
        await producer.start()
        logger.info("redpanda_connected")
        app.state.producer = producer
        yield
    except Exception as e:
        logger.critical("redpanda_connection_failed", error=str(e), exc_info=True)
        raise e
    finally:
        logger.info("redpanda_closing")
        await producer.stop()
        logger.info("redpanda_closed")

app = FastAPI(
    title="SoundChain Ingestion API",
    version="1.0.0",
    lifespan=lifespan
)

app.include_router(ingestion_router, prefix="/api/v1", tags=["Ingestion"])

@app.get("/health")
async def health_check():
    return {"status": "ok", "service": "ingestion"}