import asyncio
import grpc
import orjson  
import structlog
from aiokafka import AIOKafkaProducer
from decouple import AutoConfig
from soundchain.protos import ingestion_pb2, ingestion_pb2_grpc
from soundchain.utils.logging import setup_logging
import logging.config


config = AutoConfig(search_path=".")


logging.config.dictConfig(setup_logging(debug=True))
logger = structlog.get_logger("grpc_server")


class IngestionService(ingestion_pb2_grpc.SoundChainIngestionServicer):
    def __init__(self):
        self.producer = None

    async def start(self):
        bootstrap_servers = config("KAFKA_BOOTSTRAP_SERVERS", default="localhost:9092")
        
        self.producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            linger_ms=5,  
            max_batch_size=32768,
            compression_type="gzip" 
        )
        await self.producer.start()
        logger.info("kafka_producer_connected", servers=bootstrap_servers)

    async def stop(self):
        if self.producer:
            await self.producer.stop()
            logger.info("kafka_producer_closed")

    async def SendListen(self, request, context):
        try:
            payload = {
                "event_id": request.event_id,
                "user_id": request.user_id,
                "track_id": request.track_id,
                "duration_played_ms": request.duration_played_ms,
                "country": request.country,
                "platform": request.platform,
                "timestamp": request.timestamp,
            }
            
            value_bytes = orjson.dumps(payload)

            await self.producer.send("raw-listens", value_bytes)
            
            return ingestion_pb2.ListenResponse(success=True, message="OK")
        
        except Exception as e:
            logger.error("grpc_processing_error", error=str(e))
            return ingestion_pb2.ListenResponse(success=False, message=str(e))

async def serve():
    server = grpc.aio.server()
    service_impl = IngestionService()
    
    ingestion_pb2_grpc.add_SoundChainIngestionServicer_to_server(service_impl, server)
    listen_addr = "[::]:50051"
    server.add_insecure_port(listen_addr)

    await service_impl.start()  
    logger.info("grpc_server_started", address=listen_addr)
    await server.start()        

    try:
        await server.wait_for_termination()
    except asyncio.CancelledError:
        pass
    finally:
        await service_impl.stop()
        await server.stop(grace=0)
        logger.info("grpc_server_stopped")

if __name__ == "__main__":
    try:
        asyncio.run(serve())
    except KeyboardInterrupt:
        pass