import os
import asyncio
import grpc
import orjson  
import structlog
from decouple import AutoConfig
from google.cloud import pubsub_v1
from soundchain.protos import ingestion_pb2, ingestion_pb2_grpc
from soundchain.utils.logging import setup_logging
import logging.config

config = AutoConfig(search_path=".")

logging.config.dictConfig(setup_logging(debug=True))
logger = structlog.get_logger("grpc_server")

class IngestionService(ingestion_pb2_grpc.SoundChainIngestionServicer):
    def __init__(self):
        self.publisher = None
        self.topic_path = None

    async def start(self):
        project_id = config("GCP_PROJECT_ID")
        topic_id = config("PUBSUB_TOPIC_ID", default="raw-listens")
        
        batch_settings = pubsub_v1.types.BatchSettings(
            max_bytes=131072,  # 128 KB
            max_latency=0.05,  # 50 ms
        )
        
        self.publisher = pubsub_v1.PublisherClient(batch_settings=batch_settings)
        self.topic_path = self.publisher.topic_path(project_id, topic_id)
        
        logger.info("pubsub_publisher_connected", topic=self.topic_path)

    async def stop(self):
        logger.info("pubsub_publisher_closed")

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

            future = self.publisher.publish(self.topic_path, value_bytes)
            await asyncio.wrap_future(future)
            
            return ingestion_pb2.ListenResponse(success=True, message="OK")
        
        except Exception as e:
            logger.error("grpc_processing_error", error=str(e))
            return ingestion_pb2.ListenResponse(success=False, message=str(e))

async def serve():
    server = grpc.aio.server()
    service_impl = IngestionService()
    
    ingestion_pb2_grpc.add_SoundChainIngestionServicer_to_server(service_impl, server)
    
    port = os.environ.get("PORT", "50051")
    listen_addr = f"[::]:{port}"
    
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