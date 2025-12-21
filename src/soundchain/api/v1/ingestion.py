import orjson
import structlog
from fastapi import APIRouter, status, HTTPException
from starlette.requests import Request
from soundchain.api.schemas import ListenEventSchema


router = APIRouter()
logger = structlog.get_logger()

@router.post("/listen", status_code=status.HTTP_202_ACCEPTED)
async def ingest_listen_event(event: ListenEventSchema, request: Request):
    log = logger.bind(event_id=event.event_id, user_id=event.user_id)
    
    producer = getattr(request.app.state, "producer", None)
    
    if not producer:
        log.error("producer_unavailable")
        raise HTTPException(status_code=500, detail="Service Unavailable")

    try:
        payload: bytes = orjson.dumps(event.model_dump(mode='json'))
    except Exception as e:
        log.error("serialization_failed", error=str(e))
        raise HTTPException(status_code=400, detail="Invalid data format")

    try:
        await producer.send_and_wait(
            topic="raw-listens", 
            value=payload, 
            key=str(event.user_id).encode()
        )
        log.info("event_sent_to_redpanda")
        
    except Exception as e:
        log.error("redpanda_send_failed", error=str(e), exc_info=True)
        raise HTTPException(status_code=500, detail="Event bus error")

    return {"status": "queued", "event_id": event.event_id}