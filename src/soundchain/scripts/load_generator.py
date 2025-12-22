import asyncio
import httpx
import random
import uuid
import time
from datetime import datetime, timezone

TARGET_URL = "http://localhost:8000/api/v1/listen"
RPS_TARGET = 1000   
CONCURRENCY = 50
DURATION_SEC = 60

USERS = [str(uuid.uuid4()) for _ in range(5000)] 
TRACKS = [str(uuid.uuid4()) for _ in range(10000)]
COUNTRIES = ["US", "GE", "FR", "DE", "JP", "BR"]
PLATFORMS = ["mobile", "web", "desktop", "smart_speaker"]

async def send_request(client: httpx.AsyncClient):
    payload = {
        "event_id": str(uuid.uuid4()), 
        "user_id": random.choice(USERS),
        "track_id": random.choice(TRACKS),
        "duration_played_ms": random.randint(1000, 300000),
        "country": random.choice(COUNTRIES),
        "platform": random.choice(PLATFORMS),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    
    try:
        resp = await client.post(TARGET_URL, json=payload)
        resp.raise_for_status()
        return True
    except Exception:
        return False

async def worker(queue: asyncio.Queue, stats: dict):
    limits = httpx.Limits(max_keepalive_connections=CONCURRENCY, max_connections=CONCURRENCY)
    async with httpx.AsyncClient(limits=limits, timeout=5.0) as client:
        while True:
            _ = await queue.get()
            success = await send_request(client)
            
            stats['total'] += 1
            if success:
                stats['success'] += 1
            else:
                stats['fail'] += 1
            
            queue.task_done()

async def main():
    print(f"Starting load test: Target={RPS_TARGET} RPS | Users={len(USERS)} | Workers={CONCURRENCY}")
    print(f"Duration: {DURATION_SEC}s")
    print("-" * 60)
    
    queue = asyncio.Queue()
    stats = {'total': 0, 'success': 0, 'fail': 0}
    
    workers = [asyncio.create_task(worker(queue, stats)) for _ in range(CONCURRENCY)]
    
    start_time = time.time()
    
    try:
        while time.time() - start_time < DURATION_SEC:
            loop_start = time.perf_counter()
            
            batch_size = int(RPS_TARGET / 10)
            for _ in range(batch_size):
                queue.put_nowait(True)
            
            elapsed = time.perf_counter() - loop_start
            sleep_time = max(0, 0.1 - elapsed)
            await asyncio.sleep(sleep_time)
            
            elapsed_total = time.time() - start_time
            if elapsed_total > 0:
                current_rps = stats['success'] / elapsed_total
                print(f"⚡ Status: {stats['success']} OK | {stats['fail']} Fail | Avg RPS: {current_rps:.0f}", end='\r')

    except KeyboardInterrupt:
        print("\nStopping...")
    
    total_time = time.time() - start_time
    print(f"\n{'-' * 60}")
    print(f"✅ DONE in {total_time:.2f}s")
    print(f"📊 Total Requests: {stats['total']}")
    print(f"🟢 Success: {stats['success']}")
    print(f"🔴 Failed:  {stats['fail']}")
    print(f"🚀 Real Average RPS: {stats['success'] / total_time:.2f}")
    
    for w in workers:
        w.cancel()

if __name__ == "__main__":
    asyncio.run(main())