import asyncio
import random
import time
import uuid
from datetime import datetime, timezone
import httpx
from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from rich.table import Table


# Configuration
TARGET_URL = "http://localhost:8000/api/v1/listen"
RPS_TARGET = 1000
CONCURRENCY = 50
DURATION_SEC = 30

console = Console()

def pre_generate_pool(size: int = 5000) -> list:
    """
    Generates a pool of payloads to reduce CPU overhead during the test.
    """
    pool = []
    users = [str(uuid.uuid4()) for _ in range(1000)]
    tracks = [str(uuid.uuid4()) for _ in range(5000)]
    countries = ["US", "GE", "FR", "DE", "JP", "BR"]
    platforms = ["mobile", "web", "desktop", "smart_speaker"]

    for _ in range(size):
        payload = {
            "event_id": str(uuid.uuid4()),
            "user_id": random.choice(users),
            "track_id": random.choice(tracks),
            "duration_played_ms": random.randint(1000, 300000),
            "country": random.choice(countries),
            "platform": random.choice(platforms),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        pool.append(payload)
    return pool


DATA_POOL = pre_generate_pool(10000)

async def send_request(client: httpx.AsyncClient) -> bool:
    payload = random.choice(DATA_POOL)
    
    payload["timestamp"] = datetime.now(timezone.utc).isoformat()
    
    try:
        resp = await client.post(TARGET_URL, json=payload)
        resp.raise_for_status()
        return True
    except Exception:
        return False


async def worker(queue: asyncio.Queue, stats: dict, progress, task_id):
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
            
            progress.advance(task_id)
            queue.task_done()


async def main():
    total_expected_requests = RPS_TARGET * DURATION_SEC
    
    console.print(
        Panel(
            f"[bold green]STARTING HTTP LOAD TEST[/bold green]\n"
            f"Target: [cyan]{TARGET_URL}[/cyan]\n"
            f"Target RPS: [bold]{RPS_TARGET}[/bold]\n"
            f"Concurrency: [bold]{CONCURRENCY}[/bold]\n"
            f"Duration: [bold]{DURATION_SEC}s[/bold]",
            title="SoundChain REST Benchmark",
        )
    )
    
    queue = asyncio.Queue()
    stats = {'total': 0, 'success': 0, 'fail': 0}
    
    start_time = time.time()
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        console=console,
    ) as progress:
        
        main_task = progress.add_task("[cyan]Sending requests...", total=total_expected_requests)
        
        workers = [
            asyncio.create_task(worker(queue, stats, progress, main_task)) 
            for _ in range(CONCURRENCY)
        ]
        
        try:
            while time.time() - start_time < DURATION_SEC:
                loop_start = time.perf_counter()
                
                
                batch_size = int(RPS_TARGET / 10)
                for _ in range(batch_size):
                    queue.put_nowait(True)
                
                elapsed = time.perf_counter() - loop_start
                sleep_time = max(0, 0.1 - elapsed)
                await asyncio.sleep(sleep_time)

        except KeyboardInterrupt:
            console.print("[red]Test cancelled[/red]")
    
    total_time = time.time() - start_time
    
    for w in workers:
        w.cancel()

    rps = stats['success'] / total_time
    success_rate = 0
    if stats['total'] > 0:
        success_rate = (stats['success'] / stats['total']) * 100
    
    table = Table(title="Benchmark Results", show_header=True, header_style="bold magenta")
    table.add_column("Metric", style="dim")
    table.add_column("Value", style="bold yellow")
    
    table.add_row("Total Requests", f"{stats['total']:,}")
    table.add_row("Successful", f"[green]{stats['success']:,}[/green]")
    table.add_row("Failed", f"[red]{stats['fail']:,}[/red]")
    table.add_row("Duration", f"{total_time:.2f} s")
    table.add_row("Success Rate", f"{success_rate:.2f}%")
    table.add_row("Actual RPS", f"{rps:,.2f}")
    
    console.print("\n")
    console.print(table)

if __name__ == "__main__":
    asyncio.run(main())