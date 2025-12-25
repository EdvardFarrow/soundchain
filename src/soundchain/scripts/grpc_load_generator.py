import asyncio
import random
import time
import uuid
from datetime import datetime, timezone
import grpc
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
from soundchain.protos import ingestion_pb2, ingestion_pb2_grpc


# Configuration
TOTAL_REQUESTS = 100000
CONCURRENCY = 1000
TARGET = "localhost:50051"

console = Console()

def pre_generate_pool(size: int = 5000) -> list:
    """
    Generates a pool of Protobuf requests to avoid CPU overhead during the test.
    """
    pool = []
    user_ids = [str(uuid.uuid4()) for _ in range(1000)]
    platforms = ["ios", "android", "web", "smart_speaker"]
    countries = ["US", "GB", "DE", "FR", "RU", "JP", "BR"]

    for _ in range(size):
        req = ingestion_pb2.ListenRequest(
            event_id=str(uuid.uuid4()),
            user_id=random.choice(user_ids),
            track_id=str(uuid.uuid4()),
            duration_played_ms=random.randint(30000, 300000),
            country=random.choice(countries),
            platform=random.choice(platforms),
            timestamp=datetime.now(timezone.utc).isoformat(),
        )
        pool.append(req)
    return pool

DATA_POOL = pre_generate_pool(5000)

async def worker(channel, requests_count: int, progress, task_id) -> int:
    """
    Sends gRPC requests in a loop and tracks failures.
    Returns the count of failed requests.
    """
    stub = ingestion_pb2_grpc.SoundChainIngestionStub(channel)
    pool_size = len(DATA_POOL)
    errors = 0

    for i in range(requests_count):
        request = DATA_POOL[i % pool_size]
        try:
            await stub.SendListen(request)
            progress.advance(task_id)
        except grpc.RpcError:
            errors += 1
            
    return errors

async def run_load_test():
    console.print(
        Panel(
            f"[bold green]STARTING gRPC LOAD TEST[/bold green]\n"
            f"Target: [cyan]{TARGET}[/cyan]\n"
            f"Total Requests: [bold]{TOTAL_REQUESTS:,}[/bold]\n"
            f"Concurrency: [bold]{CONCURRENCY}[/bold]",
            title="SoundChain Benchmark",
        )
    )

    async with grpc.aio.insecure_channel(TARGET) as channel:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            main_task = progress.add_task("[cyan]Sending streams...", total=TOTAL_REQUESTS)
            
            start_time = time.time()
            tasks = []
            reqs_per_worker = TOTAL_REQUESTS // CONCURRENCY

            for _ in range(CONCURRENCY):
                tasks.append(worker(channel, reqs_per_worker, progress, main_task))

            results = await asyncio.gather(*tasks)
            total_errors = sum(results)
            duration = time.time() - start_time

    rps = TOTAL_REQUESTS / duration
    success_rate = ((TOTAL_REQUESTS - total_errors) / TOTAL_REQUESTS) * 100

    table = Table(title="Benchmark Results", show_header=True, header_style="bold magenta")
    table.add_column("Metric", style="dim")
    table.add_column("Value", style="bold yellow")

    table.add_row("Total Requests", f"{TOTAL_REQUESTS:,}")
    table.add_row("Successful", f"[green]{TOTAL_REQUESTS - total_errors:,}[/green]")
    table.add_row("Failed", f"[red]{total_errors:,}[/red]")
    table.add_row("Duration", f"{duration:.2f} s")
    table.add_row("Concurrency", str(CONCURRENCY))
    table.add_row("Success Rate", f"{success_rate:.2f}%")
    table.add_row("RPS (Requests/Sec)", f"{rps:,.2f}")

    console.print("\n")
    console.print(table)

if __name__ == "__main__":
    try:
        asyncio.run(run_load_test())
    except KeyboardInterrupt:
        console.print("[red]Test cancelled[/red]")