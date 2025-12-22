import clickhouse_connect
import structlog
import json     # <--- Добавили
import random   # <--- Добавили
from decimal import Decimal
from decouple import config

# --- Django Setup ---
import os, django, sys
sys.path.append(os.getcwd()) 
sys.path.append(os.path.join(os.getcwd(), "src"))

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "soundchain.settings")
django.setup()

from soundchain.django_apps.ledger.models import Account
from soundchain.domains.ledger.service import LedgerService, TransactionDTO
from soundchain.utils.logging import setup_logging
from soundchain.settings import DEBUG
import logging.config


logging.config.dictConfig(setup_logging(debug=DEBUG))
logger = structlog.get_logger("payout_service")


ROYALTY_RATE = Decimal("0.0030")
PLATFORM_ACCOUNT_ID = "00000000-0000-0000-0000-000000000000"

def spot_check(data, count=5):
    """
    Prints random items from a list for visual inspection
    """
    if not data:
        print("The data list is empty!")
        return

    safe_count = min(len(data), count)
    samples = random.sample(data, safe_count)

    print(f"\nSPOT CHECK (random: {safe_count} из {len(data)})")

    for i, item in enumerate(samples, 1):
        print(f"record #{i}:")
        print(json.dumps(item, indent=4, ensure_ascii=False, default=str))
        print("-" * 50)
    print("\n")

def run_payout():
    logger.info("payout_started")
    
    try:
        ch_client = clickhouse_connect.get_client(
            host=config("CLICKHOUSE_HOST"),
            port=config("CLICKHOUSE_PORT", cast=int),
            username=config("CLICKHOUSE_USER"),
            password=config("CLICKHOUSE_PASSWORD"),
            database=config("CLICKHOUSE_DB")
        )
    except Exception as e:
        logger.critical("clickhouse_connection_failed", error=str(e))
        return

    query = """
    SELECT 
        track_id,
        count() as streams
    FROM listens
    GROUP BY track_id
    HAVING streams > 0
    """
    
    logger.info("executing_analytics_query")
    result = ch_client.query(query)
    rows = result.result_rows 
    
    spot_check(rows) 
    
    if not rows:
        logger.info("no_streams_found_to_pay")
        return

    logger.info("analytics_ready", tracks_count=len(rows))

    ledger_service = LedgerService()
    transactions = []
    
    Account.objects.get_or_create(
        id=PLATFORM_ACCOUNT_ID, 
        defaults={"name": "Spotify Platform Treasury"}
    )

    for track_id, streams in rows:
        amount = Decimal(streams) * ROYALTY_RATE
        artist_account_id = str(track_id)
        
        Account.objects.get_or_create(
            id=artist_account_id, 
            defaults={"name": f"Artist Account {artist_account_id[:8]}..."}
        )

        tx = TransactionDTO(
            debit_account_id=PLATFORM_ACCOUNT_ID,
            credit_account_id=artist_account_id,
            amount=amount,
            reference_id=f"payout_v1_{track_id}", 
            description=f"Royalty for {streams} streams"
        )
        transactions.append(tx)

    logger.info("processing_ledger_batch", count=len(transactions))
    
    try:
        ledger_service.process_batch(transactions)
        logger.info("payout_completed_successfully", total_txs=len(transactions))
    except Exception as e:
        logger.critical("payout_failed", error=str(e))

if __name__ == "__main__":
    run_payout()