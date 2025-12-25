import sys
import os
import structlog
import random
import orjson
from datetime import timedelta, date
from decimal import Decimal
from decouple import config
import clickhouse_connect
import logging.config

sys.path.append(os.getcwd())
sys.path.append(os.path.join(os.getcwd(), "src"))

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "soundchain.settings")

import django
django.setup()

from soundchain.django_apps.ledger.models import Account
from soundchain.domains.ledger.service import LedgerService, TransactionDTO
from soundchain.utils.logging import setup_logging
from soundchain.settings import DEBUG

logging.config.dictConfig(setup_logging(debug=DEBUG))
logger = structlog.get_logger("payout_service")


ROYALTY_RATE = Decimal("0.0030")
PLATFORM_ACCOUNT_ID = "00000000-0000-0000-0000-000000000000"

def spot_check(data: list, count: int = 5):
    if not data:
        print("The data list is empty!")
        return

    safe_count = min(len(data), count)
    samples = random.sample(data, safe_count)

    print(f"\nSPOT CHECK (random: {safe_count} из {len(data)})")

    for i, item in enumerate(samples, 1):
        print(f"record #{i}:")
        print(
            orjson.dumps(
                item, 
                option=orjson.OPT_INDENT_2 | orjson.OPT_NAIVE_UTC
            ).decode('utf-8')
        )
        print("-" * 50)
    print("\n")

def run_payout():
    yesterday = date.today() - timedelta(days=0)
    period_str = yesterday.strftime("%Y-%m-%d")
    logger.info("starting_daily_payout", period=period_str)
    
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
    WHERE toDate(timestamp) = %(period)s
    GROUP BY track_id
    HAVING streams > 0
    """
    
    parameters = {'period': period_str}
    
    logger.info("executing_analytics_query")
    try:
        result = ch_client.query(query, parameters=parameters)
        rows = result.result_rows 
    except Exception as e:
        logger.error("query_execution_failed", error=str(e))
        return
    
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

    transactions = []
    
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
            reference_id=f"payout_{period_str}_{track_id}",
            description=f"Royalty for {streams} streams on {period_str}"
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