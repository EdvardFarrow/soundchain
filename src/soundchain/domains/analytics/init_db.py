import clickhouse_connect
import structlog
from decouple import config
from soundchain.utils.logging import setup_logging


setup_logging()
logger = structlog.get_logger("init_db")

def init_clickhouse():
    """
    Creates a database and tables in ClickHouse (Idempotent).
    """
    target_db = config("CLICKHOUSE_DB")
    
    try:
        client = clickhouse_connect.get_client(
            host=config("CLICKHOUSE_HOST"),
            port=config("CLICKHOUSE_PORT", cast=int),
            username=config("CLICKHOUSE_USER"),
            password=config("CLICKHOUSE_PASSWORD"),
            database="default"
        )
    except Exception as e:
        logger.critical("connection_failed", error=str(e))
        raise

    try:
        client.command("CREATE DATABASE IF NOT EXISTS {db}".format(db=target_db))
        logger.info("database_ensured", name=target_db)

        ddl = """
        CREATE TABLE IF NOT EXISTS {db}.listens (
            event_id UUID,
            user_id UUID,
            track_id UUID,
            timestamp DateTime64(3),
            duration_played_ms UInt32,
            country FixedString(2),
            platform LowCardinality(String),
            
            -- tech fields
            inserted_at DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY (country, timestamp)
        PARTITION BY toYYYYMM(timestamp)
        TTL toDateTime(timestamp) + INTERVAL 1 YEAR;
        """.format(db=target_db)
        
        client.command(ddl)
        logger.info("table_created", table="{}.listens".format(target_db))

    except Exception as e:
        logger.critical("init_failed", error=str(e))
        raise

if __name__ == "__main__":
    init_clickhouse()