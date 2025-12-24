.PHONY: help up down restart logs ps install migrate migrations run lint format clean check-infra

help:
	@echo "SoundChain Makefile"
	@echo "-------------------"
	@echo "make up          - Start infrastructure (Docker)"
	@echo "make down        - Stop infrastructure"
	@echo "make restart     - Restart infrastructure"
	@echo "make ps          - Check container status"
	@echo "make logs        - View container logs"
	@echo "make install     - Install dependencies via uv"
	@echo "make migrations  - Make Django migrations"
	@echo "make migrate     - Apply Django migrations to DB"
	@echo "make run         - Run development server"
	@echo "make lint        - Check code quality (Ruff + Mypy)"
	@echo "make format      - Auto-format code (Ruff)"
	@echo "make clean       - Remove cache files"

# Infrastructure
up:
	docker compose up -d

down:
	docker compose down

restart: down up

logs:
	docker compose logs -f

ps:
	docker compose ps

check-infra: ps

# Dependencies
install:
	uv sync
	uv run ./manage.py collectstatic --noinput

static:
	uv run ./manage.py collectstatic --noinput

# Development
run:
	@mkdir -p staticfiles
	PYTHONPATH=src uv run uvicorn soundchain.asgi:app --host 0.0.0.0 --port 8000 --reload --reload-dir src

migrations:
	uv run ./manage.py makemigrations

migrate:
	uv run ./manage.py migrate

superuser:
	uv run ./manage.py createsuperuser

# Quality Control
lint:
	# Check style (ruff) and types (mypy)
	PYTHONPATH=src uv run ruff check .
	PYTHONPATH=src uv run mypy src

format:
	# Auto-fix imports and format code
	uv run ruff check --fix .
	uv run ruff format .

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	rm -rf .mypy_cache .ruff_cache

init-db:
	# Init clickhouse tables
	PYTHONPATH=src uv run src/soundchain/domains/analytics/init_db.py

worker:
	# ETL workers (Redpanda -> ClickHouse)
	PYTHONPATH=src uv run src/soundchain/domains/ingestion/worker.py

payout:
	# Calculation of payments (ClickHouse -> Ledger/Postgres)
	PYTHONPATH=src uv run src/soundchain/domains/ledger/payout.py

rest-load:
	# Load via REST
	PYTHONPATH=src uv run src/soundchain/scripts/rest_load_generator.py

grpc-run:
	# Run gRPC server (Port 50051)
	PYTHONPATH=src uv run src/soundchain/grpc_server.py

grpc-load:
	# Load via gRPC (Protobuf)
	PYTHONPATH=src uv run src/soundchain/scripts/grpc_load_generator.py