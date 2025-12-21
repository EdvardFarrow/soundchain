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

# Development
run:
	# Listens on 0.0.0.0 to allow external access (e.g. from other containers)
	uv run ./manage.py runserver 0.0.0.0:8000

migrations:
	uv run ./manage.py makemigrations

migrate:
	uv run ./manage.py migrate

superuser:
	uv run ./manage.py createsuperuser

# Quality Control
lint:
	# Check style (ruff) and types (mypy)
	uv run ruff check .
	uv run mypy src

format:
	# Auto-fix imports and format code
	uv run ruff check --fix .
	uv run ruff format .

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	rm -rf .mypy_cache .ruff_cache