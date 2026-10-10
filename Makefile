# Crypto Quant Terminal — developer workflow.  Run `make` or `make help`.

SHELL := /usr/bin/env bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help
MAKEFLAGS += --no-print-directory

COMPOSE := docker compose
BACKEND := backend
UV_RUN  := cd $(BACKEND) && uv run --frozen
SYMBOL_FLAGS = $(foreach symbol,$(SYMBOLS),-s $(symbol))
WINDOW_FLAGS = --start $(START) $(if $(END),--end $(END))
TF   ?= 1h
RANK ?= sharpe
BACKTEST_FLAGS = -s $(SYMBOL) -t $(TF) $(if $(START),--start $(START)) $(if $(END),--end $(END))

.PHONY: help env .env-check up down ps logs reset psql psql-test redis-cli \
        install lock fmt lint typecheck contracts test test-integration cov check \
        migrate migrate-test ingest backfill gaps strategies backtest sweep \
        trade sessions session orders halt flatten resume stop \
        api web-install web-dev web-build web-start web-check

help: ## Show this help
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z_-]+:.*## / {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# ------------------------------------------------------------------------------------------
# Environment
# ------------------------------------------------------------------------------------------
env: ## Create .env from .env.example with freshly generated secrets (never overwrites)
	@# One shell for the whole recipe: an early exit must stop the overwrite, since
	@# regenerating secrets would lock the app out of an existing database volume.
	@if [ -f .env ]; then \
		echo ".env already exists — leaving it untouched."; \
	else \
		python3 -c 'import os, pathlib, secrets; \
		src = pathlib.Path(".env.example").read_text(encoding="utf-8"); \
		parts = src.split("<generate>"); \
		out = parts[0] + "".join(secrets.token_hex(24) + p for p in parts[1:]); \
		fd = os.open(".env", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600); \
		os.write(fd, out.encode("utf-8")); os.close(fd)'; \
		echo "Created .env with generated secrets (mode 600)."; \
	fi

.env-check:
	@if [ ! -f .env ]; then echo "Missing .env — run 'make env' first." >&2; exit 1; fi
	@if grep -q '<generate>' .env; then \
		echo ".env still contains <generate> placeholders — fill them or rerun 'make env'." >&2; \
		exit 1; \
	fi

# ------------------------------------------------------------------------------------------
# Infrastructure
# ------------------------------------------------------------------------------------------
up: .env-check ## Start TimescaleDB + Redis and wait until both are healthy
	$(COMPOSE) up -d --wait --wait-timeout 180
	@$(COMPOSE) ps

down: ## Stop the stack (data volumes are kept)
	$(COMPOSE) down

ps: ## Show service status and health
	$(COMPOSE) ps

logs: ## Follow infrastructure logs
	$(COMPOSE) logs -f --tail=200

reset: ## Destroy the stack AND its data volumes (requires CONFIRM=yes)
	@if [ "$${CONFIRM:-}" != "yes" ]; then \
		echo "This deletes all market data, sessions and research runs." >&2; \
		echo "Re-run as: make reset CONFIRM=yes" >&2; \
		exit 1; \
	fi
	$(COMPOSE) down --volumes --remove-orphans

psql: ## Open psql on the main database as the owner role
	$(COMPOSE) exec timescaledb sh -c 'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB"'

psql-test: ## Open psql on the integration-test database
	$(COMPOSE) exec timescaledb sh -c 'psql -U "$$POSTGRES_USER" -d "$$CQ_DB_TEST_NAME"'

redis-cli: ## Open redis-cli (authenticated through REDISCLI_AUTH)
	$(COMPOSE) exec redis redis-cli

# ------------------------------------------------------------------------------------------
# Backend
# ------------------------------------------------------------------------------------------
install: ## Create backend/.venv with all extras and dev tools
	cd $(BACKEND) && uv sync --all-extras

lock: ## Re-resolve backend/uv.lock after editing dependencies
	cd $(BACKEND) && uv lock

fmt: ## Format and auto-fix
	$(UV_RUN) ruff format .
	$(UV_RUN) ruff check --fix .

lint: ## Lint without modifying files
	$(UV_RUN) ruff format --check .
	$(UV_RUN) ruff check .

typecheck: ## mypy --strict over src and tests
	$(UV_RUN) mypy

contracts: ## Enforce hexagonal import boundaries
	$(UV_RUN) lint-imports

test: ## Unit tests (no infrastructure needed)
	$(UV_RUN) pytest -m "not integration"

test-integration: .env-check ## Integration tests against the running compose stack
	$(UV_RUN) pytest -m integration

cov: ## Unit tests with branch coverage
	$(UV_RUN) pytest -m "not integration" --cov --cov-report=term-missing

check: lint typecheck contracts test ## Everything CI runs before integration tests

# ------------------------------------------------------------------------------------------
# Market data (Module 1)
# ------------------------------------------------------------------------------------------
migrate: .env-check ## Apply pending database migrations (owner role)
	$(UV_RUN) cq db migrate

migrate-test: .env-check ## Apply migrations to the integration-test database
	$(UV_RUN) cq db migrate --test-db

ingest: .env-check ## Stream live bars + order books (Ctrl-C to stop). SYMBOLS="BTC/USDT ..."
	$(UV_RUN) cq ingest live $(SYMBOL_FLAGS)

backfill: .env-check ## Fill history: make backfill START=2020-01-01 [END=2024-01-01] [SYMBOLS=...]
	@if [ -z "$(START)" ]; then echo "START is required, e.g. make backfill START=2020-01-01" >&2; exit 2; fi
	$(UV_RUN) cq ingest backfill $(WINDOW_FLAGS) $(SYMBOL_FLAGS)

gaps: .env-check ## Report missing bars: make gaps START=2024-01-01 [END=...] [SYMBOLS=...]
	@if [ -z "$(START)" ]; then echo "START is required, e.g. make gaps START=2024-01-01" >&2; exit 2; fi
	$(UV_RUN) cq ingest gaps $(WINDOW_FLAGS) $(SYMBOL_FLAGS)

# ------------------------------------------------------------------------------------------
# Backtesting (Module 2) — bars come from TimescaleDB unless ARGS contains --data FILE.csv
# ------------------------------------------------------------------------------------------
strategies: ## List registered strategies, their parameters and supported engines
	$(UV_RUN) cq backtest strategies

backtest: ## make backtest STRATEGY=momentum SYMBOL=BTC/USDT [TF=1h START=.. END=.. PARAMS="lookback=24" ARGS=..]
	@if [ -z "$(STRATEGY)" ] || [ -z "$(SYMBOL)" ]; then \
		echo "STRATEGY and SYMBOL are required, e.g. make backtest STRATEGY=momentum SYMBOL=BTC/USDT" >&2; \
		exit 2; \
	fi
	$(UV_RUN) cq backtest run $(STRATEGY) $(BACKTEST_FLAGS) $(foreach p,$(PARAMS),-p $(p)) $(ARGS)

sweep: ## make sweep STRATEGY=mean_reversion SYMBOL=BTC/USDT GRID="lookback=24:96:24 entry_z=1.5,2" [RANK=sharpe]
	@if [ -z "$(STRATEGY)" ] || [ -z "$(SYMBOL)" ] || [ -z "$(GRID)" ]; then \
		echo "STRATEGY, SYMBOL and GRID are required, e.g." >&2; \
		echo "  make sweep STRATEGY=mean_reversion SYMBOL=BTC/USDT GRID='lookback=24:96:24'" >&2; \
		exit 2; \
	fi
	$(UV_RUN) cq backtest sweep $(STRATEGY) $(BACKTEST_FLAGS) $(foreach g,$(GRID),-g $(g)) --rank $(RANK) $(ARGS)

# ------------------------------------------------------------------------------------------
# Trading (Module 3) — paper unless .env arms more (docs/ARCHITECTURE.md D10)
# ------------------------------------------------------------------------------------------
define require_session
	@if [ -z "$(SESSION)" ]; then echo "SESSION is required, e.g. make $@ SESSION=mom-1" >&2; exit 2; fi
endef

define require_reason
	@if [ -z "$(REASON)" ]; then echo "REASON is required, e.g. make $@ SESSION=$(SESSION) REASON='manual check'" >&2; exit 2; fi
endef

trade: .env-check ## New: make trade SESSION=mom-1 STRATEGY=momentum SYMBOL=BTC/USDT [TF= PARAMS= ARGS=]; resume: SESSION only
	$(require_session)
	@if [ -n "$(STRATEGY)" ] && [ -z "$(SYMBOL)" ]; then echo "a new session needs SYMBOL too" >&2; exit 2; fi
	$(UV_RUN) cq trade run $(SESSION) $(if $(STRATEGY),--strategy $(STRATEGY) $(foreach s,$(SYMBOL),-s $(s)) -t $(TF) $(foreach p,$(PARAMS),-p $(p))) $(ARGS)

sessions: .env-check ## List trading sessions, most recently active first
	$(UV_RUN) cq trade sessions

session: .env-check ## Session status: make session SESSION=mom-1 (engine, equity, halts, positions, orders, risk events)
	$(require_session)
	$(UV_RUN) cq trade status $(SESSION)

orders: .env-check ## Orders of a session: make orders SESSION=mom-1 [ARGS=--all]
	$(require_session)
	$(UV_RUN) cq trade orders $(SESSION) $(ARGS)

halt: .env-check ## Kill switch — cancel orders, block new risk: make halt SESSION=mom-1 REASON='...'
	$(require_session)
	$(require_reason)
	$(UV_RUN) cq trade halt $(SESSION) -r "$(REASON)"

flatten: .env-check ## Kill switch and close every position: make flatten SESSION=mom-1 REASON='...'
	$(require_session)
	$(require_reason)
	$(UV_RUN) cq trade flatten $(SESSION) -r "$(REASON)"

resume: .env-check ## Lift operator-resumable halts: make resume SESSION=mom-1 REASON='...'
	$(require_session)
	$(require_reason)
	$(UV_RUN) cq trade resume $(SESSION) -r "$(REASON)"

stop: .env-check ## Stop a session's engine gracefully: make stop SESSION=mom-1
	$(require_session)
	$(UV_RUN) cq trade stop $(SESSION)

# ------------------------------------------------------------------------------------------
# Web terminal (Module 4) — gateway on CQ_API_PORT (8000), Next.js on :3000
# ------------------------------------------------------------------------------------------
FRONTEND := frontend
NPM      := cd $(FRONTEND) && npm

api: .env-check ## Serve the terminal gateway: REST, WebSocket and SSE streams [ARGS=--port 8000]
	$(UV_RUN) cq api serve $(ARGS)

web-install: ## Install the web terminal's dependencies (npm ci once a lockfile exists)
	@if [ -f $(FRONTEND)/package-lock.json ]; then $(NPM) ci; else $(NPM) install; fi

web-dev: ## Web terminal with hot reload on http://localhost:3000 (needs `make api`)
	$(NPM) run dev

web-build: ## Production build (self-contained server in frontend/.next/standalone)
	$(NPM) run build

web-start: ## Serve the production build on http://localhost:3000
	$(NPM) run start

web-check: ## Lint, typecheck and unit-test the web terminal
	$(NPM) run check
