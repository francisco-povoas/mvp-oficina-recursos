.PHONY: install lint test build

install:
	python -m pip install -r requirements-dev.txt

lint:
	python -m ruff check src tests

test:
	python -m pytest --cov=src/app --cov-report=term-missing

build:
	docker build --target runtime -t mvp-oficina-recursos:local .
