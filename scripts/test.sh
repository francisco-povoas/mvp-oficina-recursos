#!/bin/sh
set -eu
python -m pytest --cov=src/app --cov-report=term-missing
