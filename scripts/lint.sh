#!/bin/sh
set -eu
python -m ruff check src tests
