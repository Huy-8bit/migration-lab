#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

[ -f .env ] && set -a && source .env && set +a

echo "Generating initial dataset..."
python -m src.data_generator.main

echo "Data generation complete!"
