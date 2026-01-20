#!/bin/bash
source .env 2>/dev/null || true
export ALLOWED_USER_IDS
echo "ALLOWED_USER_IDS from env: '$ALLOWED_USER_IDS'"
echo "Parsing test:"
echo "$ALLOWED_USER_IDS" | tr ',' '\n' | while read id; do
  echo "  ID: $id"
done
