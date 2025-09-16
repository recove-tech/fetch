#!/bin/bash

cd "$(dirname "$0")/.."

log_event() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [$1] $2" >> logs/run.log
}

log_event "START" "Domain: fr"
source venv/bin/activate

echo "Domain: fr"
python3 main.py --vinted_domain fr
log_event "END"