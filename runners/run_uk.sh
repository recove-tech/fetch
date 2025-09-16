#!/bin/bash

cd "$(dirname "$0")/.."

log_event() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [$1] $2" >> logs/run.log
}

log_event "START" "Domain: co.uk"
source venv/bin/activate

echo "Domain: co.uk"
python3 main.py --vinted_domain co.uk
log_event "END"