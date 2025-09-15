#!/bin/bash

log_event() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [$1] $2" >> logs/run.log
}

log_event "START" "Beginning script execution"

echo "Activating virtual environment..."
source venv/bin/activate

echo "Running..."
log_event "INFO" "Starting data processing"
python3 main.py -fby color
log_event "INFO" "Completed data processing"

log_event "END" "Script execution completed" 