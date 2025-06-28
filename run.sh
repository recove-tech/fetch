#!/bin/bash

log_event() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [$1] $2" >> logs/run.log
}

log_event "START" "Beginning script execution"

echo "Running for women..."
log_event "INFO" "Starting women's data processing"
python main.py -w True -fby color
log_event "INFO" "Completed women's data processing"

echo -e "\nRunning for men..."
log_event "INFO" "Starting men's data processing"
python main.py -w False -fby color
log_event "INFO" "Completed men's data processing"

log_event "END" "Script execution completed" 