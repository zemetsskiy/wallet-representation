#!/bin/bash

echo "============================================================"
echo "SMART MONEY WORKER - CRON SCHEDULER"
echo "============================================================"
echo "Started at: $(date -u '+%Y-%m-%d %H:%M:%S') UTC"
echo ""
echo "Current Crontab Configuration:"
cat /etc/cron.d/wallet-cron | grep -v "^#" | grep -v "^SHELL" | grep -v "^PATH" | grep -v "^$"
echo ""
echo "Waiting for scheduled jobs..."
echo "============================================================"

# Export environment variables for cron
printenv > /etc/environment
cron
tail -f /var/log/cron.log
