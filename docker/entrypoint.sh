#!/bin/bash

echo "============================================================"
echo "SOLANA SMART MONEY WORKER - CRON SCHEDULER"
echo "============================================================"
echo "Started at: $(date -u '+%Y-%m-%d %H:%M:%S') UTC"
echo ""
echo "Schedule:"
echo "  - solana_smart_money_hourly : every hour at :00 (10k wallets)"
echo "  - solana_smart_money_daily  : daily at 00:30 UTC (50k wallets)"
echo ""
echo "Waiting for scheduled jobs..."
echo "============================================================"

printenv > /etc/environment
cron
tail -f /var/log/cron.log
