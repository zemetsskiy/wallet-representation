# Solana Smart Money Cron Jobs
SHELL=/bin/bash
PATH=/usr/local/bin:/usr/bin:/bin

# Hourly refresh of top 10k Solana smart money wallets (every hour at :00)
0 * * * * cd /app && /usr/local/bin/python worker_scheduled.py solana_smart_money_hourly >> /var/log/cron.log 2>&1

# Daily full refresh of 50k Solana smart money wallets (at 00:30 UTC)
30 0 * * * cd /app && /usr/local/bin/python worker_scheduled.py solana_smart_money_daily >> /var/log/cron.log 2>&1

# EVM Jobs are defined in crontab.evm and run by evm-smart-money-worker
# 15 * * * * cd /app && /usr/local/bin/python worker_scheduled.py evm_eth_smart_money_hourly >> /var/log/cron.log 2>&1
# 45 * * * * cd /app && /usr/local/bin/python worker_scheduled.py evm_polygon_smart_money_hourly >> /var/log/cron.log 2>&1
# 30 1 * * * cd /app && /usr/local/bin/python worker_scheduled.py evm_eth_smart_money_daily >> /var/log/cron.log 2>&1
# 30 2 * * * cd /app && /usr/local/bin/python worker_scheduled.py evm_polygon_smart_money_daily >> /var/log/cron.log 2>&1
