import logging
from typing import Dict, Any
from ..database import get_db_client, RedisClient, get_postgres_client
from ..database.redis_client import RedisPriceNotFoundError

logger = logging.getLogger(__name__)


class SolanaSmartMoneyAnalyzer:

    def __init__(self):
        self.db = get_db_client()
        self.redis = RedisClient()
        self.postgres = get_postgres_client()

    def _build_smart_money_query(self, limit: int = 10000) -> str:
        query = """
        WITH
        normalized_swaps AS (
            SELECT
                signing_wallet,
                block_time,
                CASE
                    WHEN base_coin = 'So11111111111111111111111111111111111111112' THEN quote_coin
                    ELSE base_coin
                END AS traded_token,
                CASE
                    WHEN base_coin = 'So11111111111111111111111111111111111111112' AND direction = 'S' THEN 'buy'
                    WHEN base_coin = 'So11111111111111111111111111111111111111112' AND direction = 'B' THEN 'sell'
                    WHEN quote_coin = 'So11111111111111111111111111111111111111112' AND direction = 'B' THEN 'buy'
                    WHEN quote_coin = 'So11111111111111111111111111111111111111112' AND direction = 'S' THEN 'sell'
                END AS action,
                CASE
                    WHEN base_coin = 'So11111111111111111111111111111111111111112' THEN base_coin_amount
                    ELSE quote_coin_amount
                END AS sol_amount,
                CASE
                    WHEN base_coin = 'So11111111111111111111111111111111111111112' THEN quote_coin_amount
                    ELSE base_coin_amount
                END AS traded_amount
            FROM solana.swaps
            PREWHERE block_time >= now() - INTERVAL 30 DAY
            WHERE (base_coin = 'So11111111111111111111111111111111111111112'
                   OR quote_coin = 'So11111111111111111111111111111111111111112')
        ),
        wallet_token_stats AS (
            SELECT
                signing_wallet,
                traded_token,
                SUM(IF(action = 'buy', traded_amount, 0)) AS total_bought_30d,
                SUM(IF(action = 'sell', traded_amount, 0)) AS total_sold_30d,
                SUM(IF(action = 'buy', sol_amount, 0)) AS sol_spent_30d,
                SUM(IF(action = 'sell', sol_amount, 0)) AS sol_received_30d,
                SUM(IF(action = 'buy', 1, 0)) AS buy_count_30d,
                SUM(IF(action = 'sell', 1, 0)) AS sell_count_30d,
                SUM(IF(action = 'buy' AND block_time >= now() - INTERVAL 7 DAY, traded_amount, 0)) AS total_bought_7d,
                SUM(IF(action = 'sell' AND block_time >= now() - INTERVAL 7 DAY, traded_amount, 0)) AS total_sold_7d,
                SUM(IF(action = 'buy' AND block_time >= now() - INTERVAL 7 DAY, sol_amount, 0)) AS sol_spent_7d,
                SUM(IF(action = 'sell' AND block_time >= now() - INTERVAL 7 DAY, sol_amount, 0)) AS sol_received_7d,
                SUM(IF(action = 'buy' AND block_time >= now() - INTERVAL 7 DAY, 1, 0)) AS buy_count_7d,
                SUM(IF(action = 'sell' AND block_time >= now() - INTERVAL 7 DAY, 1, 0)) AS sell_count_7d
            FROM normalized_swaps
            GROUP BY signing_wallet, traded_token
            HAVING buy_count_30d > 0 AND sell_count_30d > 0
                   AND total_bought_30d > 0 AND total_sold_30d > 0
        ),
        token_pnl AS (
            SELECT
                signing_wallet,
                traded_token,
                buy_count_30d,
                sell_count_30d,
                buy_count_7d,
                sell_count_7d,
                (sol_received_30d / total_sold_30d - sol_spent_30d / total_bought_30d)
                    * least(total_bought_30d, total_sold_30d) AS pnl_sol_30d,
                IF(sol_received_30d / total_sold_30d > sol_spent_30d / total_bought_30d, 1, 0) AS is_profitable_30d,
                IF(total_bought_7d > 0 AND total_sold_7d > 0,
                   (sol_received_7d / total_sold_7d - sol_spent_7d / total_bought_7d)
                       * least(total_bought_7d, total_sold_7d), 0) AS pnl_sol_7d,
                IF(total_bought_7d > 0 AND total_sold_7d > 0
                   AND sol_received_7d / total_sold_7d > sol_spent_7d / total_bought_7d, 1, 0) AS is_profitable_7d
            FROM wallet_token_stats
            WHERE sol_spent_30d > 0 AND sol_received_30d > 0
        ),
        wallet_metrics AS (
            SELECT
                signing_wallet,
                SUM(pnl_sol_7d) AS total_pnl_sol_7d,
                100.0 * SUM(is_profitable_7d) / NULLIF(SUM(IF(buy_count_7d > 0 AND sell_count_7d > 0, 1, 0)), 0) AS winrate_7d,
                SUM(buy_count_7d) AS total_buys_7d,
                SUM(sell_count_7d) AS total_sells_7d,
                COUNT(DISTINCT IF((buy_count_7d > 0 OR sell_count_7d > 0), traded_token, NULL)) AS unique_tokens_7d,
                SUM(pnl_sol_30d) AS total_pnl_sol_30d,
                100.0 * SUM(is_profitable_30d) / COUNT(*) AS winrate_30d,
                SUM(buy_count_30d) AS total_buys_30d,
                SUM(sell_count_30d) AS total_sells_30d,
                COUNT(DISTINCT traded_token) AS unique_tokens_30d
            FROM token_pnl
            GROUP BY signing_wallet
        ),
        transaction_counts AS (
            SELECT
                signing_wallet,
                COUNT(*) as tx_count_30d,
                SUM(IF(block_time >= now() - INTERVAL 7 DAY, 1, 0)) as tx_count_7d
            FROM normalized_swaps
            GROUP BY signing_wallet
        )
        SELECT
            w.signing_wallet AS wallet_address,
            COALESCE(tc.tx_count_7d, 0) AS transactions_7d,
            COALESCE(w.total_buys_7d, 0) AS buys_7d,
            COALESCE(w.total_sells_7d, 0) AS sells_7d,
            COALESCE(w.unique_tokens_7d, 0) AS unique_tokens_7d,
            ROUND(COALESCE(w.total_pnl_sol_7d, 0) / 1e9, 6) AS realized_pnl_sol_7d,
            ROUND(COALESCE(w.winrate_7d, 0), 2) AS winrate_percent_7d,
            COALESCE(tc.tx_count_30d, 0) AS transactions_30d,
            COALESCE(w.total_buys_30d, 0) AS buys_30d,
            COALESCE(w.total_sells_30d, 0) AS sells_30d,
            COALESCE(w.unique_tokens_30d, 0) AS unique_tokens_30d,
            ROUND(COALESCE(w.total_pnl_sol_30d, 0) / 1e9, 6) AS realized_pnl_sol_30d,
            ROUND(COALESCE(w.winrate_30d, 0), 2) AS winrate_percent_30d
        FROM wallet_metrics w
        LEFT JOIN transaction_counts tc ON w.signing_wallet = tc.signing_wallet
        ORDER BY total_pnl_sol_30d DESC
        LIMIT {limit}
        """
        return query.format(limit=limit)

    def analyze_smart_money(self, limit: int = 10000) -> Dict[str, Any]:
        logger.info("=" * 60)
        logger.info("SOLANA SMART MONEY ANALYSIS")
        logger.info("=" * 60)

        try:
            sol_price = self.redis.get_sol_price()
            logger.info(f"SOL price: ${sol_price:.2f}")
        except RedisPriceNotFoundError as e:
            logger.error(f"Cannot proceed without SOL price: {e}")
            raise

        logger.info(f"Fetching top {limit:,} wallets by PnL...")
        query = self._build_smart_money_query(limit=limit)

        try:
            metrics = self.db.execute_query_dict(query)
            logger.info(f"Retrieved {len(metrics):,} wallet metrics")
        except Exception as e:
            logger.error(f"Failed to fetch metrics: {e}")
            raise

        if not metrics:
            logger.warning("No metrics found")
            return {'wallets_processed': 0, 'sol_price_usd': sol_price, 'wallets_stored': 0}

        try:
            stored_count = self.postgres.refresh_smart_money(metrics, sol_price)
        except Exception as e:
            logger.error(f"Failed to refresh data: {e}")
            raise

        total_wallets = self.postgres.get_wallet_count()

        logger.info("=" * 60)
        logger.info(f"COMPLETE: {len(metrics):,} wallets, ${sol_price:.2f} SOL, {total_wallets:,} in DB")
        logger.info("=" * 60)

        return {
            'wallets_processed': len(metrics),
            'sol_price_usd': sol_price,
            'wallets_stored': stored_count,
            'total_wallets_in_db': total_wallets
        }

    def close(self):
        self.db.close()
        self.postgres.close()
