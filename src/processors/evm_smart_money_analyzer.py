import logging
from typing import Dict, Any, List
from ..database import get_db_client, RedisClient, get_postgres_client
from ..database.redis_client import RedisPriceNotFoundError

logger = logging.getLogger(__name__)


class EvmSmartMoneyAnalyzer:

    CHAIN_CONFIG = {
        'eth': {
            'native_tokens': [
                '0x0000000000000000000000000000000000000000',
                '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee',
                '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'  # WETH
            ],
            'price_getter': 'get_eth_price'
        },
        'polygon': {
            'native_tokens': [
                '0x0000000000000000000000000000000000000000',
                '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee',
                '0x0000000000000000000000000000000000001010', # MATIC (sometimes)
                '0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270', # WMATIC
                '0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619'  # WETH on Polygon
            ],
            'price_getter': 'get_matic_price'
        }
    }

    def __init__(self):
        self.db = get_db_client(use_evm_host=True)
        self.redis = RedisClient()
        self.postgres = get_postgres_client()

    def _build_evm_query(self, chain: str, limit: int = 10000, price: float = 0.0) -> str:
        native_tokens = self.CHAIN_CONFIG.get(chain, {}).get('native_tokens', [])
        native_tokens_str = ", ".join([f"'{t}'" for t in native_tokens])

        query = f"""
        WITH
        normalized_swaps AS (
            SELECT
                tx_from_address AS signing_wallet,
                block_time,
                CASE
                    WHEN base_coin IN ({native_tokens_str}) THEN quote_coin
                    ELSE base_coin
                END AS traded_token,
                CASE
                    WHEN base_coin IN ({native_tokens_str}) THEN 'buy' -- Buying token with Native
                    ELSE 'sell' -- Selling token for Native
                END AS action,
                CASE
                    WHEN base_coin IN ({native_tokens_str}) THEN base_coin_amount / pow(10, base_coin_decimals)
                    ELSE quote_coin_amount / pow(10, quote_coin_decimals)
                END AS native_amount,
                CASE
                    WHEN base_coin IN ({native_tokens_str}) THEN quote_coin_amount / pow(10, quote_coin_decimals)
                    ELSE base_coin_amount / pow(10, base_coin_decimals)
                END AS traded_amount
            FROM "evm"."swap_events"
            PREWHERE chain = '{chain}' AND block_time >= now() - INTERVAL 30 DAY
            WHERE (base_coin IN ({native_tokens_str}) OR quote_coin IN ({native_tokens_str}))
        ),
        wallet_token_stats AS (
            SELECT
                signing_wallet,
                traded_token,
                SUM(IF(action = 'buy', traded_amount, 0)) AS total_bought_30d,
                SUM(IF(action = 'sell', traded_amount, 0)) AS total_sold_30d,
                SUM(IF(action = 'buy', native_amount, 0)) AS native_spent_30d,
                SUM(IF(action = 'sell', native_amount, 0)) AS native_received_30d,
                SUM(IF(action = 'buy', 1, 0)) AS buy_count_30d,
                SUM(IF(action = 'sell', 1, 0)) AS sell_count_30d,
                SUM(IF(action = 'buy' AND block_time >= now() - INTERVAL 7 DAY, traded_amount, 0)) AS total_bought_7d,
                SUM(IF(action = 'sell' AND block_time >= now() - INTERVAL 7 DAY, traded_amount, 0)) AS total_sold_7d,
                SUM(IF(action = 'buy' AND block_time >= now() - INTERVAL 7 DAY, native_amount, 0)) AS native_spent_7d,
                SUM(IF(action = 'sell' AND block_time >= now() - INTERVAL 7 DAY, native_amount, 0)) AS native_received_7d,
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
                (native_received_30d / total_sold_30d - native_spent_30d / total_bought_30d)
                    * least(total_bought_30d, total_sold_30d) AS pnl_native_30d,
                IF(native_received_30d / total_sold_30d > native_spent_30d / total_bought_30d, 1, 0) AS is_profitable_30d,
                IF(total_bought_7d > 0 AND total_sold_7d > 0,
                   (native_received_7d / total_sold_7d - native_spent_7d / total_bought_7d)
                       * least(total_bought_7d, total_sold_7d), 0) AS pnl_native_7d,
                IF(total_bought_7d > 0 AND total_sold_7d > 0
                   AND native_received_7d / total_sold_7d > native_spent_7d / total_bought_7d, 1, 0) AS is_profitable_7d
            FROM wallet_token_stats
            WHERE native_spent_30d > 0 AND native_received_30d > 0
        ),
        wallet_metrics AS (
            SELECT
                signing_wallet,
                SUM(pnl_native_7d) AS total_pnl_native_7d,
                100.0 * SUM(is_profitable_7d) / NULLIF(SUM(IF(buy_count_7d > 0 AND sell_count_7d > 0, 1, 0)), 0) AS winrate_7d,
                SUM(buy_count_7d) AS total_buys_7d,
                SUM(sell_count_7d) AS total_sells_7d,
                COUNT(DISTINCT IF((buy_count_7d > 0 OR sell_count_7d > 0), traded_token, NULL)) AS unique_tokens_7d,
                SUM(pnl_native_30d) AS total_pnl_native_30d,
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
            trimBoth(toString(w.signing_wallet), '\\0') AS wallet_address,
            COALESCE(tc.tx_count_7d, 0) AS transactions_7d,
            COALESCE(w.total_buys_7d, 0) AS buys_7d,
            COALESCE(w.total_sells_7d, 0) AS sells_7d,
            COALESCE(w.unique_tokens_7d, 0) AS unique_tokens_7d,
            ROUND(COALESCE(w.total_pnl_native_7d, 0), 6) AS realized_pnl_native_7d,
            ROUND(COALESCE(w.total_pnl_native_7d, 0) * {price}, 2) AS realized_pnl_usd_7d,
            ROUND(COALESCE(w.winrate_7d, 0), 2) AS winrate_percent_7d,
            COALESCE(tc.tx_count_30d, 0) AS transactions_30d,
            COALESCE(w.total_buys_30d, 0) AS buys_30d,
            COALESCE(w.total_sells_30d, 0) AS sells_30d,
            COALESCE(w.unique_tokens_30d, 0) AS unique_tokens_30d,
            ROUND(COALESCE(w.total_pnl_native_30d, 0), 6) AS realized_pnl_native_30d,
            ROUND(COALESCE(w.total_pnl_native_30d, 0) * {price}, 2) AS realized_pnl_usd_30d,
            ROUND(COALESCE(w.winrate_30d, 0), 2) AS winrate_percent_30d
        FROM wallet_metrics w
        LEFT JOIN transaction_counts tc ON w.signing_wallet = tc.signing_wallet
        ORDER BY total_pnl_native_30d DESC
        LIMIT {limit}
        """
        return query

    def analyze_smart_money(self, chain: str, limit: int = 10000) -> Dict[str, Any]:
        if chain not in self.CHAIN_CONFIG:
            raise ValueError(f"Unsupported chain: {chain}")

        logger.info("=" * 60)
        logger.info(f"{chain.upper()} SMART MONEY ANALYSIS")
        logger.info("=" * 60)

        price_getter_name = self.CHAIN_CONFIG[chain]['price_getter']
        price_getter = getattr(self.redis, price_getter_name)

        try:
            native_price = price_getter()
            logger.info(f"{chain.upper()} price: ${native_price:.2f}")
        except RedisPriceNotFoundError as e:
            logger.error(f"Cannot proceed without {chain.upper()} price: {e}")
            raise

        logger.info(f"Fetching top {limit:,} wallets by PnL...")
        query = self._build_evm_query(chain, limit=limit, price=native_price)

        try:
            metrics = self.db.execute_query_dict(query)
            logger.info(f"Retrieved {len(metrics):,} wallet metrics")
        except Exception as e:
            logger.error(f"Failed to fetch metrics: {e}")
            raise

        if not metrics:
            logger.warning("No metrics found")
            return {'wallets_processed': 0, 'native_price_usd': native_price, 'wallets_stored': 0}

        try:
            stored_count = self.postgres.refresh_evm_smart_money(metrics, chain, native_price)
        except Exception as e:
            logger.error(f"Failed to refresh data: {e}")
            raise

        total_wallets = self.postgres.get_evm_wallet_count(chain)

        logger.info("=" * 60)
        logger.info(f"COMPLETE: {len(metrics):,} wallets, ${native_price:.2f} {chain.upper()}, {total_wallets:,} in DB")
        logger.info("=" * 60)

        return {
            'wallets_processed': len(metrics),
            'native_price_usd': native_price,
            'wallets_stored': stored_count,
            'total_wallets_in_db': total_wallets
        }

    def close(self):
        self.db.close()
        self.postgres.close()
