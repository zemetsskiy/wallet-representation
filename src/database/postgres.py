import logging
from typing import List, Dict, Any
import psycopg2
from psycopg2.extras import execute_values
from ..config import Config

logger = logging.getLogger(__name__)


class PostgresClient:

    TABLE_NAME = "smartmoney_sol"

    def __init__(self):
        self.conn = None
        self._connect()
        self._ensure_table()

    def _connect(self):
        try:
            if Config.POSTGRES_CONNECTION_STRING:
                self.conn = psycopg2.connect(Config.POSTGRES_CONNECTION_STRING)
            else:
                self.conn = psycopg2.connect(
                    host=Config.POSTGRES_HOST,
                    port=Config.POSTGRES_PORT,
                    user=Config.POSTGRES_USER,
                    password=Config.POSTGRES_PASSWORD,
                    database=Config.POSTGRES_DATABASE
                )
            self.conn.autocommit = False
            logger.info(f'Connected to PostgreSQL at {Config.POSTGRES_HOST}:{Config.POSTGRES_PORT}')
        except Exception as e:
            logger.error(f'Failed to connect to PostgreSQL: {e}')
            raise

    def _ensure_table(self):
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.TABLE_NAME} (
            id SERIAL PRIMARY KEY,
            wallet_address VARCHAR(128) NOT NULL,
            transactions_7d INTEGER DEFAULT 0,
            buys_7d INTEGER DEFAULT 0,
            sells_7d INTEGER DEFAULT 0,
            unique_tokens_7d INTEGER DEFAULT 0,
            realized_pnl_sol_7d DOUBLE PRECISION DEFAULT 0,
            realized_pnl_usd_7d DOUBLE PRECISION DEFAULT 0,
            winrate_percent_7d DOUBLE PRECISION DEFAULT 0,
            transactions_30d INTEGER DEFAULT 0,
            buys_30d INTEGER DEFAULT 0,
            sells_30d INTEGER DEFAULT 0,
            unique_tokens_30d INTEGER DEFAULT 0,
            realized_pnl_sol_30d DOUBLE PRECISION DEFAULT 0,
            realized_pnl_usd_30d DOUBLE PRECISION DEFAULT 0,
            winrate_percent_30d DOUBLE PRECISION DEFAULT 0,
            sol_price_usd DOUBLE PRECISION DEFAULT 0,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_smartmoney_sol_wallet ON {self.TABLE_NAME} (wallet_address);
        CREATE INDEX IF NOT EXISTS idx_smartmoney_sol_pnl_30d ON {self.TABLE_NAME} (realized_pnl_usd_30d DESC);
        CREATE INDEX IF NOT EXISTS idx_smartmoney_sol_pnl_7d ON {self.TABLE_NAME} (realized_pnl_usd_7d DESC);
        CREATE INDEX IF NOT EXISTS idx_smartmoney_sol_winrate_30d ON {self.TABLE_NAME} (winrate_percent_30d DESC);
        CREATE INDEX IF NOT EXISTS idx_smartmoney_sol_created ON {self.TABLE_NAME} (created_at DESC);
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute(create_sql)
            self.conn.commit()
            logger.info(f'Ensured table {self.TABLE_NAME} exists')
        except Exception as e:
            self.conn.rollback()
            logger.error(f'Failed to create table: {e}')
            raise

    def refresh_smart_money(self, metrics: List[Dict[str, Any]], sol_price: float) -> int:
        if not metrics:
            logger.warning("No metrics to insert")
            return 0

        try:
            with self.conn.cursor() as cur:
                cur.execute("SELECT NOW()")
                insert_timestamp = cur.fetchone()[0]

                insert_sql = f"""
                INSERT INTO {self.TABLE_NAME} (
                    wallet_address, transactions_7d, buys_7d, sells_7d, unique_tokens_7d,
                    realized_pnl_sol_7d, realized_pnl_usd_7d, winrate_percent_7d,
                    transactions_30d, buys_30d, sells_30d, unique_tokens_30d,
                    realized_pnl_sol_30d, realized_pnl_usd_30d, winrate_percent_30d,
                    sol_price_usd, created_at
                ) VALUES %s
                """

                values = []
                for m in metrics:
                    pnl_sol_7d = float(m.get('realized_pnl_sol_7d', 0))
                    pnl_sol_30d = float(m.get('realized_pnl_sol_30d', 0))
                    values.append((
                        m['wallet_address'],
                        int(m.get('transactions_7d', 0)),
                        int(m.get('buys_7d', 0)),
                        int(m.get('sells_7d', 0)),
                        int(m.get('unique_tokens_7d', 0)),
                        pnl_sol_7d,
                        pnl_sol_7d * sol_price,
                        float(m.get('winrate_percent_7d', 0)),
                        int(m.get('transactions_30d', 0)),
                        int(m.get('buys_30d', 0)),
                        int(m.get('sells_30d', 0)),
                        int(m.get('unique_tokens_30d', 0)),
                        pnl_sol_30d,
                        pnl_sol_30d * sol_price,
                        float(m.get('winrate_percent_30d', 0)),
                        sol_price,
                    ))

                execute_values(
                    cur, insert_sql, values,
                    template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())"
                )
                logger.info(f'Inserted {len(metrics):,} fresh smart money records')

                cur.execute(f"DELETE FROM {self.TABLE_NAME} WHERE created_at < %s", (insert_timestamp,))
                deleted_count = cur.rowcount
                logger.info(f'Deleted {deleted_count:,} old records')

            self.conn.commit()
            return len(metrics)

        except Exception as e:
            self.conn.rollback()
            logger.error(f'Failed to refresh smart money data: {e}')
            raise

    def get_wallet_count(self) -> int:
        try:
            with self.conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {self.TABLE_NAME}")
                result = cur.fetchone()
                return result[0] if result else 0
        except Exception as e:
            logger.error(f'Failed to get wallet count: {e}')
            return 0

    def close(self):
        if self.conn:
            self.conn.close()
            logger.info('PostgreSQL connection closed')


_postgres_client = None


def get_postgres_client() -> PostgresClient:
    global _postgres_client
    if _postgres_client is None:
        _postgres_client = PostgresClient()
    return _postgres_client
