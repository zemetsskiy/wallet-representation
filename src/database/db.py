import logging
from typing import List, Dict, Any, Optional
from uuid import uuid4
import clickhouse_connect
from ..config import Config

logger = logging.getLogger(__name__)


class ClickHouseClient:

    def __init__(self, use_evm_host: bool = False):
        self.client = None
        self.use_evm_host = use_evm_host
        self._connect()

    def _connect(self):
        try:
            host = Config.CLICKHOUSE_HOST_EVM if self.use_evm_host and Config.CLICKHOUSE_HOST_EVM else Config.CLICKHOUSE_HOST
            database = Config.CLICKHOUSE_DATABASE_EVM if self.use_evm_host else Config.CLICKHOUSE_DATABASE
            
            self.client = clickhouse_connect.get_client(
                host=host,
                port=Config.CLICKHOUSE_PORT,
                username=Config.CLICKHOUSE_USER,
                password=Config.CLICKHOUSE_PASSWORD,
                database=database
            )
            logger.info(f'Connected to ClickHouse at {host}:{Config.CLICKHOUSE_PORT} (DB: {database})')
        except Exception as e:
            logger.error(f'Failed to connect to ClickHouse: {e}')
            raise

    def execute_query_dict(self, query: str, parameters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        attempts = 2
        for attempt in range(attempts):
            try:
                logger.info('Executing query...')
                settings = {
                    'session_id': str(uuid4()),
                    'session_timeout': 900,
                    'max_execution_time': 900
                }
                result = self.client.query(query, parameters=parameters or {}, settings=settings)
                column_names = result.column_names
                dict_rows = [dict(zip(column_names, row)) for row in result.result_rows]
                logger.info(f'Query completed: {len(dict_rows):,} rows')
                return dict_rows
            except Exception as e:
                msg = str(e)
                if ('SESSION_IS_LOCKED' in msg or 'code: 373' in msg) and attempt < attempts - 1:
                    logger.warning('Session locked, reconnecting...')
                    self._connect()
                    continue
                logger.error(f'Query execution failed: {e}', exc_info=True)
                raise

    def close(self):
        if self.client:
            self.client.close()
            logger.info('ClickHouse connection closed')


_db_client = None
_db_client_evm = None


def get_db_client(use_evm_host: bool = False) -> ClickHouseClient:
    global _db_client, _db_client_evm
    
    if use_evm_host:
        if _db_client_evm is None:
            _db_client_evm = ClickHouseClient(use_evm_host=True)
        return _db_client_evm
    else:
        if _db_client is None:
            _db_client = ClickHouseClient(use_evm_host=False)
        return _db_client
