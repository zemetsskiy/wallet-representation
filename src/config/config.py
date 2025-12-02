import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'localhost')
    CLICKHOUSE_PORT = int(os.getenv('CLICKHOUSE_PORT', '8123'))
    CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER', 'default')
    CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', '')
    CLICKHOUSE_DATABASE = os.getenv('CLICKHOUSE_DATABASE', 'solana')

    POSTGRES_CONNECTION_STRING = os.getenv('POSTGRES_CONNECTION_STRING', None)
    POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
    POSTGRES_PORT = int(os.getenv('POSTGRES_PORT', '5432'))
    POSTGRES_USER = os.getenv('POSTGRES_USER', 'postgres')
    POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'postgres')
    POSTGRES_DATABASE = os.getenv('POSTGRES_DATABASE', 'wallet_metrics')

    BATCH_SIZE = int(os.getenv('BATCH_SIZE', '10000'))
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

    REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
    REDIS_PORT = int(os.getenv('REDIS_PORT', '6379'))
    REDIS_DB = int(os.getenv('REDIS_DB', '2'))
    REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', None)
    SOL_PRICE_KEY = os.getenv('SOL_PRICE_KEY', 'solana:price_usd')

    SOL_ADDRESS = 'So11111111111111111111111111111111111111112'
    SOL_DECIMALS = 9

    @classmethod
    def validate(cls):
        required = ['CLICKHOUSE_HOST', 'CLICKHOUSE_PORT', 'CLICKHOUSE_USER', 'CLICKHOUSE_DATABASE']
        missing = [f for f in required if not getattr(cls, f)]
        if missing:
            raise ValueError(f"Missing required configuration: {', '.join(missing)}")
        return True


Config.validate()
