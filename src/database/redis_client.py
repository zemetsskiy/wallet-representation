import redis
import logging
from ..config import Config

logger = logging.getLogger(__name__)


class RedisConnectionError(Exception):
    pass


class RedisPriceNotFoundError(Exception):
    pass


class RedisClient:

    def __init__(self):
        self.enabled = False
        self.client = None

        if not Config.REDIS_HOST:
            raise RedisConnectionError("REDIS_HOST is not configured")

        logger.info(f"Connecting to Redis: {Config.REDIS_HOST}:{Config.REDIS_PORT}/{Config.REDIS_DB}")

        try:
            self.client = redis.Redis(
                host=Config.REDIS_HOST,
                port=Config.REDIS_PORT,
                db=Config.REDIS_DB,
                password=Config.REDIS_PASSWORD,
                decode_responses=True,
                socket_timeout=5.0
            )
            self.client.ping()
            self.enabled = True
            logger.info(f"Connected to Redis")
        except redis.ConnectionError as e:
            raise RedisConnectionError(f"Failed to connect to Redis: {e}")
        except Exception as e:
            raise RedisConnectionError(f"Unexpected Redis error: {e}")

    def get_sol_price(self) -> float:
        if not self.enabled or not self.client:
            raise RedisPriceNotFoundError("Redis client is not connected")

        try:
            price_str = self.client.get(Config.SOL_PRICE_KEY)
            if price_str:
                price = float(price_str)
                logger.info(f"SOL price from Redis: ${price:.2f}")
                return price
            else:
                raise RedisPriceNotFoundError(f"Key '{Config.SOL_PRICE_KEY}' not found in Redis")
        except RedisPriceNotFoundError:
            raise
        except Exception as e:
            raise RedisPriceNotFoundError(f"Error fetching SOL price: {e}")

    def get_eth_price(self) -> float:
        if not self.enabled or not self.client:
            raise RedisPriceNotFoundError("Redis client is not connected")

        try:
            price_str = self.client.get(Config.ETH_PRICE_KEY)
            if price_str:
                price = float(price_str)
                logger.info(f"ETH price from Redis: ${price:.2f}")
                return price
            else:
                raise RedisPriceNotFoundError(f"Key '{Config.ETH_PRICE_KEY}' not found in Redis")
        except RedisPriceNotFoundError:
            raise
        except Exception as e:
            raise RedisPriceNotFoundError(f"Error fetching ETH price: {e}")

    def get_matic_price(self) -> float:
        if not self.enabled or not self.client:
            raise RedisPriceNotFoundError("Redis client is not connected")

        try:
            price_str = self.client.get(Config.MATIC_PRICE_KEY)
            if price_str:
                price = float(price_str)
                logger.info(f"MATIC price from Redis: ${price:.2f}")
                return price
            else:
                raise RedisPriceNotFoundError(f"Key '{Config.MATIC_PRICE_KEY}' not found in Redis")
        except RedisPriceNotFoundError:
            raise
        except Exception as e:
            raise RedisPriceNotFoundError(f"Error fetching MATIC price: {e}")
