import logging
import redis

LOGGER = logging.getLogger(__name__)


class RedisRepository:
    def __init__(self, host="localhost", port=6379, ttl=0, db=0):
        self.r = None
        self.connect(host, port, db)
        self.ttl = ttl

    def connect(self, host: str, port: int, db: int) -> None:
        try:
            self.r = redis.Redis(host=host, port=port, db=db, decode_responses=True)
            self.r.ping()
            LOGGER.info("Successfully connected to Redis")
        except redis.ConnectionError as e:
            LOGGER.error(f"Redis connection error: {e}")

    def increment_product_count(self,key:str,product_id: int) -> bool:
        try:
            success = self.r.zincrby(key, 1, product_id)
            self.r.expire(key, self.ttl)
            LOGGER.info(f"Incremented count for product {product_id}")
            return success

        except redis.RedisError as e:
            LOGGER.error(f"Redis error: {e}")
            return False

    def get_hot_products(self,key, count=3) -> list:
        try:
            hot_products = self.r.zrevrange(key, 0, count - 1, withscores=True)
            return hot_products
        except redis.RedisError as e:
            LOGGER.error(f"Failed to fetch hot products: {e}")
            return []

    def close_connection(self) -> None:
        if self.r:
            self.r.close()
            LOGGER.info("Redis connection closed safely.")
