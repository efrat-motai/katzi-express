import logging
import redis

from src.infrastructure.repositories.purchase.base_purchase_repository import BasePurchaseRepository

LOGGER = logging.getLogger(__name__)


class RedisPurchaseRepository(BasePurchaseRepository):
    def __init__(self, ttl=0, redis_client=None):
        self.redis_client = redis_client
        self.ttl = ttl


    def increment_product_count(self,key:str,product_id: int) -> bool:
        try:
            success = self.redis_client.zincrby(key, 1, product_id)
            self.redis_client.expire(key, self.ttl)
            LOGGER.info(f"Incremented count for product {product_id}")
            return success

        except redis.RedisError as e:
            LOGGER.error(f"Redis error: {e}")
            return False

    def get_hot_products(self,key, count=3) -> list:
        try:
            hot_products = self.redis_client.zrevrange(key, 0, count - 1, withscores=True)
            return hot_products
        except redis.RedisError as e:
            LOGGER.error(f"Failed to fetch hot products: {e}")
            return []

    def close_connection(self) -> None:
        if self.redis_client:
            self.redis_client.close()
            LOGGER.info("Redis connection closed safely.")
