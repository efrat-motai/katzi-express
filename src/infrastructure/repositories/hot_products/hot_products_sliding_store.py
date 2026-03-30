import logging
import redis

LOGGER = logging.getLogger(__name__)


class HotProductsSlidingStore:
    def __init__(self, ttl=0, redis_client=None):
        self.redis_client = redis_client
        self.ttl = ttl

    def record_purchase(self, purchase_notification, event_timestamp) -> bool:
        try:
            success = self.redis_client.zadd("hot_products", {purchase_notification: event_timestamp})
            self.redis_client.expire("hot_products", self.ttl, nx=True)
            return success

        except redis.RedisError as e:
            LOGGER.error(f"Redis error: {e}")
            return False
        except Exception as e:
            LOGGER.error(f"Unexpected error: {e}")
            return False

    