import json
import logging
import redis

LOGGER = logging.getLogger(__name__)

class RedisRepository:
    def __init__(self, host="localhost", port=6379):
        self.r = None
        self.connect(host, port)

    def connect(self, host, port):
        try:
            self.r = redis.Redis(host=host, port=port, decode_responses=True)
            self.r.ping()
            LOGGER.info("Successfully connected to Redis")
        except redis.ConnectionError as e:
            LOGGER.error(f"Redis connection error: {e}")

    def add_notification(self, notification: dict) -> bool:
        try:
            payload = json.dumps(notification)
            success= self.r.setex(f"order:{notification['order_id']}", 180, payload)
            LOGGER.info(f"Successfully added notification: {notification['order_id']} to redis")
            return success
        except redis.RedisError as e:
            LOGGER.error(f"Redis error: {e}")
            return False

    def close_connection(self):
        if self.r:
            self.r.close()
            LOGGER.info("Redis connection closed safely.")