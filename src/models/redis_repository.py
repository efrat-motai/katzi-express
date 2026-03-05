import json
import redis

class RedisRepository:
    def __init__(self, host="localhost", port=6379):
        self.r = redis.Redis(host=host, port=port, decode_responses=True)

    def add_notification(self, notification: dict) -> bool:
        try:
            payload = json.dumps(notification)
            success= self.r.setex(f"order:{notification['order_id']}", 180, payload)
            return success
        except redis.RedisError as e:
            print(f"Redis error: {e}")
            return False
        finally:
            self.r.close()