import json
from dataclasses import asdict
import redis
from src.models.purchase_notification import PurchaseNotification

class RedisRepository:
    def __init__(self):
        self.r = redis.Redis(host='localhost', port=6379, decode_responses=True)

    def add_notification(self, notification: PurchaseNotification) -> bool:
        try:
            payload = json.dumps(asdict(notification), default=lambda o: o.isoformat())
            success= self.r.setex(f"order:{notification.order_id}", 180, payload)
            return success
        except redis.RedisError as e:
            print(f"Redis error: {e}")
            return False
