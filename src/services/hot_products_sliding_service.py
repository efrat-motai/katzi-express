import json
import logging
import time
from collections import defaultdict
from datetime import datetime
from src.infrastructure.storage.product.base_product_repository import BaseProductRepository

LOGGER = logging.getLogger(__name__)


class HotProductsSlidingService:

    def __init__(self, redis_repo, product_repo: BaseProductRepository,
                 window_duration_minutes: int = 1):
        self.redis_repository = redis_repo
        self.product_repository = product_repo
        self.window_size_seconds = window_duration_minutes * 60

    def handle_purchase_event(self, purchase_notification) -> bool:
        try:
            purchase_notification = json.loads(purchase_notification)
            if not all(k in purchase_notification for k in ["product_id", "quantity", "purchase_timestamp"]):
                LOGGER.warning("Missing required fields in hot_products notification.")
                return True

            event_time = datetime.fromisoformat(purchase_notification['purchase_timestamp'])
            event_timestamp = event_time.timestamp()
            payload = json.dumps(purchase_notification)
            return self.redis_repository.record_purchase(payload, event_timestamp)
        except json.JSONDecodeError:
            LOGGER.error("Invalid JSON received")
            return True


    