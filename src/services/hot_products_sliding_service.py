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


    def get_top_products(self, count: int) -> list:
        window_timestamp = time.time() - self.window_size_seconds
        row_result = self.redis_repository.get_hot_products(start_window= window_timestamp)
        hot_products = defaultdict(int)

        for product in row_result:
            p = json.loads(product)
            hot_products[p["product_id"]] += p["quantity"]

        sorted_hot_products = sorted(hot_products.items(), key=lambda x: x[1], reverse=True)
        full_details = []
        for product_id, score in sorted_hot_products[:count]:
            product_info = self.product_repository.get_by_id(product_id)
            if product_info:
                result = product_info.copy()
                result["current_score"] = int(score)
                full_details.append(result)

        return full_details

   
