import json
import logging
import time

from src.utils.data.mock_data import products

LOGGER = logging.getLogger(__name__)


class HotProductService:

    def __init__(self, redis_repo, window_duration_minutes: int = 1):
        self.redis = redis_repo
        self.window_size_seconds = window_duration_minutes * 60

    def process(self, purchase_notification: str) -> bool:
        try:
            purchase_notification = json.loads(purchase_notification)
            product_id = purchase_notification['product_id']
            window_timestamp = int(
                time.time() // self.window_size_seconds) * self.window_size_seconds
            key = f"hot_products:{window_timestamp}"
            return self.redis.increment_product_count(key, product_id)
        except json.JSONDecodeError:
            LOGGER.error("Invalid JSON received")
            return True

    def get_top_products(self, count) -> list:
        window_timestamp = int(time.time() // self.window_size_seconds) * self.window_size_seconds
        key = f"hot_products:{window_timestamp}"
        row_result = self.redis.get_hot_products(key=key, count=count)

        if not row_result:
            key = f"hot_products:{window_timestamp - self.window_size_seconds // 60}"
            row_result = self.redis.get_hot_products(key=key, count=3)

        products_mock = {str(p["product_id"]): p for p in products}
        full_details = []

        for product_id, score in row_result:
            product_info = products_mock.get(product_id)

            if product_info:
                result = product_info.copy()
                result["current_score"] = int(score)
                full_details.append(result)

        return full_details

    def close_connections(self) -> None:
        self.redis.close_connection()
