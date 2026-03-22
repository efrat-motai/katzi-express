import json
import logging
import time
from src.infrastructure.repositories.product.base_product_repository import BaseProductRepository
from src.infrastructure.repositories.purchase.base_purchase_repository import BasePurchaseRepository

LOGGER = logging.getLogger(__name__)


class HotProductService:

    def __init__(self, redis_repo: BasePurchaseRepository, product_repo: BaseProductRepository,
                 window_duration_minutes: int = 1):
        self.redis_repository = redis_repo
        self.product_repository = product_repo
        self.window_size_seconds = window_duration_minutes * 60

    def process(self, purchase_notification: str) -> bool:
        try:
            purchase_notification = json.loads(purchase_notification)
            product_id = purchase_notification['product_id']
            quantity = purchase_notification['quantity']
            if not product_id or not quantity:
                LOGGER.warning("An object without required fields was received.")
                return True
            window_timestamp = int(
                time.time() // self.window_size_seconds) * self.window_size_seconds
            key = f"hot_products:{window_timestamp}"
            return self.redis_repository.increment_product_count(key=key, product_id=product_id, amount=quantity)
        except json.JSONDecodeError:
            LOGGER.error("Invalid JSON received")
            return True

    def get_top_products(self, count) -> list:
        window_timestamp = int(time.time() // self.window_size_seconds) * self.window_size_seconds
        key = f"hot_products:{window_timestamp}"
        row_result = self.redis_repository.get_hot_products(key=key, count=count)

        if not row_result:
            key = f"hot_products:{window_timestamp - self.window_size_seconds}"
            row_result = self.redis_repository.get_hot_products(key=key, count=3)

        full_details = []

        for product_id, score in row_result:
            product_info = self.product_repository.get_by_id(product_id)
            if product_info:
                result = product_info.copy()
                result["current_score"] = int(score)
                full_details.append(result)

        return full_details

    def close_connections(self) -> None:
        self.redis_repository.close_connection()
