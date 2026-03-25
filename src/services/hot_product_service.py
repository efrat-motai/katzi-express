import json
import logging
import time
from datetime import datetime
from src.infrastructure.repositories.product.base_product_repository import BaseProductRepository
from src.infrastructure.repositories.purchase.base_purchase_repository import BasePurchaseRepository
from src.utils.types.redis_keys_prefix import KeysPrefix

LOGGER = logging.getLogger(__name__)


class HotProductService:

    def __init__(self, redis_repo: BasePurchaseRepository, product_repo: BaseProductRepository,
                 window_duration_minutes: int = 1):
        self.redis_repository = redis_repo
        self.product_repository = product_repo
        self.window_size_seconds = window_duration_minutes * 60

    def handle_purchase_event(self, purchase_notification: str) -> bool:
        try:
            purchase_notification = json.loads(purchase_notification)
            if not all(k in purchase_notification for k in ["product_id", "quantity", "purchase_timestamp"]):
                LOGGER.warning("Missing required fields in purchase notification.")
                return True

            event_time = datetime.fromisoformat(purchase_notification['purchase_timestamp'])
            event_timestamp = event_time.timestamp()
            return self._update_product_score(product_id=purchase_notification['product_id'],
                                              quantity=purchase_notification['quantity'],
                                              event_timestamp=event_timestamp)
        except json.JSONDecodeError:
            LOGGER.error("Invalid JSON received")
            return True

    def _update_product_score(self, product_id: int, quantity: int, event_timestamp: float) -> bool:
        window_timestamp = self._calculate_window_start(event_timestamp)
        key = f"{KeysPrefix.HOT_PRODUCTS.value}:{window_timestamp}"
        return self.redis_repository.increment_product_count(key=key, product_id=product_id, amount=quantity)

    def _calculate_window_start(self, timestamp: float) -> int:
        return int(
            timestamp // self.window_size_seconds) * self.window_size_seconds

    def get_top_products(self, count: int) -> list:
        window_timestamp = self._calculate_window_start(time.time())
        key = f"{KeysPrefix.HOT_PRODUCTS.value}:{window_timestamp}"
        row_result = self.redis_repository.get_hot_products(key=key, count=count)

        if not row_result:
            key = f"{KeysPrefix.HOT_PRODUCTS.value}:{window_timestamp - self.window_size_seconds}"
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
