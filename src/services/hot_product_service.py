import json
import time

from src.data.mock_data import products


class HotProductService:
    def __init__(self, redis_repo):
        self.redis = redis_repo

    def process(self, purchase_notification: str) -> bool:
        purchase_notification = json.loads(purchase_notification)
        product_id = purchase_notification['product_id']
        window_timestamp = int(time.time() // 60) * 60
        key = f"hot_products:{window_timestamp}"
        success = self.redis.increment_product_count(key, product_id)
        return success

    def get_top_products(self, count) -> list:
        window_timestamp = int(time.time() // 60) * 60
        key = f"hot_products:{window_timestamp}"
        row_result = self.redis.get_hot_products(key=key, count=count)

        if not row_result:
            key = f"hot_products:{window_timestamp - 60}"
            row_result = self.redis.get_hot_products(key=key, count=3)

        products_mock = {str(p["product_id"]): p for p in products}
        full_details = []

        for product_id, score in row_result:
            product_info = products_mock.get(product_id)

            if product_info:
                result = product_info.copy()
                result["current_score"] = int(score)
                full_details.append(result)
            #full_details.append(products_mock.get(product_id))

        return full_details

    def close_connections(self) -> None:
        self.redis.close_connection()
