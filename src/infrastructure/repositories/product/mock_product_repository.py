from src.infrastructure.repositories.product.base_product_repository import BaseProductRepository
from src.utils.data.mock_data import PRODUCTS


class MockProductRepository(BaseProductRepository):
    def get_all(self) -> list:
        return PRODUCTS

    def get_by_id(self, product_id: str) -> dict:
        product = [p for p in PRODUCTS if str(p['product_id']) == product_id]
        return product[0] if product else None
