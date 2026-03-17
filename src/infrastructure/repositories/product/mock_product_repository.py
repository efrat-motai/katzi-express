from src.infrastructure.repositories.product.base_product_repository import BaseProductRepository
from src.utils.data.mock_data import PRODUCTS


class MockProductRepository(BaseProductRepository):
    def get_all(self) -> list:
        return PRODUCTS