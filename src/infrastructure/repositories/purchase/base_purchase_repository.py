from abc import ABC, abstractmethod


class BasePurchaseRepository(ABC):

    @abstractmethod
    def increment_product_count(self, key: str, product_id: int) -> bool:
        pass

    @abstractmethod
    def get_hot_products(self, key: str, count: int) -> list:
        pass
