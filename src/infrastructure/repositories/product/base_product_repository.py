from abc import ABC, abstractmethod


class BaseProductRepository(ABC):

    @abstractmethod
    def get_by_id(self, product_id: int) -> dict:
        pass
