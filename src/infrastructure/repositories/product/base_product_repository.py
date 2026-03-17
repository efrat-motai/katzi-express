from abc import ABC, abstractmethod

class BaseProductRepository(ABC):
    @abstractmethod
    def get_all(self)->list:
        pass