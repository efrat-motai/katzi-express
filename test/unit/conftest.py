import pytest
from unittest.mock import MagicMock
from src.services.hot_product_service import HotProductService
from src.infrastructure.repositories.product.base_product_repository import BaseProductRepository
from src.infrastructure.repositories.purchase.base_purchase_repository import BasePurchaseRepository

@pytest.fixture
def mock_redis_repo():
    return MagicMock(spec=BasePurchaseRepository)

@pytest.fixture
def mock_products_repo():
    return MagicMock(spec=BaseProductRepository)

@pytest.fixture
def hot_product_service(mock_redis_repo, mock_products_repo):
    return HotProductService(redis_repo=mock_redis_repo, product_repo=mock_products_repo)
