from unittest.mock import MagicMock
import pytest

from src.services.hot_product_service import HotProductService


@pytest.fixture
def mock_redis_repo():
    return MagicMock()

@pytest.fixture
def service(mock_redis_repo):
    return HotProductService(redis_repo=mock_redis_repo)

def test_process_success(service, mock_redis_repo):
    purchase_notification = {"product_id": 1, "product_name": "violin", "product_category": "musical_instrumental", "price": 7000, "customer_id": 101, "quantity": 7, "order_date": "2026-03-17T10:39:07.454259", "order_id": "c3135807-21dc-11f1-abe2-e3e83e2d319b"}