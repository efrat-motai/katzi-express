import time
from src.api.hot_products_api import app, get_hot_product_service


def test_hot_products_api(api_client,integration_service):
    app.dependency_overrides[get_hot_product_service] = lambda: integration_service
    response = api_client.get("/hot_products")
    assert response.status_code == 200


def test_get_hot_products(api_client, redis_client, integration_service):
    app.dependency_overrides[get_hot_product_service] = lambda: integration_service
    current_key = f"hot_products:{int(time.time() // 60) * 60}"
    redis_client.zadd(current_key, {"1": 10, "2": 5})
    response = api_client.get("/hot_products")
    data = response.json()
    assert len(data) == 2
    assert data[0]["product_id"] == 1
    assert data[0]["current_score"] == 10.0
    assert data[1]["product_id"] == 2
