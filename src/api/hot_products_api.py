import uvicorn
from fastapi import FastAPI

from src.infrastructure.repositories.product.mock_product_repository import MockProductRepository
from src.infrastructure.repositories.purchase.redis_purchase_repository import RedisPurchaseRepository
from src.services.hot_product_service import HotProductService
from src.utils.config_loader import load_config

app = FastAPI()

@app.get("/hot_products")
async def get_hot_products(count:int = 3):
   return hot_product_service.get_top_products(count=count)

if __name__ == "__main__":
    config = load_config()
    redis_conf = config["redis"]
    redis = RedisPurchaseRepository(redis_conf["host"], redis_conf["port"], redis_conf["ttl"])
    hot_product_service = HotProductService(redis, MockProductRepository())
    uvicorn.run(app, host="0.0.0.0", port=8000)