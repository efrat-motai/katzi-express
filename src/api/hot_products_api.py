import uvicorn
import yaml
from fastapi import FastAPI

from src.infrastructure.repositories.redis_repository import RedisRepository
from src.services.hot_product_service import HotProductService

app = FastAPI()
config = yaml.safe_load(open("../../config/config.yml"))
redis_host = config["redis"]["host"]
redis_port = config["redis"]["port"]
ttl = config["redis"]["ttl"]
redis = RedisRepository(redis_host, redis_port, ttl)
hot_product_service = HotProductService(redis)

@app.get("/hot_products")
async def get_hot_products(count:int = 3):
   return hot_product_service.get_top_products(count=count)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)