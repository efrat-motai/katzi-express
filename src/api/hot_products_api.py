import uvicorn
from fastapi import FastAPI
from src.infrastructure.repositories.redis_repository import RedisRepository
from src.services.hot_product_service import HotProductService
from src.utils.config_loader import load_config

app = FastAPI()

@app.get("/hot_products")
async def get_hot_products(count:int = 3):
   return hot_product_service.get_top_products(count=count)

if __name__ == "__main__":
    config = load_config()
    redis_conf = config["redis"]
    redis = RedisRepository(redis_conf["host"], redis_conf["port"], redis_conf["ttl"])
    hot_product_service = HotProductService(redis)
    uvicorn.run(app, host="0.0.0.0", port=8000)