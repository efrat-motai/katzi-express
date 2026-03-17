import uvicorn
from fastapi import FastAPI
from src.setup import bootstrap_service

app = FastAPI()

@app.get("/hot_products")
async def get_hot_products(count: int = 3):
    return hot_product_service.get_top_products(count=count)


if __name__ == "__main__":
    hot_product_service = bootstrap_service()
    uvicorn.run(app, host="0.0.0.0", port=8000)
