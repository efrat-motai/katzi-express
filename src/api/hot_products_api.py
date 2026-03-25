import uvicorn
from fastapi import FastAPI, Depends, Request
from src.setup import bootstrap_service
from contextlib import asynccontextmanager


@asynccontextmanager
async def lifespan(app: FastAPI):
    service = bootstrap_service()
    if not service:
        error_msg = "Critical: Could not initialize HotProductService. Check Redis connection."
        raise RuntimeError(error_msg)
    app.state.hot_product_service = service
    yield
    app.state.hot_product_service.close_connections()

app = FastAPI(lifespan=lifespan)


def get_hot_product_service(request: Request):
    return request.app.state.hot_product_service


@app.get("/hot_products")
async def get_hot_products(count: int = 3, service=Depends(get_hot_product_service)):
    return service.get_top_products(count=count)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
