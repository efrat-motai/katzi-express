import uvicorn
from fastapi import FastAPI, Depends, Request
from src.setup import bootstrap_service
from contextlib import asynccontextmanager


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.service = bootstrap_service()
    yield
    app.state.service.close_connections()

app = FastAPI(lifespan=lifespan)


def get_hot_product_service(request: Request):
    return request.app.state.service


@app.get("/hot_products")
async def get_hot_products(count: int = 3, service=Depends(get_hot_product_service)):
    return service.get_top_products(count=count)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
