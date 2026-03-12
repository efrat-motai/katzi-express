import uvicorn
from fastapi import FastAPI

app = FastAPI()


@app.get("/hot_products/")
def read_item(item_id: int, q: str | None = None):
    return {"item_id": item_id, "q": q}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)