from fastapi import FastAPI
from routes_auth import router as auth_router

app = FastAPI()

app.include_router(auth_router)

@app.get("/")
def read_root():
    return {"message": "Welcome to the FastAPI server!"}
