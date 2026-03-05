# main.py
from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
import uvicorn
from app.api.routes import router as api_router

# Initialize the FastAPI app
app = FastAPI(title="TrueData Market API")

templates = Jinja2Templates(directory="app/templates")

app.include_router(api_router, prefix="/api")

@app.get("/")
async def serve_home(request: Request):
    return templates.TemplateResponse("home.html", {"request": request})

@app.get("/strategy")
async def serve_strategy(request: Request):
    return templates.TemplateResponse("strategy.html", {"request": request})

def main():
    print("🚀 Starting Server on http://127.0.0.1:800")
    uvicorn.run("main:app", host="127.0.0.1", port=8001, reload=True)

if __name__ == "__main__":
    main()