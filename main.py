# main.py
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
import uvicorn

from app.api.routes import router as api_router
from app.routes.views import router as web_router  
from app.config.db import get_db_connection

# Define startup and shutdown events
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("🔄 Checking XAMPP MySQL database connection on startup...")
    db = get_db_connection()
    if db:
        print("✅ Database connection verified successfully!")
        db.close()
    else:
        print("⚠️ Warning: Could not connect to the database. Check if XAMPP is running.")
    
    yield # The application runs here
    
    print("🛑 Shutting down server...")


app = FastAPI(title="TrueData Market API", lifespan=lifespan)

# Include the API routes
app.include_router(api_router, prefix="/api")

# Include the Web/UI routes (no prefix needed for root paths)
app.include_router(web_router)

def main():
    # Fixed the port to match 8001 exactly
    print("🚀 Starting Server on http://127.0.0.1:8001")
    uvicorn.run("main:app", host="127.0.0.1", port=8001, reload=True)

if __name__ == "__main__":
    main()