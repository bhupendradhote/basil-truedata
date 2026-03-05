# app/routes/views.py
from fastapi import APIRouter, Request
from fastapi.templating import Jinja2Templates

# Initialize the router for web pages
router = APIRouter()

# Set up templates
templates = Jinja2Templates(directory="app/templates")

@router.get("/")
async def serve_home(request: Request):
    return templates.TemplateResponse("home.html", {"request": request})

@router.get("/strategy")
async def serve_strategy(request: Request):
    return templates.TemplateResponse("strategy.html", {"request": request})

