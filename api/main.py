from dotenv import load_dotenv
from fastapi import FastAPI

from api.api import api_router

# Load environment variables from .env file
load_dotenv()

app = FastAPI()

app.include_router(api_router)