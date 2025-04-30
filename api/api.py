from fastapi import APIRouter
from api.routes.ercot import router as ercot_router
from api.routes.modelling import router as model_router

api_router = APIRouter()

api_router.include_router(ercot_router, prefix="/ercot", tags=["ERCOT"])
api_router.include_router(model_router, prefix="/model", tags=["MODEL"])
