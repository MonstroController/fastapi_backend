__all__ = ("monstro",)

from fastapi import APIRouter
from .stats.views import router as stats_router
from .results.views import router as results_router
from .keywords.view import router as keywords_router
from .profiles.view import router as profiles_router

monstro = APIRouter(prefix="/monstro")
monstro.include_router(router=results_router)
monstro.include_router(router=keywords_router)
monstro.include_router(router=profiles_router)
monstro.include_router(router=stats_router)
