from fastapi import APIRouter, Query, HTTPException, status, Body, Header, Depends
from fastapi.responses import FileResponse, HTMLResponse
from sqlalchemy.ext.asyncio import AsyncSession


router = APIRouter(prefix="/settings", tags=["Keywords"])


@router.get("/stream")
async def get_stream():
    return 1
