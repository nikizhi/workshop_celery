from fastapi import FastAPI

from app.core.settings import settings
from app.routes.job import router as jobs_router

app = FastAPI(title=settings.APP_TITLE)
app.include_router(jobs_router)