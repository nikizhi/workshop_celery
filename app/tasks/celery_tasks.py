import asyncio
from datetime import datetime, UTC
from uuid import UUID

import httpx
from loguru import logger

from app.core.celery_app import celery_app
from app.core.database import AsyncSessionLocal
from app.models.job import JobStatus
from app.repositories.job import get_job, update_job_state
from app.tasks.exceptions import PermanentJobError
from app.tasks.executor import execute_job


async def _mark_job_failed(job_id: UUID, error: str):
    async with AsyncSessionLocal() as session:
        job = await get_job(session=session, job_id=job_id)
        if job is None:
            return
        
        await update_job_state(
            session=session,
            job=job,
            status=JobStatus.FAILED,
            finished_at=datetime.now(UTC),
            error=error
        )

        logger.error("Set status -> FAILED")


@celery_app.task(name="run_job_task", bind=True, max_retries=3)
def run_job_task(self, job_id: str):
    try:
        asyncio.run(execute_job(UUID(job_id)))
    
    except PermanentJobError as e:
        logger.error(f"Permanent error for job_id={job_id}: {e}")
        asyncio.run(_mark_job_failed(job_id=UUID(job_id), error=str(e)))
        return
    
    except (httpx.RequestError, httpx.HTTPStatusError) as e:
        logger.warning(
            f"Retryable error for job_id={job_id}. "
            f"Attempt #{self.request.retries + 1}/{self.max_retries + 1}. "
        )

        if self.request.retries >= self.max_retries:
            asyncio.run(_mark_job_failed(job_id=UUID(job_id), error=str(e)))
            raise

        raise self.retry(exc=e, countdown=2**(self.request.retries + 1))