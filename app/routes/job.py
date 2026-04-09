import json
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query
from loguru import logger

from app.core.settings import settings
from app.core.redis import redis_client
from app.core.database import SessionDep
from app.models.job import JobOut, JobCreate, JobsSummaryOut, JobDB, JobType, QueueName
from app.repositories import job as job_repo
from app.tasks.celery_tasks import run_job_task

router = APIRouter(prefix="/jobs", tags=["Jobs"])

JOBS_SUMMARY_CACHE_KEY = "jobs:summary"


def _build_job_out(job: JobDB) -> JobOut:
    payload = json.loads(job.payload) if job.payload is not None else None
    result = json.loads(job.result) if job.result is not None else None

    return JobOut(
        id=job.id,
        title=job.title,
        job_type=JobType(job.job_type),
        status=job.status,
        created_at=job.created_at,
        finished_at=job.finished_at,
        error=job.error,
        payload=payload,
        result=result
    )


@router.post("", response_model=JobOut)
async def create_job(
    payload: JobCreate,
    session: SessionDep,
    queue_name: QueueName = Query(default=QueueName.DEFAULT),
    priority: int = Query(default=5, ge=0, le=9)
) -> JobOut:
    job = await job_repo.create_job(session, payload)

    await redis_client.delete(JOBS_SUMMARY_CACHE_KEY)

    logger.info(
        f"Scheduling background task for job_id={job.id}, "
        f"title={job.title}, "
        f"queue={queue_name.value}, priority={priority}"
    )

    run_job_task.apply_async(
        args=[str(job.id)], 
        queue=queue_name.value,
        priority=priority
    )

    return _build_job_out(job)


@router.get("/summary", response_model=JobsSummaryOut)
async def get_jobs_summary(session: SessionDep) -> JobsSummaryOut:
    cached_summary = await redis_client.get(JOBS_SUMMARY_CACHE_KEY)

    if cached_summary is not None:
        logger.info("jobs summary returned from Redis cache")
        cached_data = json.loads(cached_summary)
        return JobsSummaryOut(**cached_data)
    
    logger.info("jobs summary returned from PostgreSQL")
    summary = await job_repo.get_jobs_summary(session=session)
    summary_json = json.dumps(summary.model_dump())
    await redis_client.set(JOBS_SUMMARY_CACHE_KEY, summary_json)
    await redis_client.expire(JOBS_SUMMARY_CACHE_KEY, settings.REDIS_CACHE_TTL)

    return summary


@router.get("/{job_id}", response_model=JobOut)
async def get_job(
    job_id: UUID,
    session: SessionDep,
) -> JobOut:
    job = await job_repo.get_job(session, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")

    return _build_job_out(job)