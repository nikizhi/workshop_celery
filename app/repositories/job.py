import json
from datetime import datetime
from uuid import UUID

from sqlmodel import select, func
from sqlmodel.ext.asyncio.session import AsyncSession

from app.models.job import JobDB, JobStatus, JobCreate, JobsSummaryOut


async def create_job(session: AsyncSession, data: JobCreate) -> JobDB:
    payload_json = None
    if data.payload is not None:
        payload_json = json.dumps(data.payload, ensure_ascii=False)

    job = JobDB(
        title=data.title,
        job_type=data.job_type.value,
        payload=payload_json
    )

    session.add(job)
    await session.commit()
    await session.refresh(job)
    return job


async def get_job(session: AsyncSession, job_id: UUID) -> JobDB | None:
    return await session.get(JobDB, job_id)


async def get_jobs_summary(session: AsyncSession) -> JobsSummaryOut:
    stmt = select(
        func.count().filter(JobDB.status == JobStatus.PENDING),
        func.count().filter(JobDB.status == JobStatus.PROCESSING),
        func.count().filter(JobDB.status == JobStatus.DONE),
        func.count().filter(JobDB.status == JobStatus.FAILED)
    )

    result = await session.exec(stmt)
    pending, processing, done, failed = result.one()
    
    return JobsSummaryOut(
        pending=pending,
        processing=processing,
        done=done,
        failed=failed
    )


async def update_job_state(
    session: AsyncSession,
    job: JobDB,
    status: JobStatus,
    finished_at: datetime | None = None,
    error: str | None = None,
    result: dict | None = None
) -> JobDB:
    job.status = status

    if finished_at is not None:
        job.finished_at = finished_at
    if error is not None:
        job.error = error
    if result is not None:
        job.result = json.dumps(result, ensure_ascii=False)

    session.add(job)
    await session.commit()
    await session.refresh(job)
    return job