import json
from datetime import datetime, UTC
from uuid import UUID

from loguru import logger

from app.core.database import AsyncSessionLocal
from app.models.job import JobStatus, JobType
from app.repositories.job import get_job, update_job_state
from app.tasks.exceptions import PermanentJobError
from app.tasks.handlers.http_check import analyze_page, extract_http_check_url
from app.tasks.handlers.word_stats import analyze_word_stats, extract_word_stats

from app.tasks.handlers.word_stats_compare import extract_compare_payload, analyze_word_stats_compare


async def execute_job(job_id: UUID) -> None:
    log = logger.bind(job_id=str(job_id), task="execute_job")
    log.info("Celery job started")

    async with AsyncSessionLocal() as session:
        job = await get_job(session=session, job_id=job_id)
        if job is None:
            log.warning(f"Job {job_id} not found in DB, nothing to do")
            return
        
        log.info(f"Running job: {job.title}")

        payload = json.loads(job.payload) if job.payload is not None else None

        await update_job_state(session=session, job=job, status=JobStatus.PROCESSING)
        log.info("Set status -> PROCESSING")
        if job.job_type == JobType.HTTP_CHECK.value:
            url = extract_http_check_url(payload=payload)
            result = await analyze_page(url=url)
        elif job.job_type == JobType.WORD_STATS.value:
            url, top_n = extract_word_stats(payload=payload)
            result = await analyze_word_stats(url=url, top_n=top_n)
        elif job.job_type == JobType.WORD_STATS_COMPARE.value:
            left_id, right_id = extract_compare_payload(payload=payload)
            result = await analyze_word_stats_compare(session=session, left_id=left_id, right_id=right_id)
        else:
            raise PermanentJobError(f"Unsupported job_type: {job.job_type}")
            
        await update_job_state(
            session=session,
            job=job,
            status=JobStatus.DONE,
            finished_at=datetime.now(UTC),
            result=result
        )
        log.success("Set status -> DONE")