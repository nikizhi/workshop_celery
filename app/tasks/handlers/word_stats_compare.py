import uuid
import json
from sqlalchemy.ext.asyncio import AsyncSession
from app.models.job import JobType, JobStatus
from app.tasks.exceptions import PermanentJobError
from app.repositories.job import get_job

def extract_compare_payload(payload: dict | None) -> tuple[uuid.UUID, uuid.UUID]:
    if payload is None:
        raise PermanentJobError("WORD_STATS_COMPARE requires payload")

    left_id = payload.get("left_job_id")
    right_id = payload.get("right_job_id")

    if not left_id or not right_id:
        raise PermanentJobError("WORD_STATS_COMPARE requires left_job_id and right_job_id")

    try:
        left_uuid = uuid.UUID(str(left_id))
        right_uuid = uuid.UUID(str(right_id))
    except (ValueError, TypeError):
        raise PermanentJobError("left_job_id and right_job_id must be passed as UUID")

    if left_uuid == right_uuid:
        raise PermanentJobError("left_job_id and right_job_id must be different")

    return left_uuid, right_uuid


async def analyze_word_stats_compare(session: AsyncSession, left_id: uuid.UUID, right_id: uuid.UUID) -> dict:
    sources = []
    for j_id, side in [(left_id, "left"), (right_id, "right")]:
        job = await get_job(session, j_id)
        if job is None:
            raise PermanentJobError(f"{side.capitalize()} source job not found")
        if job.job_type != JobType.WORD_STATS:
            raise PermanentJobError(f"{side.capitalize()} job must be WORD_STATS")
        if job.status != JobStatus.DONE:
            raise PermanentJobError(f"{side.capitalize()} job is not DONE")

        res_data = json.loads(job.result) if isinstance(job.result, str) else job.result
        if not isinstance(res_data, dict) or "top_words" not in res_data:
            raise PermanentJobError(f"Field result of {side} job must be a dict with top_words")

        sources.append((job, res_data))

    (left_job, left_res), (right_job, right_res) = sources

    left_words = set(left_res["top_words"].keys())
    right_words = set(right_res["top_words"].keys())

    return {
        "left_job_id": str(left_job.id),
        "right_job_id": str(right_job.id),
        "left_url": left_res.get("url"),
        "right_url": right_res.get("url"),
        "common_words": sorted(list(left_words & right_words)),
        "left_only": sorted(list(left_words - right_words)),
        "right_only": sorted(list(right_words - left_words)),
    }
