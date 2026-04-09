import httpx

from app.tasks.exceptions import PermanentJobError


async def analyze_page(url: str) -> dict:
    async with httpx.AsyncClient(follow_redirects=True) as client:
        response = await client.get(url)
        response.raise_for_status()

    return {
        "url": url,
        "status_code": response.status_code,
        "elapsed_sec": response.elapsed.total_seconds(),
        "content_length_bytes": len(response.content),
    }


def extract_http_check_url(payload: dict | None) -> str:
    if payload is None:
        raise PermanentJobError("HTTP_CHECK requires payload")
    
    url = payload.get("url")
    if not isinstance(url, str) or not url.strip():
        raise PermanentJobError("HTTP_CHECK requires payload.url")
    
    return url