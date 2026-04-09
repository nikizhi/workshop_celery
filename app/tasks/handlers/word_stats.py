import re
from collections import Counter

import httpx

from app.models.job import JobType, JobStatus
from app.tasks.exceptions import PermanentJobError
from bs4 import BeautifulSoup

async def analyze_word_stats(url: str, top_n: int) -> dict:
    async with httpx.AsyncClient(follow_redirects=True) as client:
        response = await client.get(url)
        response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    text = soup.get_text(separator=" ", strip=True).lower()
    words = re.findall(pattern=r"[a-zа-яё]{3,}", string=text)
    counts = Counter(words)
    return {
        "url": url,
        "total_words": len(words),
        "unique_words": len(counts),
        "top_n": top_n,
        "top_words": dict(counts.most_common(top_n)),
    }


def extract_word_stats(payload: dict | None) -> tuple[str, int]:
    if payload is None:
        raise PermanentJobError("WORD_STATS requires payload")

    url = payload.get("url")
    top_n = payload.get("top_n")
    if not isinstance(url, str) or not url.strip():
        raise PermanentJobError("WORD_STATS requires payload.url")
    if not isinstance(top_n, int) or not(1 <= top_n <= 20):
        raise PermanentJobError("WORD_STATS requires payload.top_n")

    return url, top_n