FROM python:3.14-slim

WORKDIR /app

ENV PYTHONDONTWRYTEBYTECODE=1

ENV PYTHONUNBUFFERED=1

RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

ENTRYPOINT ["celery", "-A", "app.core.celery_app:celery_app", "worker", "--loglevel=info"]
CMD ["-Q", "default", "--hostname=default@%h", "-c", "1"]