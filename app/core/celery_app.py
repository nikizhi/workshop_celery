from celery import Celery
from kombu import Queue

from app.core.settings import settings

celery_app = Celery(main="jobs", broker=settings.redis_url)
celery_app.conf.update(
    imports="app.tasks.celery_tasks",
    task_default_queue="default",
    task_queues=(
        Queue("default"),
        Queue("priority")
    ),
    broker_transport_options={
        "queue_order_strategy": "priority",
        "priority_steps": list(range(10)),
        "sep": ":"
    },
    worker_prefetch_multiplier=1
)