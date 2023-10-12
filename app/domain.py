import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel


class Queue(BaseModel):
    name: str
    priority: int
    min_workers: int = 0
    max_workers: int = 1
    workers_count: int = 0

    @property
    def ratio(self):
        return self.workers_count / self.max_workers


class Worker(BaseModel):
    id: str
    queue_name: str
    started_at: datetime.datetime
    last_ping_at: datetime.datetime


class TaskStatus(str, Enum):
    PENDING = 'pending'
    COMPLETED = 'completed'


class Task(BaseModel):
    id: Optional[str] = None
    status: TaskStatus
    queue_name: str


class TasksCountByQueueName(BaseModel):
    queue_name: str
    count: int
    queue: Queue | None

    @property
    def ratio(self):
        return self.queue.workers_count / self.queue.max_workers
