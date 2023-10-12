
from celery import Celery

app = Celery('poc', broker='redis://localhost:6379/0')
