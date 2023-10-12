import time

from . import repository
from .celery_config import app
from .domain import TaskStatus


@app.task
def do_task(task_id: str):
    print(f'Executando tarefa: {task_id}')

    if not task_id:
        print('Task ID NONE!!!!')
        return

    task = repository.update_status_task(task_id, TaskStatus.COMPLETED)
    print(f'Tarefa completada ({task.queue_name}): {task}')

    time.sleep(50)
