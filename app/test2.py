import threading
import time

from app import repository
from app.domain import Task, TaskStatus
from app.task import do_task

if __name__ == '__main__':
    def _create_task():
        while True:
            print('Criando tarefa')
            task = Task(queue_name='fila2', status=TaskStatus.PENDING)
            task = repository.save_task(task)
            do_task.apply_async([task.id], queue=task.queue_name)
            print(f'Task enviada para execução: {task.id}')

            time.sleep(10)

    thread = threading.Thread(target=_create_task)
    thread.start()
