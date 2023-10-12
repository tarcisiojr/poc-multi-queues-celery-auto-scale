import datetime
import threading
import time
import uuid

from app import repository
from app.domain import Worker, Queue


def _get_pending_tasks_by_queue():
    queues = repository.get_working_queues()

    def _sort_by_rate_and_priority(elem: Queue):
        return elem.ratio, -elem.priority

    queues.sort(key=_sort_by_rate_and_priority)
    return queues


def select_queue_for_worker():
    queues = _get_pending_tasks_by_queue()

    for queue in queues:
        if queue.workers_count >= queue.max_workers:
            print(f'Ignorando queue, pois o limite de filas já foi atingido: {queue.name}')
            continue

        if repository.count_pending_tasks_of_queue(queue.name) == 0:
            print(f'Ignorando queue, pois não tem tarefa pendente: {queue.name}')
            continue

        print(f'Selecionando fila: {queue.name}')
        return queue

    print(f'Não foi possível iniciar nenhuma fila! Nenhum trabalho a ser feito.')
    return None


def create_worker():
    pass


def register_worker(queue: Queue):
    worker = Worker(
        id=str(uuid.uuid4()),
        queue_name=queue.name,
        started_at=datetime.datetime.utcnow(),
        last_ping_at=datetime.datetime.utcnow(),
    )
    repository.save_worker(worker)
    return worker


def start_keep_alive_worker(worker: Worker, stop_worker_callback):
    def _update_keep_alive():
        while True:
            print(f'Atualizando Keep Alive para: {worker.id}/{worker.queue_name}')
            repository.update_last_ping_at_of_worker(worker.id, datetime.datetime.utcnow())
            time.sleep(10)

            count = repository.count_pending_tasks_of_queue(worker.queue_name)
            print(f'Total de tarefas para a fila {worker.queue_name}: {count}')

            if repository.count_pending_tasks_of_queue(worker.queue_name) == 0:
                print('Forçando para do worker por falta de tarefas pendentes')
                repository.refresh_worker_count(worker.queue_name, 1)
                stop_worker_callback()
                break

    thread = threading.Thread(target=_update_keep_alive)
    thread.start()


def start_keep_workers_count_updated():
    def _keep_refreshed():
        while True:
            print('Atualizando worker das filas')
            queues = repository.get_all_queues()
            for queue in queues:
                count = repository.refresh_worker_count(queue.name)
                print(f'Atualizado: {queue.name} -> {count}')

            time.sleep(30)

    thread = threading.Thread(target=_keep_refreshed)
    thread.start()

