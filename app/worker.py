import subprocess
import sys

from app import scaling_rule


def start():
    queue = scaling_rule.select_queue_for_worker()

    if not queue:
        return

    worker = scaling_rule.register_worker(queue)

    command = f"celery -A app.task worker --loglevel=info --concurrency=3 -Q {queue.name}"

    try:
        print(f'Comando: {command}')
        process = subprocess.Popen(command.split(' '))

        def _stop_process():
            process.terminate()

        scaling_rule.start_keep_alive_worker(worker, _stop_process)

        process.wait()
    except subprocess.CalledProcessError as e:
        print(f"Erro ao executar o comando: {e}")
    except KeyboardInterrupt:
        print("Comando interrompido pelo usuário.")


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == 'refresh':
        scaling_rule.start_keep_workers_count_updated()
    else:
        start()
