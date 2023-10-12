import datetime
from typing import List

from bson import ObjectId
from pymongo import MongoClient, ReturnDocument

from app.domain import Task, TasksCountByQueueName, Queue, Worker, TaskStatus

client = MongoClient("localhost", 27017)
_database = client.poc


def count_pending_tasks_by_queue() -> List[TasksCountByQueueName]:
    cursor = _database.tasks.aggregate([
        {'$match': {'status': 'pending'}},
        {
            '$group': {
                '_id': "$queue_name",
                'count': {'$sum': 1}
            }
        },
        {
            '$lookup': {
                'from': 'queues',
                'localField': '_id',
                'foreignField': 'name',
                'as': 'queue'
            }
        },
        {
            '$unwind': {
                'path': '$queue',
                'preserveNullAndEmptyArrays': False
            }
        }
    ])

    return [TasksCountByQueueName(**row,
                                  queue_name=row.get('_id'))
            for row in cursor]


def count_live_workers_by_queue():
    cursor = _database.workers.aggregate([
        {
            '$group': {
                '_id': "$queue_name",
                'count': {'$sum': 1}
            },
        },
        {
            '$addFields': {'queue_name': '$_id'}
        }
    ])

    return cursor


def get_working_queues():
    cursor = _database.queues.find({'$expr': {'$lt': ['$workers_count', '$max_workers']}})

    return [Queue(**row) for row in cursor]


def get_all_queues():
    cursor = _database.queues.find({})

    return [Queue(**row) for row in cursor]


def save_task(task: Task):
    ret = _database.tasks.insert_one(task.model_dump(exclude={'id'}))
    doc = _database.tasks.find_one({'_id': ret.inserted_id})
    return Task(id=str(doc.pop('_id')), **doc)


def update_status_task(task_id: str, status: TaskStatus):
    doc = _database.tasks.find_one_and_update({'_id': ObjectId(task_id)},
                                              {'$set': {'status': status}},
                                              return_document=ReturnDocument.AFTER)
    return Task(id=str(doc.pop('_id')), **doc)


def save_queue(queue: Queue):
    _database.queues.replace_one({'name': queue.name},
                                 queue.model_dump(),
                                 upsert=True)


def refresh_worker_count(queue_name: str, decrement: int = 0):
    count = _database.workers.count_documents({'queue_name': queue_name})

    update_worker_count_of_queue(queue_name, max(count - decrement, 0))
    return count


def update_worker_count_of_queue(queue_name: str, count: int = 0):
    _database.queues.update_one({'name': queue_name},
                                {'$set': {'workers_count': count}})


def save_worker(worker: Worker):
    _database.workers.insert_one(worker.model_dump())

    refresh_worker_count(worker.queue_name)


def update_last_ping_at_of_worker(worker_id: str, last_ping: datetime.datetime):
    _database.workers.update_one({'id': worker_id},
                                 {'$set': {'last_ping_at': last_ping}})


def count_pending_tasks_of_queue(queue_name: str):
    return _database.tasks.count_documents({'queue_name': queue_name,
                                            'status': TaskStatus.PENDING})


if __name__ == '__main__':
    print(count_pending_tasks_by_queue())
    save_queue(Queue(name='default',
                     priority=100,
                     min_workers=1,
                     max_workers=3,
                     workers_count=0))

    save_queue(Queue(name='fila2',
                     priority=200,
                     min_workers=1,
                     max_workers=1,
                     workers_count=0))
