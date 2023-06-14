from redis import Redis
from rq import Worker, Queue, Connection
import os

listen = ['default']

redis_url = os.getenv('REDIS_URL', 'redis://redis:6379')

if __name__ == '__main__':
    with Connection(Redis.from_url(redis_url)):
        worker = Worker(list(map(Queue, listen)))
        worker.work()
