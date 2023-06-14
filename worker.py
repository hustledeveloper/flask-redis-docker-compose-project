import os
import json
import redis
import psycopg2
from rq import Worker, Queue, Connection
from utils import process_user

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

listen = ['default']

redis_url = os.getenv('REDIS_URL', 'redis://redis:6379')
conn = redis.from_url(redis_url)

if __name__ == '__main__':
    with Connection(conn):
        worker = Worker(list(map(Queue, listen)))
        logging.info('WWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWW processed: %s', worker.all_keys)
        worker.work(process_user)
