from redis import Redis
from rq import Queue
import json
from app import process_user

redis_conn = Redis(host='redis', port=6379)
queue = Queue(connection=redis_conn)

def process_file(file_path):
    with open(file_path) as f:
        file_data = json.load(f)
    queue.enqueue(process_user, file_data)

def process_files():
    file_paths = ['users/1.json', 'users/2.json', 'users/3.json', 'users/4.json', 'users/5.json', 'users/6.json', 'users/7.json', 'users/8.json', 'users/9.json', 'users/10.json']
    for file_path in file_paths:
        process_file(file_path)

if __name__ == '__main__':
    process_files()