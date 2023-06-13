from flask import Flask, request, jsonify
from rq import Queue
from redis import Redis
import logging
from utils import process_user

# Log settings
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = Flask(__name__)
redis_conn = Redis(host='redis', port=6379)
queue = Queue(connection=redis_conn)

@app.route('/users', methods=['POST'])
def enqueue_user():
    user = request.get_json()
    queue.enqueue(process_user, user)
    logging.info('User enqueued successfully')
    return jsonify({'message': 'User enqueued successfully'}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)