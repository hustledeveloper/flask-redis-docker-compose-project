from flask import Flask, request, jsonify
import logging
from rq import Queue
from redis import Redis
from utils import enqueue_user_to_redis

# Log settings
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = Flask(__name__)
redis_conn = Redis(host='redis', port=6379)
queue = Queue(connection=redis_conn)

@app.route('/users', methods=['POST'])
def enqueue_user():
    user_list = request.get_json(force=True)
    if isinstance(user_list, list):
        for user in user_list:
            if isinstance(user, dict):
                enqueue_user_to_redis(user)
                logging.info('AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA processed: %s', user)

            else:
                logging.error('Invalid user format')
        logging.info('AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA enqueued successfully')
        return jsonify({'message': 'Users enqueued successfully'}), 200
    else:
        logging.error('Invalid user list format')
        return jsonify({'error': 'Invalid user list format'}), 400

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
