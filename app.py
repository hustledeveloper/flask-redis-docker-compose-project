from flask import Flask, request, jsonify
import psycopg2
import logging
from utils import process_user

# Log settings
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = Flask(__name__)

@app.route('/users', methods=['POST'])
def enqueue_user():
    user_list = request.get_json(force=True)
    if isinstance(user_list, list):
        for user in user_list:
            if isinstance(user, dict):
                process_user(user)
            else:
                logging.error('Invalid user format')
        logging.info('Users enqueued and processed successfully')
        return jsonify({'message': 'Users enqueued and processed successfully'}), 200
    else:
        logging.error('Invalid user list format')
        return jsonify({'error': 'Invalid user list format'}), 400

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
