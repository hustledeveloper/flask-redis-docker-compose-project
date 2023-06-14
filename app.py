from flask import Flask, request
import json
from redis import Redis

app = Flask(__name__)
redis = Redis(host='redis', port=6379)

@app.route('/users', methods=['POST'])
def receive_user_data():
    user_data = request.get_json()
    json_data = json.dumps(user_data)
    redis.rpush('user_queue', json_data.encode())
    return 'User data received and added to the queue.'

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
