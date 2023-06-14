from flask import Flask, request, jsonify
from flask_redis import FlaskRedis
from rq import Queue
import os
import json
import requests
import psycopg2

app = Flask(__name__)
app.config['REDIS_URL'] = 'redis://redis:6379/0'
redis_store = FlaskRedis(app)
queue = Queue(connection=redis_store)

def process_user(user):

    conn = psycopg2.connect(
        host='db',
        port=5432,
        user='postgres',
        password='postgres',
        database='postgres'
    )
    try:

        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO users (id, first_name, last_name, email, gender, ip_address, user_name, agent, country)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            user['id'],
            user['first_name'],
            user['last_name'],
            user['email'],
            user['gender'],
            user['ip_address'],
            user['user_name'],
            user['agent'],
            user['country']
        ))
        conn.commit()
        print(f"User {user['id']} inserted into the database successfully")
    except Exception as e:
        print(f"Failed to insert user {user['id']} into the database:", str(e))
    finally:
        conn.close()

@app.route('/users', methods=['POST'])
def receive_user():
    user = request.get_json()
    queue.enqueue(process_user, user)
    return jsonify({'message': 'User received and enqueued for processing'})

def send_users_to_server(users):
    for filename in os.listdir(users):
        if filename.endswith('.json'):
            file_path = os.path.join(users, filename)
            with open(file_path) as file:
                users = json.load(file)
                for user in users:
                    response = requests.post('http://localhost:5000/users', json=user)
                    if response.status_code == 201:
                        print('User sent to the server successfully')
                    else:
                        print('Failed to send user to the server')

send_users_to_server('users')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
