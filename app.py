from flask import Flask, request
from redis import Redis
from rq import Queue
from datetime import datetime
import json
import psycopg2

app = Flask(__name__)
redis_conn = Redis(host='redis', port=6379)
queue = Queue(connection=redis_conn)

@app.route('/users', methods=['POST'])
def enqueue_user():
    user = request.get_json()
    
    return user, 200

def process_user(user):
    conn = psycopg2.connect(
        host='postgres',
        port=5432,
        dbname='mydatabase',
        user='myuser',
        password='mypassword'
    )
    cur = conn.cursor()

    insert_query = """
        INSERT INTO users (id, first_name, last_name, email, gender, ip_address, user_name, agent, country, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cur.execute(insert_query, (
        user['id'],
        user['first_name'],
        user['last_name'],
        user['email'],
        user['gender'],
        user['ip_address'],
        user['user_name'],
        user['agent'],
        user['country'],
        datetime.now()
    ))

    conn.commit()
    cur.close()
    conn.close()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)