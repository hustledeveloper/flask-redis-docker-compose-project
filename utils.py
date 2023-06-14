import os
import redis
import psycopg2
import json

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def enqueue_user_to_redis(user):
    redis_url = os.getenv('REDIS_URL', 'redis://redis:6379')
    conn = redis.from_url(redis_url)
    conn.rpush('user_queue', json.dumps(user))
    logging.info('UUUUUUUUUUUUUUUUUUUUURRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRUU enqueued to Redis: %s', user)

def process_user(user):
    try:
        conn = psycopg2.connect(
            host="db",
            port=5432,
            database="postgres",
            user="postgres",
            password="postgres"
        )
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
        cursor.close()
        conn.close()
        logging.info('PPPPPPPPPPPPPPPPPPPPPPPPPPP processed: %s', user)
    except psycopg2.Error as e:
        logging.error('Error inserting user: %s', e)
