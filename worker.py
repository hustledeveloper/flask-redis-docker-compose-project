from redis import Redis
import psycopg2
import json

redis = Redis(host='redis', port=6379)

def process_user_data(user):
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
    except psycopg2.Error as e:
        print("Error inserting user:", e)

def process_user_queue():
    while True:
        _, user_data = redis.blpop('user_queue')
        user_list = json.loads(user_data)
        for user in user_list:
            process_user_data(user)

if __name__ == '__main__':
    process_user_queue()
