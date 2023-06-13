import psycopg2

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
    except psycopg2.Error as e:
        print("Error inserting user:", e)
