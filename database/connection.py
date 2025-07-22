import psycopg


def create_connection(config: dict):
    conn = psycopg.connect(
        host=config['host'],
        port=config['port'],
        dbname=config['database'],
        user=config['user'],
        password=config['password'],
        autocommit=True
    )

    conn.set_read_only(True)
    return conn


def execute_read_query(conn, query: str):
    with conn.cursor() as cursor:
        cursor.execute(query)
        return cursor.fetchall()