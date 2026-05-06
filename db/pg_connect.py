import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def connect_to_db():
    """Connect to PostgreSQL using psycopg2."""
    print('Connecting to PostgreSQL')
    try:
        return psycopg2.connect(
            host=os.environ['POSTGRES_DB_HOST'],
            port=os.environ['POSTGRES_DB_PORT'],
            dbname=os.environ['POSTGRES_DB_NAME'],
            user=os.environ['POSTGRES_DB_USER'],
            password=os.environ['POSTGRES_DB_PASSWORD'],
        )
    except psycopg2.Error as e:
        print(f'Database connection failed: {e}')
        raise
    
    
class DbEngine:
    """Single database connection engine for process."""
    def __init__(self):
        self.connection = psycopg2.connect(
            host=os.environ['POSTGRES_DB_HOST'],
            port=os.environ['POSTGRES_DB_PORT'],
            dbname=os.environ['POSTGRES_DB_NAME'],
            user=os.environ['POSTGRES_DB_USER'],
            password=os.environ['POSTGRES_DB_PASSWORD'],
        )
        
db_connection = DbEngine().connection