from dotenv import load_dotenv
import os

load_dotenv()

DB_CONFIG = {
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "dbname": os.getenv("DB_NAME")
}

TABLE_NAME = os.getenv("TABLE_NAME", "call_logs")
VIEW_NAME = os.getenv("VIEW_NAME", "v_incoming_reiteration")
