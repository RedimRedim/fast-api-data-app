from db.db_config import DatabaseConnection
from dotenv import load_dotenv
import os

load_dotenv()


db = DatabaseConnection(
    host=os.getenv("DB_HOST"),
    database=os.getenv("DB_DATABASE"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    port=os.getenv("DB_PORT"),
)
