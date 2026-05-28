import os

def get_db_url():
    return os.getenv("DATABASE_URL")

def connect():
    url = get_db_url()
    return url
