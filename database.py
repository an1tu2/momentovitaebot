import sqlite3
from datetime import datetime

DB_NAME = "users.db"


def create_database():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        birth_date TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    conn.commit()
    conn.close()


def get_user(user_id: int) -> str:
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT birth_date FROM users WHERE user_id = ?", (user_id, ))
    result = c.fetchone()
    conn.close()
    return result[0] if result else None


def save_user(user_id: int, birth_date: str):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    try:
        datetime.strptime(birth_date, "%d.%m.%Y")
    except ValueError:
        raise ValueError("Неверный формат даты")
    c.execute(
        '''INSERT OR REPLACE INTO users (user_id, birth_date) VALUES (?, ?)''',
        (user_id, birth_date))
    conn.commit()
    conn.close()
