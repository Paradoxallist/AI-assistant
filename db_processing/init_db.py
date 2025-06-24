import sqlite3
from utils.path import DB_ABS_PATH
from db_utils import  drop_table


def init_db():
    """
    Initialize the SQLite database and create the `files` table.
    """
    conn = sqlite3.connect(DB_ABS_PATH)
    cursor = conn.cursor()

    # Create the `files` table to track archive members
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS files (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        archive_path TEXT    NOT NULL,
        member_path  TEXT    NOT NULL,
        file_name    TEXT    NOT NULL,
        extension    TEXT    NOT NULL,
        file_size    INTEGER NOT NULL,
        processed    BOOLEAN NOT NULL DEFAULT 0,
        detected_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    """
    )

    conn.commit()
    conn.close()


if __name__ == "__main__":
    drop_table()
    init_db()
    print(f"Initialized database at {DB_ABS_PATH}")
