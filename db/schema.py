import sqlite3
from utils.path import DB_ABS_PATH

# --------------------- DDL STATEMENTS ---------------------
FILES_TABLE_DDL = """
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

WORD_COUNTS_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS word_counts (
    word  TEXT PRIMARY KEY,
    count INTEGER NOT NULL DEFAULT 0
);
"""

TOKENS_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS tokens (
    piece TEXT PRIMARY KEY,
    id    INTEGER UNIQUE NOT NULL
);
"""

# --------------------- SCHEMA INITIALIZER ---------------------
def init_all(drop_existing: bool = False) -> None:
    """
    Initialize all database tables. If drop_existing is True, drop tables first.
    """
    conn = sqlite3.connect(DB_ABS_PATH)
    cur = conn.cursor()

    if drop_existing:
        cur.execute("DROP TABLE IF EXISTS files;")
        cur.execute("DROP TABLE IF EXISTS word_counts;")
        cur.execute("DROP TABLE IF EXISTS tokens;")

    # Create tables
    cur.execute(FILES_TABLE_DDL)
    cur.execute(WORD_COUNTS_TABLE_DDL)
    cur.execute(TOKENS_TABLE_DDL)

    conn.commit()
    conn.close()

if __name__ == '__main__':
    init_all(drop_existing=True)
    print(f"Initialized schema in {DB_ABS_PATH}")
