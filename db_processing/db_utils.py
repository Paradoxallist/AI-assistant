# db_processing/db_utils.py
import sqlite3
from utils.path import DB_ABS_PATH

# --------------------- CONNECTION ---------------------
def get_connection():
    """Return a new SQLite connection to the project database."""
    return sqlite3.connect(DB_ABS_PATH)

# --------------------- INSERT ---------------------
def add_file(archive_path: str, member_path: str, file_name: str, extension: str, file_size: int) -> None:
    """
    Insert a new file record, or ignore if it already exists.
    """
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
         INSERT OR IGNORE INTO files
           (archive_path, member_path, file_name, extension, file_size)
         VALUES (?, ?, ?, ?, ?)
        """,
        (archive_path, member_path, file_name, extension, file_size)
    )
    conn.commit()
    conn.close()

# --------------------- SELECT ---------------------
def get_unprocessed_files() -> list:
    """
    Return all files that have not yet been processed.
    Each row is a tuple: (id, archive_path, member_path, file_name, file_size).
    """
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, archive_path, member_path, file_name, file_size
          FROM files
         WHERE processed = 0
        """
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def get_all_files(limit: int = None) -> list:
    """
    Retrieve all file records, optionally limited in number.
    Rows: (id, archive_path, member_path, file_name, file_size, processed)
    """
    conn = get_connection()
    cur = conn.cursor()
    sql = "SELECT id, archive_path, member_path, file_name, file_size, processed FROM files"
    if limit is not None:
        sql += " LIMIT ?"
        cur.execute(sql, (limit,))
    else:
        cur.execute(sql)
    rows = cur.fetchall()
    conn.close()
    return rows


def count_unprocessed() -> int:
    """Return the number of unprocessed files."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM files WHERE processed = 0")
    count, = cur.fetchone()
    conn.close()
    return count

# --------------------- UPDATE ---------------------
def mark_file_processed(file_id: int) -> None:
    """
    Mark the given file record as processed.
    """
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE files
           SET processed = 1
         WHERE id = ?
        """,
        (file_id,)
    )
    conn.commit()
    conn.close()


def update_file(file_id: int, **kwargs) -> None:
    """
    Update arbitrary fields on a file record.
    Allowed keys: archive_path, member_path, file_name, file_size, processed.
    """
    allowed = {"archive_path", "member_path", "file_name", "extension", "file_size", "processed"}
    set_clause = []
    params = []
    for key, value in kwargs.items():
        if key in allowed:
            set_clause.append(f"{key} = ?")
            params.append(value)
    if not set_clause:
        return
    params.append(file_id)

    conn = get_connection()
    cur = conn.cursor()
    sql = f"UPDATE files SET {', '.join(set_clause)} WHERE id = ?"
    cur.execute(sql, params)
    conn.commit()
    conn.close()

# --------------------- DELETE ---------------------
def delete_file(file_id: int) -> None:
    """
    Delete a file record by its ID.
    """
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        DELETE FROM files
         WHERE id = ?
        """,
        (file_id,)
    )
    conn.commit()
    conn.close()

# --------------------- DELETE ALL ---------------------
def delete_all_files() -> None:
    """
    Delete all file records from the table.
    """
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        DELETE FROM files
        """
    )
    conn.commit()
    conn.close()

# --------------------- SCHEMA ---------------------
def drop_table() -> None:
    """
    Drop the `files` table entirely.
    """
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        DROP TABLE IF EXISTS files
        """
    )
    conn.commit()
    conn.close()
