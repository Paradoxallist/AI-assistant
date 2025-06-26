import sqlite3
from utils.path import DB_ABS_PATH

# --------------------- CONNECTION ---------------------
def _get_conn():
    return sqlite3.connect(DB_ABS_PATH)

# --------------------- INSERT ---------------------
def add_token(piece: str, token_id: int) -> None:
    """
    Insert a new token piece with its ID, or ignore if it exists.
    """
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT OR IGNORE INTO tokens(piece, id) VALUES(?, ?)",
        (piece, token_id)
    )
    conn.commit()
    conn.close()

# --------------------- SELECT ---------------------
def get_token_id(piece: str) -> int:
    """
    Return the ID for a given piece, or None if not found.
    """
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id FROM tokens WHERE piece = ?", (piece,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None


def get_piece(token_id: int) -> str:
    """
    Return the piece for a given ID, or None if not found.
    """
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("SELECT piece FROM tokens WHERE id = ?", (token_id,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None


def all_tokens() -> list:
    """
    Return all tokens as a list of (piece, id).
    """
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("SELECT piece, id FROM tokens ORDER BY id")
    results = cur.fetchall()
    conn.close()
    return results

# --------------------- DELETE / RESET ---------------------
def delete_token(piece: str) -> None:
    """Remove a token piece from the table."""
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM tokens WHERE piece = ?", (piece,))
    conn.commit()
    conn.close()


def reset_tokens() -> None:
    """Delete all tokens."""
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM tokens")
    conn.commit()
    conn.close()
