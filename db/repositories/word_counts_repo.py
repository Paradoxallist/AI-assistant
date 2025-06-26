import sqlite3
from utils.path import DB_ABS_PATH

# --------------------- CONNECTION ---------------------
def _get_conn():
    return sqlite3.connect(DB_ABS_PATH)

# --------------------- INSERT / UPDATE ---------------------
def increment_word(word: str, amount: int = 1) -> None:
    """
    Insert or increment the count for a given word by `amount`.
    """
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO word_counts(word, count)
        VALUES(?, ?)
        ON CONFLICT(word) DO UPDATE SET count = count + ?;
        """,
        (word, amount, amount)
    )
    conn.commit()
    conn.close()

# --------------------- SELECT ---------------------
def get_count(word: str) -> int:
    """
    Return the count for a given word, or 0 if not present.
    """
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT count FROM word_counts WHERE word = ?",
        (word,)
    )
    row = cur.fetchone()
    conn.close()
    return row[0] if row else 0


def top_n_words(n: int) -> list:
    """
    Return the top `n` words by frequency as a list of (word, count).
    """
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT word, count FROM word_counts ORDER BY count DESC LIMIT ?",
        (n,)
    )
    results = cur.fetchall()
    conn.close()
    return results

# --------------------- DELETE / RESET ---------------------
def reset_counts() -> None:
    """Reset all word counts to zero (keeps keys)."""
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE word_counts SET count = 0;")
    conn.commit()
    conn.close()


def delete_word(word: str) -> None:
    """Remove a word from the table."""
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM word_counts WHERE word = ?", (word,))
    conn.commit()
    conn.close()
