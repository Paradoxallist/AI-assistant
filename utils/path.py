import os
from config.settings import DB_PATH, TEXTS_PATH

def resolve_from_root(relative_path):
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))
    return os.path.join(root, relative_path)

DB_ABS_PATH = resolve_from_root(DB_PATH)
TEXTS_ABS_PATH = resolve_from_root(TEXTS_PATH)