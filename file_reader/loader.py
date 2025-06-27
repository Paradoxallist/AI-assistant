import os
import zipfile
import tarfile
from tqdm import tqdm

from utils.path import TEXTS_ABS_PATH
from config.settings import  ARCHIVE_EXTENSION
from db.repositories.files_repo import add_file



def discover_archives(base_dir: str = TEXTS_ABS_PATH) -> list:
    """
    Walk `base_dir` and return a list of supported archive file paths.
    """
    archives = []
    for root, _, files in os.walk(base_dir):
        for fname in files:
            fpath = os.path.join(root, fname)
            ext = os.path.splitext(fname)[1].lower()
            if ext in ARCHIVE_EXTENSION and (zipfile.is_zipfile(fpath) or tarfile.is_tarfile(fpath)):
                archives.append(fpath)
    return archives


def load_and_register_archives(base_dir: str = TEXTS_ABS_PATH) -> None:
    """
    Discover archives in `base_dir`, extract member metadata, and register each text file in the database.
    """
    archives = discover_archives(base_dir)

    for archive_path in tqdm(archives, desc="Registering archives"):
        try:
            if zipfile.is_zipfile(archive_path):
                with zipfile.ZipFile(archive_path) as arch:
                    members = arch.infolist()
                    get_name = lambda m: m.filename
                    get_size = lambda m: m.file_size
            else:
                with tarfile.open(archive_path, 'r:*') as arch:
                    members = arch.getmembers()
                    get_name = lambda m: m.name
                    get_size = lambda m: m.size

            for member in tqdm(members, desc=os.path.basename(archive_path), leave=False):
                # Skip directories
                name = get_name(member)
                if name.endswith('/'):
                    continue

                # You may filter to .json or text files here if desired
                # if not name.lower().endswith('.json'):
                #     continue

                file_name = os.path.basename(name)
                file_size = get_size(member)
                # Determine extension of the member
                _, ext = os.path.splitext(file_name)
                extension = ext.lstrip('.').lower()

                add_file(
                    archive_path=archive_path,
                    member_path=name,
                    file_name=file_name,
                    extension=extension,
                    file_size=file_size
                )
        except Exception as e:
            print(f"Error processing {archive_path}: {e}")


if __name__ == '__main__':
    load_and_register_archives()
    print("Archive discovery and member registration complete.")
