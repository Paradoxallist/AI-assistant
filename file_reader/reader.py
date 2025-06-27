# file_reader/reader.py
import zipfile
import tarfile
import lzma

from db.repositories.files_repo import get_unprocessed_files, get_file_by_id, mark_file_processed_by_id

class FileReader:
    """
    Bridge between the database and archive processing.

    Provides methods to read the next unprocessed file or a specific file by ID,
    decode its text content (including nested .xz files), and mark it as processed.
    """
    def __init__(self, encoding: str = 'utf-8', errors: str = 'ignore'):
        self._archives = {}
        self.encoding = encoding
        self.errors = errors

    def _open_archive(self, archive_path: str):
        """Open or reuse an archive handle (ZIP or TAR)."""
        if archive_path in self._archives:
            return self._archives[archive_path]

        if zipfile.is_zipfile(archive_path):
            archive = zipfile.ZipFile(archive_path, mode='r')
        else:
            archive = tarfile.open(archive_path, mode='r:*')

        self._archives[archive_path] = archive
        return archive

    def get_next_file(self):
        """Return the next unprocessed file record from the DB, or None if none remain."""
        rows = get_unprocessed_files()
        return rows[0] if rows else None

    def get_file_record(self, file_id: int):
        """Return the file record for a given ID, or None if not found."""
        return get_file_by_id(file_id)

    def read_text(self, file_record) -> str:
        """
        Given a DB row tuple (id, archive_path, member_path, ...),
        read and return its decoded text content, then mark it processed.
        Supports nested .xz members inside tar archives.
        """
        file_id, archive_path, member_path, *rest = file_record
        archive = self._open_archive(archive_path)

        # Read raw bytes from archive member
        if isinstance(archive, zipfile.ZipFile):
            with archive.open(member_path, 'r') as fp:
                raw = fp.read()
        else:
            member = archive.getmember(member_path)
            with archive.extractfile(member) as fp:
                raw = fp.read() if fp else b''

        # If the member itself is compressed (e.g. an .xz file inside tar), decompress
        if member_path.lower().endswith('.xz'):
            raw = lzma.decompress(raw)

        # Decode to string
        text = raw.decode(self.encoding, errors=self.errors)

        # Mark as processed in DB
        mark_file_processed_by_id(file_id)
        return text

    def read_text_by_id(self, file_id: int) -> str:
        """
        Fetch the file record by ID, read its text content, and mark it processed.
        """
        record = self.get_file_record(file_id)
        if record is None:
            raise ValueError(f"No file found with ID {file_id}")
        return self.read_text(record)

    def close(self):
        """Close all open archive handles."""
        for archive in self._archives.values():
            archive.close()
        self._archives.clear()

if __name__ == '__main__':
    reader = FileReader()
    content = reader.read_text_by_id(2000)
    print(content)
    print(f"Read {len(content)} characters")
    reader.close()
