import json
from data_service.reader import FileReader

class TextExtractor:
    """
    Uses FileReader to load JSON records and extract the 'text' field from each.
    """
    def __init__(self, encoding: str = 'utf-8', errors: str = 'ignore'):
        self.reader = FileReader(encoding=encoding, errors=errors)

    def extract_next(self):
        """
        Reads the next unprocessed JSON file, extracts its 'text' entries,
        and returns a tuple (file_id, texts_list). Returns None when done.
        """
        record = self.reader.get_next_file()
        if record is None:
            return None

        file_id, archive_path, member_path, *rest = record
        raw_text = self.reader.read_text(record)

        try:
            data = json.loads(raw_text)
        except json.JSONDecodeError:
            # Skip invalid JSON
            return (file_id, [])

        texts = []
        if isinstance(data, list):
            for entry in data:
                if isinstance(entry, dict) and 'text' in entry:
                    texts.append(entry['text'])
        elif isinstance(data, dict) and 'text' in data:
            texts.append(data['text'])
        return file_id, texts

    def close(self):
        """Clean up underlying reader resources."""
        self.reader.close()

if __name__ == '__main__':
    extractor = TextExtractor()
    result = extractor.extract_next()
    file_id, texts = result
    print(f"File {file_id}: extracted {len(texts)} text entries.")
    extractor.close()
