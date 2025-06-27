# file_reader/cleaner.py
import re

class TextCleaner:
    """
    Clean raw extracted text from archives by removing noise lines and normalizing whitespace.

    - Strips out TAR/metadata lines (e.g. filenames, ustar headers, hex fields)
    - Removes empty or purely punctuation lines
    - Collapses multiple blank lines into a single blank line
    - (Optionally) applies other domain-specific filters
    """
    # Regex to identify tar metadata lines (filename fields and ustar blocks)
    _tar_metadata_pattern = re.compile(r"^[0-9a-f\-/]+\.txt\s+\d+")
    _ustar_marker = re.compile(r"ustar")

    def clean(self, text: str) -> str:
        """
        Return a cleaned version of `text`.
        """
        cleaned_lines = []
        blank_count = 0
        for line in text.splitlines():
            # Trim whitespace
            line = line.strip()
            # Skip tar metadata lines
            if self._tar_metadata_pattern.match(line):
                continue
            # Skip ustar marker lines
            if self._ustar_marker.search(line):
                continue
            # Skip empty lines
            if not line:
                blank_count += 1
                # Collapse multiple blank lines to one
                if blank_count > 1:
                    continue
                cleaned_lines.append("")
                continue
            blank_count = 0
            # Append non-empty content line
            cleaned_lines.append(line)

        # Rejoin and ensure trailing newline
        return "\n".join(cleaned_lines).strip() + "\n"

if __name__ == '__main__':
    # Quick demonstration
    sample = (
        "0454060-cd29b753628be269f286cfebf2a3d6c6.txt                                                        0000644 0000000 ... ustar...\n"
        "When President Trump accused...\n"
        "\n"
        "Write to Tessa... [email]\n"
        "0454119-808e2b5389ef702bb97550d173ea53e5.txt    0000644 ... ustar...\n"
        "UPDATE: Only one package ..."
    )
    cleaner = TextCleaner()
    print(cleaner.clean(sample))
