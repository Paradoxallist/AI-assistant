#!/usr/bin/env python3
"""
scripts/extract_samples.py

Extract sample texts by ID and save them as .txt files for inspection.
"""
import os

from utils.path import resolve_from_root
from file_reader.reader import FileReader
from  file_reader.cleaner import TextCleaner

# IDs of sample records to extract
SAMPLE_IDS = [2000, 3000, 4000]

# Output directory relative to project root
OUTPUT_DIR = resolve_from_root(os.path.join("storage", "samples"))


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    reader = FileReader()
    text_cleaner = TextCleaner()

    for file_id in SAMPLE_IDS:
        try:
            text = reader.read_text_by_id(file_id)
        except ValueError as e:
            print(f"Skipping ID {file_id}: {e}")
            continue

        out_path = os.path.join(OUTPUT_DIR, f"{file_id}.txt")
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(text)
        out_path_1 = os.path.join(OUTPUT_DIR, f"{file_id}_clean.txt")
        with open(out_path_1, "w", encoding="utf-8") as f1:
            f1.write(text_cleaner.clean(text))
        print(f"Wrote sample text for ID {file_id} to {out_path}")

    reader.close()


if __name__ == "__main__":
    main()
