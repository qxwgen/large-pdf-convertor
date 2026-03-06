import pdfplumber
import polars as pd
import os
import time
import json
from pathlib import Path

pdf_path = 'src/swift-test.pdf'
output_csv = Path(pdf_path).with_suffix('.csv')
progress_file = Path(pdf_path).with_suffix('.progress.json')

if progress_file.exists():
    with open(progress_file, 'r') as f:
        progress = json.load(f)
    start_page = progress.get("last_page", 0) + 1
    print(f"🔄 Resuming from page {start_page + 1}...")
else:
    start_page = 0
    print("🆕 Starting fresh...")

first_table = not output_csv.exists()

start_time = time.time()
print("⏱️  Starting PDF to CSV conversion...")

with pdfplumber.open(pdf_path) as pdf:
    total_pages = len(pdf.pages)

    for i in range(start_page, total_pages):
        try:
            print(f"🔍 Extracting tables from page {i+1}/{total_pages}")

            page = pdf.pages[i]
            tables = page.extract_tables()

            for table in tables:
                if table and len(table) > 1:
                    header = table[0]
                    data_rows = table[1:]
                    df = pd.DataFrame(data_rows, schema=header, orient='row')

                    with open(output_csv.resolve(), 'a', newline='', encoding='utf-8') as f:
                        df.write_csv(f, include_header=first_table)

                    first_table = False

            with open(progress_file, 'w') as f:
                json.dump({"last_page": i}, f)

        except Exception as e:
            print(f"❌ Error on page {i+1}: {e}")
            print("💾 Progress saved. You can resume the script later.")
            break

if progress_file.exists() and i == total_pages - 1:
    progress_file.unlink()
    print("🧹 Progress file cleaned up.")

end_time = time.time()
duration = end_time - start_time
print(f"✅ Combined CSV saved as '{output_csv.resolve()}'")
print(f"⏳ Total processing time: {duration:.2f} seconds")