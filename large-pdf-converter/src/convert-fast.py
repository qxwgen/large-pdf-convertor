import pdfplumber
import polars as pd # tabula-py
import multiprocessing as mp
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from pathlib import Path

pdf_path = 'src/swift-test.pdf' 
output_csv = Path(pdf_path).with_suffix('.csv')

# Shared state
results_buffer = {}           # page_index: DataFrame
write_index = 0               # Next page index to write
buffer_lock = threading.Lock()
PDF_LEN = 0

def extract_and_queue(i, page):
    """Extract tables and try writing if it's the next in line."""
    tables = page.extract_tables()
    page_frames = []
    for table in tables:
        print(f"Extracting tables from page {i+1}/{PDF_LEN}")
        if table:
            df = pd.DataFrame(table[1:], columns=table[0])
            page_frames.append(df)

    if page_frames:
        df_combined = pd.concat(page_frames, ignore_index=True)
    else:
        df_combined = pd.DataFrame()

    with buffer_lock:
        results_buffer[i] = df_combined
        try_write_ready_chunks()

def try_write_ready_chunks():
    global write_index
    while write_index in results_buffer:
        df = results_buffer.pop(write_index)
        if not df.empty:
            mode = 'a' if write_index > 0 else 'w'
            header = write_index == 0
            df.to_csv(output_csv, mode=mode, header=header, index=False)
        write_index += 1

with pdfplumber.open(pdf_path) as pdf:
    PDF_LEN = len(pdf.pages)
    with ThreadPoolExecutor(max_workers=mp.cpu_count()) as executor:
        futures = [
            executor.submit(extract_and_queue, i, page)
            for i, page in enumerate(pdf.pages)
        ]

    for future in as_completed(futures):
        future.result()

print(f"Saved: {output_csv}")

