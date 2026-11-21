"""Zero-copy line reader shared by the engine pipelines.

rules.md: only "\n" ends a line ("\r" is data), and the first line of every file
is its header (skipped by position, never by value). Engine CSV readers break
either rule, so we split lines ourselves: mmap each file, find the "\n" bytes
with numpy, and build an Arrow large_string array whose offsets point into the
mapped file. Every string keeps its trailing "\n" (Arrow offsets must be
contiguous); the parsers match it explicitly.
"""
import glob
import os
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pyarrow as pa

BATCH_ROWS = 1 << 20
BLOCK = 1 << 26  # scan 64 MB at a time to bound the temporary bool array


def _newlines(buf):
    parts = []
    for start in range(0, len(buf), BLOCK):
        block = buf[start:start + BLOCK]
        parts.append(np.flatnonzero(block == 10) + start)
    return np.concatenate(parts) if parts else np.empty(0, np.int64)


def _file_array(path):
    """Arrow array of the file's data lines (header dropped), each ending in '\n'."""
    if os.path.getsize(path) == 0:
        return None
    data = pa.memory_map(path, "r").read_buffer()
    buf = np.frombuffer(data, dtype=np.uint8)
    if buf[-1] != 10:  # last line without "\n": copy and terminate it
        buf = np.concatenate([buf, np.array([10], np.uint8)])
        data = pa.py_buffer(buf)
    nl = _newlines(buf).astype(np.int64)
    n = len(nl) - 1  # lines after the header
    if n <= 0:
        return None
    offsets = nl + 1
    return pa.LargeStringArray.from_buffers(n, pa.py_buffer(offsets), data)


def read_lines(folder_or_file, threads=8):
    """pa.Table with one column `line` holding every data line of the CSV file(s)."""
    if os.path.isdir(folder_or_file):
        paths = sorted(glob.glob(os.path.join(folder_or_file, "*.csv")))
    else:
        paths = [folder_or_file]
    with ThreadPoolExecutor(threads) as pool:
        arrays = [a for a in pool.map(_file_array, paths) if a is not None]
    batches = []
    for arr in arrays:
        for start in range(0, len(arr), BATCH_ROWS):
            batches.append(pa.record_batch([arr.slice(start, BATCH_ROWS)], names=["line"]))
    schema = pa.schema([("line", pa.large_string())])
    return pa.Table.from_batches(batches, schema=schema)
