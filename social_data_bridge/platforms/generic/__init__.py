"""
Generic platform parser.

A simple parser for arbitrary JSON/CSV data without platform-specific logic.
Uses only the shared utilities from core/parser.py.
"""

from .parser import (
    transform_json,
    parse_to_csv,
    parse_files_parallel,
    process_single_file,
)

__all__ = [
    'transform_json',
    'parse_to_csv',
    'parse_files_parallel',
    'process_single_file',
]
