"""
Custom platform JSON to CSV parsing.

This module provides a simple parser for arbitrary JSON/NDJSON data
without any platform-specific transformation logic.
Used by all custom/* platforms.
"""

import json
import os
import logging
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor
from typing import Dict, List, Tuple

from ...core.config import ConfigurationError
from ...core.parser import (
    escape_string,
    quote_field,
    flatten_record,
)


def transform_json(data: Dict, dataset: str, data_type_config: Dict, fields_to_extract: List[str]) -> List:
    """
    Transform JSON data into a list of extracted values.
    
    Generic transformation - no platform-specific logic.
    
    Args:
        data: Source JSON object
        dataset: Dataset identifier (prepended to row)
        data_type_config: Field type configuration
        fields_to_extract: List of fields to extract
        
    Returns:
        List of [dataset, ...extracted_fields]
    """
    return [dataset] + flatten_record(data, fields_to_extract, data_type_config)


def process_single_file(
    input_file: str,
    output_file: str,
    data_type: str,
    data_type_config: Dict,
    fields_to_extract: List[str]
) -> tuple:
    """
    Process a single JSON file and write to CSV with headers.
    
    Uses a .temp file during writing and renames to final name on success.
    
    Args:
        input_file: Path to JSON/NDJSON file
        output_file: Path for output CSV file
        data_type: Data type identifier
        data_type_config: Field type configuration
        fields_to_extract: List of fields to extract
        
    Returns:
        Tuple of (input_size, output_file)
    """
    dataset = Path(input_file).stem
    
    output_path = Path(output_file)
    temp_path = output_path.with_suffix(output_path.suffix + '.temp')
    
    # Clean up any leftover temp file from interrupted run
    if temp_path.exists():
        print(f"[sdb] Removing incomplete temp file: {temp_path.name}")
        temp_path.unlink()
    
    line_count = 0
    error_count = 0
    
    # Get column names for header
    columns = ['dataset'] + fields_to_extract
    header_row = ','.join(columns)
    
    try:
        with open(input_file, 'r', encoding='utf-8', errors='replace') as infile, \
             open(temp_path, 'w', newline='', encoding='utf-8') as outfile:
            
            # Write header row
            outfile.write(header_row + '\n')
            
            for line in infile:
                cleaned_line = line.replace('\x00', '')
                if not cleaned_line.strip():
                    continue
                try:
                    data = json.loads(cleaned_line)
                    csv_data = transform_json(data, dataset, data_type_config, fields_to_extract)
                    csv_row = ','.join(map(quote_field, csv_data))
                    outfile.write(csv_row + '\n')
                    line_count += 1
                except json.JSONDecodeError as e:
                    error_count += 1
                    logging.error(f"Failed to decode line in {input_file}: {cleaned_line[:100]}... Error: {e}")
                    continue
        
        # Rename temp file to final output path on success
        temp_path.rename(output_path)
        
    except Exception:
        # Clean up temp file on failure
        if temp_path.exists():
            temp_path.unlink()
        raise
    
    input_size = os.path.getsize(input_file)
    output_size = os.path.getsize(output_file)
    
    print(f"[sdb] {Path(input_file).name} -> {Path(output_file).name}")
    print(f"[sdb] Rows: {line_count:,}, Errors: {error_count}, Output: {output_size / (1024**3):.2f} GB")
    
    return input_size, output_file


def parse_to_csv(
    input_file: str,
    output_dir: str,
    data_type: str,
    platform_config: Dict,
    use_type_subdir: bool = True
) -> str:
    """
    Parse a JSON/NDJSON file to CSV with headers.

    Args:
        input_file: Path to JSON/NDJSON file
        output_dir: Directory for output CSV file
        data_type: Data type identifier (used to select fields from config)
        platform_config: Loaded platform configuration dict (fields, field_types, etc.)
        use_type_subdir: If True, output to output_dir/data_type/

    Returns:
        Path to the output CSV file

    Raises:
        ConfigurationError: If config is missing required keys
    """
    output_dir = Path(output_dir)
    if use_type_subdir:
        output_dir = output_dir / data_type
    output_dir.mkdir(parents=True, exist_ok=True)

    # Extract fields and types from platform config
    field_types = platform_config.get('field_types', {})
    if not field_types:
        raise ConfigurationError("No field_types configured in platform config")

    fields_to_extract = platform_config.get('fields', {}).get(data_type, [])
    if not fields_to_extract:
        raise ConfigurationError(f"No fields configured for data type: {data_type}")
    
    # Configure logging
    log_filename = output_dir / f"parsing_errors_{data_type}.log"
    logging.basicConfig(
        filename=str(log_filename),
        level=logging.ERROR,
        format='%(asctime)s:%(levelname)s:%(message)s'
    )
    
    # Determine output filename
    input_path = Path(input_file)
    output_file = output_dir / f"{input_path.name}.csv"
    
    # Process the file
    _, output_path = process_single_file(
        input_file=str(input_path),
        output_file=str(output_file),
        data_type=data_type,
        data_type_config=field_types,
        fields_to_extract=fields_to_extract
    )
    
    # Clean up empty log file
    try:
        if log_filename.exists() and log_filename.stat().st_size == 0:
            log_filename.unlink()
    except Exception:
        pass
    
    return output_path


def _parse_file_worker(args: Tuple[str, str, str, Dict]) -> Tuple[str, str, str]:
    """Worker function for parallel parsing."""
    input_file, output_dir, data_type, platform_config = args
    csv_path = parse_to_csv(input_file, output_dir, data_type, platform_config)
    return input_file, csv_path, data_type


def parse_files_parallel(
    files: List[Tuple[str, str]],
    output_dir: str,
    platform_config: Dict,
    workers: int
) -> List[Tuple[str, str]]:
    """
    Parse multiple JSON files to CSV in parallel.

    Args:
        files: List of tuples (input_file, data_type)
        output_dir: Directory for output CSV files
        platform_config: Loaded platform configuration dict
        workers: Number of parallel workers

    Returns:
        List of tuples (csv_path, data_type) in the same order as input
    """
    if not files:
        return []

    print(f"[sdb] Starting parallel parsing with up to {workers} workers for {len(files)} files")

    worker_args = [
        (input_file, output_dir, data_type, platform_config)
        for input_file, data_type in files
    ]
    
    results = []
    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(_parse_file_worker, args) for args in worker_args]
        
        for future in futures:
            try:
                input_file, csv_path, data_type = future.result()
                results.append((csv_path, data_type))
            except Exception as e:
                print(f"[sdb] Error in parallel parsing: {e}")
                raise
    
    print(f"[sdb] Parallel parsing complete: {len(results)} files processed")
    return results
