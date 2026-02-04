"""
Generic JSON to CSV parsing utilities.

This module contains shared utilities used by all platform-specific parsers.
Platform-specific logic (like Reddit's waterfall algorithm) lives in platforms/*.
"""

from typing import Dict, List, Any, Optional


def escape_string(value: str) -> str:
    """Escape special characters in a string value for CSV output."""
    if isinstance(value, str):
        return value.replace('\\', '\\\\').replace('\n', '\\n').replace('\r', '\\r').replace('\u0000', '')
    return value


def quote_field(field: Any) -> str:
    """Quote a field value for CSV formatting."""
    if field is None:
        return ''
    elif isinstance(field, str) and field:
        escaped_field = field.replace('"', '""')
        return f'"{escaped_field}"'
    return str(field)


def get_nested_data(data: Dict, field: str) -> Any:
    """
    Retrieve nested data from a dictionary using dot notation.
    
    Supports:
    - Simple nested access: 'user.name' -> data['user']['name']
    - Array index access: 'items.0.id' -> data['items'][0]['id']
    - Arrays are converted to pipe-separated strings
    
    Args:
        data: Source dictionary
        field: Dot-separated field path (e.g., 'user.profile.name')
        
    Returns:
        The nested value, or None if not found.
        Arrays are returned as pipe-separated strings.
    """
    keys = field.split('.')
    current = data
    
    for key in keys:
        if current is None:
            return None
        if isinstance(current, dict) and key in current:
            current = current[key]
        elif isinstance(current, list) and key.isdigit():
            # Array index access
            idx = int(key)
            current = current[idx] if 0 <= idx < len(current) else None
        else:
            return None
    
    # Convert arrays to pipe-separated string
    if isinstance(current, list):
        return '|'.join(str(item) for item in current if item is not None)
    
    return current


def enforce_data_type(key: str, value: Any, data_types: Dict) -> Any:
    """
    Enforce a specific data type for a value based on the configuration.
    
    Args:
        key: Field name (used to look up type and max length)
        value: Value to cast
        data_types: Dict mapping field names to types. Types can be:
            - 'integer', 'bigint': Cast to int
            - 'float': Cast to float
            - 'boolean': Cast to bool
            - 'text': Cast to str
            - ['char', N] or ['varchar', N]: Cast to str with max length N
            
    Returns:
        The value cast to the appropriate type, or None if conversion fails.
    """
    if not data_types:
        return value

    def cast_value(data_type, val):
        if data_type in ('integer', 'bigint'):
            try:
                return int(val)
            except (ValueError, TypeError):
                return None
        elif data_type == 'boolean':
            return val in (True, 'True', 'true', 1)
        elif data_type == 'float':
            try:
                return float(val)
            except (ValueError, TypeError):
                return None
        elif data_type in ('char', 'varchar', 'text'):
            if val is None:
                return None
            val = str(val)
            if data_type in ('char', 'varchar') and key in data_types:
                type_def = data_types[key]
                if isinstance(type_def, list) and len(type_def) > 1:
                    max_length = type_def[1]
                    return val[:max_length]
            return val
        else:
            return None

    data_type = data_types.get(key)
    if data_type:
        return cast_value(data_type[0] if isinstance(data_type, list) else data_type, value)
    else:
        return value


def flatten_record(record: Dict, fields: List[str], types: Dict) -> List[Any]:
    """
    Generic JSON to CSV row conversion.
    
    Extracts specified fields from a JSON record, applies type enforcement,
    and returns a list suitable for CSV output.
    
    Args:
        record: Source JSON object as dict
        fields: List of field names to extract (supports dot notation)
        types: Field type definitions for type enforcement
        
    Returns:
        List of extracted and type-enforced values
    """
    row = []
    for field in fields:
        value = get_nested_data(record, field)
        if isinstance(value, str):
            value = escape_string(value)
        last_key = field.split('.')[-1]
        value = enforce_data_type(last_key, value, types)
        row.append(value)
    return row


def write_csv_row(values: List[Any]) -> str:
    """Convert a list of values to a CSV row string."""
    return ','.join(map(quote_field, values))
