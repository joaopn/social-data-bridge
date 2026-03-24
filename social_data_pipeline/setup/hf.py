"""Hugging Face dataset integration for Social Data Pipeline.

Fetches dataset metadata and parquet files from the HF Hub API using only
stdlib (urllib, json). No additional dependencies required.

API endpoints used:
  GET /api/datasets/{id}         → metadata, configs, features
  GET /api/datasets/{id}/parquet → parquet file URLs per config/split
"""

import json
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path


HF_API_BASE = "https://huggingface.co/api/datasets"


# ============================================================================
# HF API errors
# ============================================================================

class HFAPIError(Exception):
    """Error from HF API request."""
    def __init__(self, message, status_code=None):
        super().__init__(message)
        self.status_code = status_code


# ============================================================================
# HTTP helpers
# ============================================================================

def _hf_api_request(url, token=None):
    """Make a GET request to the HF API. Returns parsed JSON.

    Args:
        url: Full URL to fetch.
        token: Optional HF token for private datasets.

    Returns:
        Parsed JSON response.

    Raises:
        HFAPIError: On HTTP errors with meaningful messages.
    """
    headers = {"User-Agent": "social-data-pipeline/1.0"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        if e.code == 404:
            raise HFAPIError(f"Dataset not found: {url}", 404) from e
        elif e.code in (401, 403):
            raise HFAPIError(
                "Access denied. Use --token or set HF_TOKEN for private datasets.",
                e.code,
            ) from e
        else:
            raise HFAPIError(f"HF API error {e.code}: {e.reason}", e.code) from e
    except urllib.error.URLError as e:
        raise HFAPIError(f"Network error: {e.reason}") from e


# ============================================================================
# API fetchers
# ============================================================================

def fetch_dataset_metadata(dataset_id, token=None):
    """Fetch dataset metadata including configs and features.

    Returns:
        dict with full API response (cardData, configs, etc.)
    """
    url = f"{HF_API_BASE}/{dataset_id}"
    return _hf_api_request(url, token)


def fetch_parquet_urls(dataset_id, token=None):
    """Fetch parquet file download URLs.

    Returns:
        dict: {config_name: {split_name: [url_strings]}}
    """
    url = f"{HF_API_BASE}/{dataset_id}/parquet"
    return _hf_api_request(url, token)


# ============================================================================
# Type mapping
# ============================================================================

# Maps HF feature dtype strings to SDP SQL types.
# Returns None for unmappable types (sequence, struct, etc.)
_HF_TYPE_MAP = {
    "string": "text",
    "large_string": "text",
    "int8": "integer",
    "int16": "integer",
    "int32": "integer",
    "int64": "bigint",
    "uint8": "integer",
    "uint16": "integer",
    "uint32": "integer",
    "uint64": "bigint",
    "float16": "float",
    "float32": "float",
    "float64": "float",
    "bool": "boolean",
}


def map_hf_type_to_sql(hf_type):
    """Map an HF feature type descriptor to an SDP SQL type string.

    Args:
        hf_type: HF feature type — either a string like "string", "int64",
                 or a dict like {"dtype": "float32", "_type": "Value"} or
                 {"feature": {...}, "_type": "Sequence"}.

    Returns:
        (sql_type, is_mappable) tuple. sql_type is None if unmappable.
    """
    # Handle dict-style type descriptors from HF API
    if isinstance(hf_type, dict):
        type_class = hf_type.get("_type", "")
        if type_class == "Value":
            return map_hf_type_to_sql(hf_type.get("dtype", ""))
        # Sequence, Struct, Image, Audio, etc. are unmappable
        return (None, False)

    # Handle string type descriptors
    dtype = str(hf_type).lower().strip()

    # Direct match
    if dtype in _HF_TYPE_MAP:
        return (_HF_TYPE_MAP[dtype], True)

    # Timestamp variants → integer (epoch)
    if dtype.startswith("timestamp"):
        return ("bigint", True)

    # Unknown/complex type
    return (None, False)


# ============================================================================
# Schema extraction and grouping
# ============================================================================

def _extract_config_features(metadata):
    """Extract per-config feature lists from API metadata.

    The HF API nests data in two places within cardData:
      - cardData.configs: [{config_name, data_files}] — has names but no features
      - cardData.dataset_info: [{config_name, features, splits}] — has features

    We merge these by config_name, or fall back to top-level dataset_info.

    Returns:
        list of dicts: [{name, features: [{name, dtype, ...}], num_rows}]
    """
    configs = []
    card_data = metadata.get("cardData") or {}

    # Primary source: cardData.dataset_info (contains features + splits)
    ds_info = card_data.get("dataset_info", [])
    if not ds_info:
        # Fall back to top-level dataset_info
        ds_info = metadata.get("dataset_info", metadata.get("config", []))

    if isinstance(ds_info, dict):
        ds_info = [ds_info]
    if isinstance(ds_info, list):
        for info in ds_info:
            config_name = info.get("config_name", "default")
            features_info = info.get("features", [])
            num_rows = None
            splits = info.get("splits", [])
            if isinstance(splits, list):
                num_rows = sum(s.get("num_examples", 0) for s in splits)
            elif isinstance(splits, dict):
                num_rows = sum(
                    s.get("num_examples", 0) for s in splits.values()
                )
            configs.append({
                "name": config_name,
                "features": features_info,
                "num_rows": num_rows,
                })

    return configs


def _feature_key(features):
    """Create a hashable key from a feature list for schema grouping.

    Groups by field names only (ignoring types), since the same logical
    schema may have minor type differences across configs.
    """
    names = sorted(f.get("name", "") for f in features if f.get("name"))
    return tuple(names)


def group_configs_by_schema(configs):
    """Group configs that share the same set of field names.

    Args:
        configs: list from _extract_config_features()

    Returns:
        list of schema groups: [{
            configs: [config_dicts],
            field_names: [str],
            features: [feature_dicts],  # from first config in group
        }]
    """
    groups = {}
    for cfg in configs:
        key = _feature_key(cfg["features"])
        if key not in groups:
            groups[key] = {
                "configs": [],
                "field_names": sorted(key),
                "features": cfg["features"],
            }
        groups[key]["configs"].append(cfg)

    return list(groups.values())


def extract_hf_defaults(metadata):
    """Extract SDP-relevant defaults from HF dataset metadata.

    Returns:
        dict with:
            schema_groups: list of schema group dicts (see group_configs_by_schema)
            all_configs: list of all config dicts
            fields_by_group: list of [{name, sql_type, mappable}] per group
            field_types_by_group: list of {field_name: sql_type} per group
    """
    configs = _extract_config_features(metadata)
    if not configs:
        raise HFAPIError("Dataset has no configs. Cannot determine data types.")

    schema_groups = group_configs_by_schema(configs)

    fields_by_group = []
    field_types_by_group = []

    for group in schema_groups:
        fields = []
        field_types = {}
        for feat in group["features"]:
            name = feat.get("name", "")
            if not name:
                continue
            dtype = feat.get("dtype", feat)
            sql_type, mappable = map_hf_type_to_sql(dtype)
            fields.append({
                "name": name,
                "sql_type": sql_type,
                "mappable": mappable,
                "hf_type": _describe_hf_type(dtype),
            })
            if sql_type:
                field_types[name] = sql_type
        fields_by_group.append(fields)
        field_types_by_group.append(field_types)

    return {
        "schema_groups": schema_groups,
        "all_configs": configs,
        "fields_by_group": fields_by_group,
        "field_types_by_group": field_types_by_group,
    }


def _describe_hf_type(dtype):
    """Human-readable description of an HF type."""
    if isinstance(dtype, dict):
        type_class = dtype.get("_type", "unknown")
        if type_class == "Value":
            return dtype.get("dtype", "unknown")
        elif type_class == "Sequence":
            inner = dtype.get("feature", {})
            if isinstance(inner, dict) and inner.get("_type") == "Value":
                return f"sequence<{inner.get('dtype', '?')}>"
            return f"sequence<{inner}>"
        return type_class.lower()
    return str(dtype)


# ============================================================================
# Download
# ============================================================================

def download_hf_files(parquet_urls, dumps_dir, dataset_id=None, token=None):
    """Download parquet files from HF as a 1-to-1 mirror of the repo structure.

    Downloads to: dumps_dir/<config>/<split>/<filename>.parquet
    Also downloads the dataset README.md if dataset_id is provided.

    Args:
        parquet_urls: dict from fetch_parquet_urls() — {config: {split: [urls]}}
        dumps_dir: base directory (e.g., data/dumps/<source>)
        dataset_id: HF dataset ID (e.g., 'org/dataset') — used to download README
        token: optional HF token for private datasets

    Resume: skips files where local size matches Content-Length header.
    Atomic: downloads to .partial suffix, renames on completion.
    """
    target = Path(dumps_dir)
    target.mkdir(parents=True, exist_ok=True)
    headers = {"User-Agent": "social-data-pipeline/1.0"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    # Download README.md
    if dataset_id:
        readme_path = target / "README.md"
        if not readme_path.exists():
            readme_url = f"https://huggingface.co/datasets/{dataset_id}/raw/main/README.md"
            try:
                req = urllib.request.Request(readme_url, headers=headers)
                with urllib.request.urlopen(req, timeout=30) as resp:
                    readme_path.write_bytes(resp.read())
                print(f"  Downloaded README.md")
            except (urllib.error.URLError, OSError) as e:
                print(f"  Warning: could not download README.md: {e}")

    total_files = 0
    total_bytes = 0
    skipped = 0

    for config_name, splits in parquet_urls.items():
        for split_name, urls in splits.items():
            split_dir = target / config_name / split_name
            split_dir.mkdir(parents=True, exist_ok=True)

            for url in urls:
                filename = Path(urllib.parse.unquote(urllib.parse.urlparse(url).path)).name
                filepath = split_dir / filename
                partial = split_dir / f"{filename}.partial"

                # Resume check: skip if file exists with matching size
                if filepath.exists():
                    remote_size = _get_content_length(url, headers)
                    if remote_size and filepath.stat().st_size == remote_size:
                        skipped += 1
                        continue

                # Download
                total_files += 1
                label = f"{config_name}/{split_name}"
                print(f"  [{label}] Downloading {filename}...", end="", flush=True)
                try:
                    req = urllib.request.Request(url, headers=headers)
                    with urllib.request.urlopen(req, timeout=300) as resp:
                        size = int(resp.headers.get("Content-Length", 0))
                        downloaded = 0
                        with open(partial, "wb") as f:
                            while True:
                                chunk = resp.read(8 * 1024 * 1024)  # 8MB chunks
                                if not chunk:
                                    break
                                f.write(chunk)
                                downloaded += len(chunk)
                                if size:
                                    pct = downloaded * 100 // size
                                    mb = downloaded / (1024 * 1024)
                                    print(f"\r  [{label}] Downloading {filename}... "
                                          f"{mb:.1f} MB ({pct}%)", end="", flush=True)

                    # Atomic rename
                    partial.rename(filepath)
                    total_bytes += downloaded
                    print(f"\r  [{label}] Downloaded {filename} "
                          f"({downloaded / (1024*1024):.1f} MB)")

                except (urllib.error.URLError, OSError) as e:
                    print(f"\n  Error downloading {filename}: {e}")
                    if partial.exists():
                        print(f"    Partial file kept: {partial}")
                    continue

    print(f"\n  Done: {total_files} files downloaded ({total_bytes / (1024**3):.2f} GB)")
    if skipped:
        print(f"  Skipped: {skipped} files already present")


def organize_hf_downloads(dumps_dir, extracted_dir, config_map):
    """Organize downloaded HF parquet files from dumps into extracted/<data_type>/.

    Copies files from dumps/<config>/<split>/<index>.parquet into
    extracted/<data_type>/<config>_<index>.parquet (or <index>.parquet
    for single-config data types).

    Skips files already present in extracted with matching size.

    Args:
        dumps_dir: base dumps directory (e.g., data/dumps/<source>)
        extracted_dir: base extracted directory (e.g., data/extracted/<source>)
        config_map: dict {data_type: [config_names]} from platform.yaml
    """
    import shutil

    dumps = Path(dumps_dir)
    extracted = Path(extracted_dir)
    organized = 0
    skipped = 0

    for data_type, config_names in config_map.items():
        dt_dir = extracted / data_type
        dt_dir.mkdir(parents=True, exist_ok=True)

        multi_config = len(config_names) > 1

        for config_name in config_names:
            config_dir = dumps / config_name
            if not config_dir.is_dir():
                print(f"  Warning: config '{config_name}' not found in {dumps_dir}")
                continue

            # Collect all parquet files across splits
            parquet_files = sorted(config_dir.rglob("*.parquet"))
            for i, src_path in enumerate(parquet_files):
                if multi_config:
                    dest_name = f"{config_name}_{i}.parquet"
                else:
                    dest_name = f"{i}.parquet"

                dest_path = dt_dir / dest_name

                # Skip if already organized with matching size
                if dest_path.exists() and dest_path.stat().st_size == src_path.stat().st_size:
                    skipped += 1
                    continue

                shutil.copy2(src_path, dest_path)
                organized += 1

    print(f"  Organized: {organized} files into {extracted_dir}")
    if skipped:
        print(f"  Skipped: {skipped} files already present")


def _get_content_length(url, headers):
    """Get Content-Length for a URL via HEAD request. Returns int or None."""
    try:
        req = urllib.request.Request(url, method="HEAD", headers=headers)
        with urllib.request.urlopen(req, timeout=10) as resp:
            cl = resp.headers.get("Content-Length")
            return int(cl) if cl else None
    except (urllib.error.URLError, ValueError, OSError):
        return None
