# Adding New Platforms

Social Data Bridge supports two ways to add new platforms:

1. **Custom platform** (config-only) — Create a single YAML file. No code required.
2. **Built-in platform** (with custom parser) — For platforms needing specialized logic (computed fields, format handling, etc.).

---

## Option 1: Custom Platform (Recommended)

If your data is standard JSON/NDJSON, use the custom platform system. No code changes needed.

Create `config/platforms/custom/<name>.yaml`:

```yaml
db_schema: my_platform
data_types:
  - posts
  - comments
file_patterns:
  posts:
    zst: '^posts_(\d{4}-\d{2})\.zst$'
    json: '^posts_(\d{4}-\d{2})$'
    csv: '^posts_(\d{4}-\d{2})\.csv$'
    prefix: 'posts_'
indexes:
  posts: [author, created_at]
field_types:
  created_at: integer
  author: text
  content: text
fields:
  posts:
    - created_at
    - author
    - content
```

Run with `PLATFORM=custom/<name>`. See [Custom Platforms](custom.md) for full details.

---

## Option 2: Built-in Platform (Custom Parser)

For platforms needing specialized parsing logic (like Reddit's deletion detection or base-36 ID conversion):

### 1. Create Platform Configuration

Create `config/platforms/{platform}/platform.yaml` with all sections (`db_schema`, `data_types`, `file_patterns`, `indexes`, `field_types`, `fields`).

Users can override any section via an optional `user.yaml` in the same directory (deep-merged over `platform.yaml`, lists replace).

### 2. Create Parser Module

Create `social_data_bridge/platforms/{platform}/parser.py` with these functions:

```python
def transform_json(data, dataset, data_type_config, fields_to_extract):
    """Transform a single JSON record into a list of CSV values."""
    ...

def process_single_file(input_file, output_file, data_type, data_type_config, fields_to_extract):
    """Process a single JSON file to CSV. Returns (input_size, output_file)."""
    ...

def parse_to_csv(input_file, output_dir, data_type, platform_config, use_type_subdir=True):
    """Main entry point. Parse a JSON file to CSV. Returns output CSV path."""
    ...

def parse_files_parallel(files, output_dir, platform_config, workers):
    """Parse multiple files in parallel. Returns list of (csv_path, data_type)."""
    ...
```

### 3. Register the Platform

Update `social_data_bridge/orchestrators/parse.py` to handle your platform in the `get_platform_parser()` function:

```python
def get_platform_parser(platform):
    if platform == 'reddit':
        from ..platforms.reddit import parser
        return parser
    elif platform == 'my_platform':
        from ..platforms.my_platform import parser
        return parser
    elif platform.startswith('custom/'):
        from ..platforms.custom import parser
        return parser
    else:
        raise ConfigurationError(f"Unknown platform: {platform}")
```

### 4. Run

```bash
python sdb.py run parse
```

> [!NOTE]
> The platform is configured during `python sdb.py setup`. Select your platform during setup.
