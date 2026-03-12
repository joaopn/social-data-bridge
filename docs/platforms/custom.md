# Custom Platforms

Custom platforms (`PLATFORM=custom/<name>`) provide simple JSON-to-CSV conversion for arbitrary data sources, without platform-specific logic. Each custom platform is a single self-contained YAML file.

---

## Setup Guide

### 1. Prepare Your Data

Place your decompressed NDJSON files in `data/extracted/{data_type}/`:

```
data/extracted/
├── posts/
│   ├── data_2024-01
│   └── data_2024-02
└── users/
    └── users_export
```

> [!NOTE]
> Custom platforms currently require pre-extracted files placed directly in `data/extracted/`. Automatic `.zst` decompression uses platform-specific filename patterns.

### 2. Configure Platform

Create a single file `config/platforms/custom/<name>.yaml` (see `example.yaml`):

```yaml
db_schema: my_data
data_types:
  - posts
  - users
file_patterns:
  posts:
    zst: '^posts_(\d{4}-\d{2})\.zst$'
    json: '^posts_(\d{4}-\d{2})$'
    csv: '^posts_(\d{4}-\d{2})\.csv$'
    prefix: 'posts_'
  users:
    zst: '^users_.*\.zst$'
    json: '^users_.*$'
    csv: '^users_.*\.csv$'
    prefix: 'users_'
field_types:
  id: text
  created_at: integer
  author: text
  content: text
  likes: integer
  username: text
  email: text
fields:
  posts:
    - id
    - created_at
    - author
    - content
    - likes
  users:
    - id
    - username
    - email
    - profile.bio        # Nested field access with dot notation
indexes:
  posts:
    - dataset
    - author
```

Custom platform files are self-contained — no `user.yaml` override, no base to merge with.

### 3. Run

```bash
PLATFORM=custom/mydata python sdb.py run parse

# Classification works the same way
PLATFORM=custom/mydata python sdb.py run ml_cpu
PLATFORM=custom/mydata python sdb.py run ml
```

> [!TIP]
> The platform is configured during `python sdb.py setup`. If you select "custom", the base settings and file patterns will be generated for you.

---

## Features

- **Dot-notation nested field access**: Access nested JSON with `user.profile.name`
- **Array indexing**: Access array elements with `items.0.id`
- **Type enforcement**: Field types defined in YAML are enforced during parsing
- **No platform-specific logic**: Pure JSON-to-CSV conversion
- **Self-contained config**: One file per platform, no merging

## Supported Field Types

| Type | Description | Example |
|------|-------------|---------|
| `integer` | Integer values | `42` |
| `bigint` | Large integer values | `1234567890123` |
| `float` | Floating point numbers | `3.14` |
| `boolean` | True/False values | `true` |
| `text` | Variable-length strings | `"hello world"` |
| `['char', N]` | Fixed-length string | `['char', 2]` |
| `['varchar', N]` | Variable-length string up to N chars | `['varchar', 10]` |

## Limitations

- Requires pre-extracted files in `data/extracted/` (no automatic `.zst` decompression from `data/dumps/`)
- No automatic file detection — file patterns must be configured
- No computed fields (unlike Reddit's `id10`, `is_deleted`, `removal_type`)
