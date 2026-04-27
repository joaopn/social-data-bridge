"""Regression test: parser CSV columns must match DB COPY column list.

Bug history: the production reddit template had `retrieved_utc` listed both as
a parser-hardcoded mandatory field and in the per-data-type `fields` list. This
produced a CSV with a duplicate `retrieved_utc` column while
`get_column_list` returned only one — `COPY` then failed with "extra data after
last expected column" on CSV ingest. Parquet hid the bug because
`dict(zip(...))` collapses duplicate keys.

Both PG and StarRocks share the same `mandatory_fields + yaml_fields` shape, so
both are validated against the parser's CSV header here.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml

# Stub DB drivers absent from test deps before importing ingest modules
sys.modules.setdefault('psycopg', MagicMock())
sys.modules.setdefault('mysql', MagicMock())
sys.modules.setdefault('mysql.connector', MagicMock())

from social_data_pipeline.db.postgres.ingest import get_column_list as pg_get_column_list
from social_data_pipeline.db.starrocks.ingest import get_column_list as sr_get_column_list
from social_data_pipeline.platforms.reddit.parser import get_all_columns


REDDIT_TEMPLATE = Path(__file__).resolve().parents[2] / "config" / "templates" / "reddit.yaml"


def _load_template():
    return yaml.safe_load(REDDIT_TEMPLATE.read_text())


def _assert_no_duplicates(columns, label):
    seen = []
    for col in columns:
        assert col not in seen, f"{label} has duplicate column {col!r}: {columns}"
        seen.append(col)


@pytest.mark.parametrize(
    "backend,column_list_fn",
    [("postgres", pg_get_column_list), ("starrocks", sr_get_column_list)],
)
def test_reddit_template_parser_and_db_column_lists_agree(backend, column_list_fn):
    """For every reddit data type, parser CSV columns == DB COPY columns.

    `get_all_columns(data_type, fields)` builds the parser CSV header.
    The DB-side `get_column_list(data_type, platform_config)` builds the COPY /
    INSERT FROM FILES column list. These must match exactly — same names, same
    order, no duplicates — or CSV ingest will reject every row.
    """
    config = _load_template()
    for data_type in config["data_types"]:
        fields = config["fields"][data_type]
        parser_cols = get_all_columns(data_type, fields)
        db_cols = column_list_fn(data_type, config)

        _assert_no_duplicates(parser_cols, f"parser columns for {data_type}")
        _assert_no_duplicates(db_cols, f"{backend} columns for {data_type}")
        assert parser_cols == db_cols, (
            f"{backend}/{data_type} column mismatch:\n"
            f"  parser: {parser_cols}\n"
            f"  {backend:<7}: {db_cols}"
        )
