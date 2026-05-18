"""Tests for source-setup config generators."""

import pytest
import yaml

from social_data_pipeline.setup.source import (
    generate_platform_yaml,
    generate_postgres_yaml,
    generate_postgres_ml_yaml,
    generate_starrocks_yaml,
    generate_sr_ml_yaml,
)


def _base_settings(**overrides):
    settings = {
        "source_name": "mydata",
        "data_types": ["events"],
        "dumps_path": "./data/dumps/mydata",
        "extracted_path": "./data/extracted/mydata",
        "parsed_path": "./data/parsed/mydata",
        "output_path": "./data/output/mydata",
        "custom_file_patterns": {
            "events": {
                "dump": r"^events_.+\.zst$",
                "json": r"^events_.+$",
                "csv": r"^events_.+\.csv$",
                "parquet": r"^events_.+\.parquet$",
                "prefix": "events_",
                "compression": "zst",
                "dump_glob": "events_*.zst",
            }
        },
    }
    settings.update(overrides)
    return settings


class TestGeneratePlatformYaml:
    def test_writes_fields_when_provided(self):
        """Happy path: explicit fields land in the generated YAML."""
        settings = _base_settings(
            custom_fields={"events": ["timestamp", "user.name", "score"]}
        )
        config = yaml.safe_load(generate_platform_yaml(settings))
        assert config["fields"]["events"] == ["timestamp", "user.name", "score"]

    def test_raises_when_no_fields(self):
        """Regression: empty/missing fields used to silently produce an
        empty fields list, breaking parse with "No fields configured for
        data type". Now the generator refuses.
        """
        settings = _base_settings()  # no custom_fields
        with pytest.raises(ValueError, match="No fields configured"):
            generate_platform_yaml(settings)

    def test_raises_when_fields_dict_empty(self):
        """Defensive: a dict with no entries is also rejected."""
        settings = _base_settings(custom_fields={})
        with pytest.raises(ValueError, match="No fields configured"):
            generate_platform_yaml(settings)


class TestProfileGeneratorsNoPrimaryKey:
    """When a custom source has no primary_key, db-profile generators must
    emit check_duplicates: false. Otherwise the runtime guards
    (postgres_ingest.py 'check_duplicates is enabled but no primary_key'
    and sr_ingest.py's equivalent) would raise on the first run, and the SR
    ingest path would also emit invalid PROPERTIES("merge_condition") SQL
    against a Duplicate Key table.
    """

    def _settings(self, primary_key=None):
        return {"data_types": ["events"], "primary_key": primary_key}

    def test_postgres_yaml_disables_check_duplicates_when_no_pk(self):
        out = yaml.safe_load(generate_postgres_yaml(self._settings(primary_key=None)))
        assert out["pipeline"]["processing"]["check_duplicates"] is False

    def test_postgres_yaml_omits_check_duplicates_when_pk_set(self):
        out = yaml.safe_load(generate_postgres_yaml(self._settings(primary_key="id")))
        assert "check_duplicates" not in out["pipeline"]["processing"]

    def test_starrocks_yaml_disables_check_duplicates_when_no_pk(self):
        out = yaml.safe_load(generate_starrocks_yaml(self._settings(primary_key=None)))
        assert out["pipeline"]["processing"]["check_duplicates"] is False

    def test_starrocks_yaml_omits_check_duplicates_when_pk_set(self):
        out = yaml.safe_load(generate_starrocks_yaml(self._settings(primary_key="id")))
        assert "check_duplicates" not in out["pipeline"]["processing"]

    def test_postgres_ml_yaml_disables_check_duplicates_when_no_pk(self):
        out = yaml.safe_load(generate_postgres_ml_yaml(self._settings(primary_key=None)))
        assert out["pipeline"]["processing"]["check_duplicates"] is False

    def test_sr_ml_yaml_disables_check_duplicates_when_no_pk(self):
        out = yaml.safe_load(generate_sr_ml_yaml(self._settings(primary_key=None)))
        assert out["pipeline"]["processing"]["check_duplicates"] is False


class TestPlatformYamlNoPrimaryKey:
    """primary_key is optional in the generated platform.yaml — absent means
    'no source-level dedup, db autogenerates / Duplicate Key model'."""

    def test_omitted_when_settings_primary_key_is_none(self):
        settings = _base_settings(
            custom_fields={"events": ["timestamp", "text"]},
            primary_key=None,
        )
        config = yaml.safe_load(generate_platform_yaml(settings))
        assert "primary_key" not in config

    def test_omitted_when_settings_primary_key_is_empty_string(self):
        settings = _base_settings(
            custom_fields={"events": ["timestamp", "text"]},
            primary_key="",
        )
        config = yaml.safe_load(generate_platform_yaml(settings))
        assert "primary_key" not in config

    def test_emitted_when_settings_primary_key_set(self):
        settings = _base_settings(
            custom_fields={"events": ["timestamp", "text"]},
            primary_key="timestamp",
        )
        config = yaml.safe_load(generate_platform_yaml(settings))
        assert config["primary_key"] == "timestamp"


class TestPlatformYamlMandatoryDataset:
    """The custom parser always prepends a `dataset` column. Without
    mandatory_fields: [dataset] in platform.yaml, downstream PG/SR
    get_column_list returns N columns while the parsed Parquet has N+1,
    and PG's pg_parquet COPY fails on the count mismatch. Reddit is
    immune (its template sets mandatory_fields). This pins the fix for
    custom platforms (HF and non-HF alike).
    """

    def test_non_hf_custom_emits_dataset_in_mandatory_fields(self):
        settings = _base_settings(custom_fields={"events": ["id", "score"]})
        config = yaml.safe_load(generate_platform_yaml(settings))
        assert config["mandatory_fields"] == ["dataset"]

    def test_non_hf_custom_emits_dataset_in_field_types(self):
        settings = _base_settings(custom_fields={"events": ["id", "score"]})
        config = yaml.safe_load(generate_platform_yaml(settings))
        assert config["field_types"]["dataset"] == "text"

    def test_hf_custom_emits_dataset_in_mandatory_fields(self):
        # HF path provides custom_field_types (derived from HF metadata).
        settings = _base_settings(
            custom_fields={"events": ["text", "label"]},
            custom_field_types={"text": "text", "label": "integer"},
            hf_dataset="some/dataset",
        )
        config = yaml.safe_load(generate_platform_yaml(settings))
        assert config["mandatory_fields"] == ["dataset"]

    def test_hf_custom_emits_dataset_in_field_types_when_absent_from_hf(self):
        # HF metadata won't include `dataset` (parser-synthesized column);
        # generator must inject it so PG/SR DDL knows the type.
        settings = _base_settings(
            custom_fields={"events": ["text"]},
            custom_field_types={"text": "text"},
            hf_dataset="some/dataset",
        )
        config = yaml.safe_load(generate_platform_yaml(settings))
        assert config["field_types"]["dataset"] == "text"

    def test_hf_custom_preserves_existing_dataset_type_if_provided(self):
        # Defensive: if some upstream ever provides `dataset` via
        # custom_field_types, don't clobber it.
        settings = _base_settings(
            custom_fields={"events": ["text"]},
            custom_field_types={"dataset": ["char", 7], "text": "text"},
            hf_dataset="some/dataset",
        )
        config = yaml.safe_load(generate_platform_yaml(settings))
        assert config["field_types"]["dataset"] == ["char", 7]
