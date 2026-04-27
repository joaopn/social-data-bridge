"""Tests for source-setup config generators."""

import pytest
import yaml

from social_data_pipeline.setup.source import generate_platform_yaml


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
