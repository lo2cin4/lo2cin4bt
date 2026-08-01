import json
from pathlib import Path

from jsonschema import Draft202012Validator


REPO_ROOT = Path(__file__).resolve().parents[1]
FEATURE_ROOT = REPO_ROOT / "backtester" / "contracts" / "feature"


def test_feature_contract_examples_use_typed_bar_specs_only() -> None:
    schema = json.loads(
        (FEATURE_ROOT / "feature-contract-v1.schema.json").read_text(encoding="utf-8")
    )
    validator = Draft202012Validator(schema)

    for path in sorted((FEATURE_ROOT / "examples").glob("*.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        validator.validate(payload)
        for feature in payload["features"]:
            assert "bar_spec" in feature
            assert "frequency" not in feature
            assert feature["bar_spec"]["aggregation"] == "time"
            assert isinstance(feature["bar_spec"]["step"], int)


def test_feature_contract_schema_has_no_frequency_authoring_field() -> None:
    schema_text = (FEATURE_ROOT / "feature-contract-v1.schema.json").read_text(
        encoding="utf-8"
    )

    assert '"frequency"' not in schema_text
    assert '"bar_spec"' in schema_text
