import hashlib
import json
from pathlib import Path

from jsonschema import Draft202012Validator

SCHEMA = Path("contracts/vendor/ordens/saga-stock.v1.schema.json")
EXPECTED_CANONICAL_SHA256 = "22aa7c9d068c4898e9f9fb29da4b9313457125ebde17b2a70c74b56a1cc95869"


def test_schema_consumido_de_ordens_e_semanticamente_imutavel() -> None:
    document = json.loads(SCHEMA.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(document)
    canonical = json.dumps(
        document,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    assert hashlib.sha256(canonical).hexdigest() == EXPECTED_CANONICAL_SHA256
