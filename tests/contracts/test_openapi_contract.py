import json
from pathlib import Path

from jsonschema import Draft202012Validator

CONTRACTS = Path("contracts")
OPENAPI_PATH = CONTRACTS / "openapi" / "internal-v1.openapi.json"

EXPECTED = {
    "/catalog/services/{service_id}": "get",
    "/catalog/resources/{resource_id}": "get",
    "/operations/{operation_id}": "get"
}
EXPECTED_SCOPES = {
    "/catalog/services/{service_id}": [
        "resources:catalog:read"
    ],
    "/catalog/resources/{resource_id}": [
        "resources:catalog:read"
    ],
    "/operations/{operation_id}": [
        "resources:operation:read"
    ]
}


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def test_internal_openapi_is_versioned_and_marks_implementation_status() -> None:
    document = _load(OPENAPI_PATH)
    assert document["openapi"] == "3.1.0"
    assert document["info"]["version"] == "1.1.0"
    assert set(document["paths"]) == set(EXPECTED)

    for path, method in EXPECTED.items():
        operation = document["paths"][path][method]
        assert operation["x-required-scopes"] == EXPECTED_SCOPES[path]
        assert operation["security"] == [{"serviceBearer": []}]
        if path in {
            "/catalog/services/{service_id}",
            "/catalog/resources/{resource_id}",
        }:
            assert operation["x-implementation-status"] == "implemented-stage-2-rest"
            assert "501" not in operation["responses"]
        else:
            assert operation["x-implementation-status"] == "planned-stage-2"
            assert "501" in operation["responses"]


def test_openapi_materializes_error_correlation_and_idempotency_contracts() -> None:
    components = _load(OPENAPI_PATH)["components"]
    assert components["parameters"]["CorrelationId"]["required"] is True
    assert components["parameters"]["IdempotencyKey"]["required"] is True
    assert components["schemas"]["Problem"]["additionalProperties"] is False
    Draft202012Validator.check_schema(components["schemas"]["Problem"])


def test_common_message_envelope_and_problem_schemas_are_valid() -> None:
    for path in sorted((CONTRACTS / "common").glob("*.schema.json")):
        Draft202012Validator.check_schema(_load(path))

