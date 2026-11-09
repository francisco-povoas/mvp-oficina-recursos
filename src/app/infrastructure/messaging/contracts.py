"""Validação do envelope contra o schema imutável produzido por Ordens."""

import json
from collections.abc import Mapping
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker

from app.application.messaging import MessageContractError


class JsonSchemaMessageValidator:
    def __init__(self, schema_path: str | Path) -> None:
        schema = json.loads(Path(schema_path).read_text(encoding="utf-8"))
        Draft202012Validator.check_schema(schema)
        self.validator = Draft202012Validator(schema, format_checker=FormatChecker())

    def validate(self, envelope: Mapping[str, object]) -> None:
        errors = sorted(self.validator.iter_errors(envelope), key=lambda item: list(item.path))
        if errors:
            first = errors[0]
            location = ".".join(str(part) for part in first.absolute_path) or "$"
            raise MessageContractError(
                f"JSON Schema rejeitou o campo {location} ({first.validator})"
            )
