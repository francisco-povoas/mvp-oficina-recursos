import json
from pathlib import Path
from uuid import uuid4

from fastapi.testclient import TestClient
from jsonschema import Draft202012Validator, FormatChecker

from app.config import Settings
from app.main import create_app
from app.modules.estoque.presentation.dependencies import get_item_estoque_repo
from app.shared.infra.security.jwt import JWTProvider

CONTRACT = json.loads(
    Path("contracts/openapi/internal-v1.openapi.json").read_text(encoding="utf-8")
)
PROBLEM = CONTRACT["components"]["schemas"]["Problem"]
CORRELATION_ID = str(uuid4())
SECRET = "segredo-hs256-exclusivo-dos-testes"


class _MissingResourceRepository:
    async def obter_por_id(self, resource_id):
        return None


def _assert_problem(response, status_code: int, correlation_id: str | None = None) -> None:
    assert response.status_code == status_code
    Draft202012Validator(PROBLEM, format_checker=FormatChecker()).validate(
        response.json()
    )
    assert "detail" not in response.json()
    assert response.headers["X-Correlation-ID"] == response.json()["correlation_id"]
    if correlation_id:
        assert response.json()["correlation_id"] == correlation_id


def test_success_response_echoes_correlation_id() -> None:
    with TestClient(create_app(Settings(environment="test"))) as client:
        response = client.get(
            "/health", headers={"X-Correlation-ID": CORRELATION_ID}
        )

    assert response.status_code == 200
    assert response.headers["X-Correlation-ID"] == CORRELATION_ID


def test_authentication_authorization_validation_and_planned_responses() -> None:
    settings = Settings(environment="test", jwt_secret_key=SECRET)
    provider = JWTProvider(
        settings.jwt_secret_key,
        settings.jwt_access_token_expire_minutes,
        settings.jwt_algorithm,
        settings.jwt_issuer,
    )
    wrong_scope = provider.create_service_token(
        client_id="mvp-oficina-ordens",
        audience=settings.service_name,
        scopes={"resources:operation:read"},
        expire_minutes=5,
    )["access_token"]
    correct = provider.create_service_token(
        client_id="mvp-oficina-ordens",
        audience=settings.service_name,
        scopes={"resources:catalog:read"},
        expire_minutes=5,
    )["access_token"]
    path = f"/internal/v1/catalog/resources/{uuid4()}"

    app = create_app(settings)
    app.dependency_overrides[get_item_estoque_repo] = _MissingResourceRepository
    with TestClient(app) as client:
        unauthorized = client.get(
            path, headers={"X-Correlation-ID": CORRELATION_ID}
        )
        forbidden = client.get(
            path,
            headers={
                "X-Correlation-ID": CORRELATION_ID,
                "Authorization": f"Bearer {wrong_scope}",
            },
        )
        missing = client.get(
            path,
            headers={
                "X-Correlation-ID": CORRELATION_ID,
                "Authorization": f"Bearer {correct}",
            },
        )
        missing_correlation = client.get(
            path, headers={"Authorization": f"Bearer {correct}"}
        )

    _assert_problem(unauthorized, 401, CORRELATION_ID)
    _assert_problem(forbidden, 403, CORRELATION_ID)
    _assert_problem(missing, 404, CORRELATION_ID)
    _assert_problem(missing_correlation, 422)
    assert missing.json()["code"] == "RESOURCE_NOT_FOUND"
