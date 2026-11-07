from uuid import uuid4

import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app
from app.shared.infra.api.dependencies.auth import get_usuario_autenticado
from app.shared.infra.security.jwt import JWTProvider

SECRET = "segredo-hs256-exclusivo-dos-testes"


def _settings() -> Settings:
    return Settings(environment="test", jwt_secret_key=SECRET)


def _provider(settings: Settings) -> JWTProvider:
    return JWTProvider(
        settings.jwt_secret_key,
        settings.jwt_access_token_expire_minutes,
        settings.jwt_algorithm,
        settings.jwt_issuer,
    )


def test_internal_resources_endpoint_requires_exact_service_scope() -> None:
    settings = _settings()
    provider = _provider(settings)
    correct = provider.create_service_token(
        client_id="mvp-oficina-ordens",
        audience=settings.service_name,
        scopes={"resources:catalog:read"},
        expire_minutes=5,
    )["access_token"]
    wrong_scope = provider.create_service_token(
        client_id="mvp-oficina-ordens",
        audience=settings.service_name,
        scopes={"resources:operation:read"},
        expire_minutes=5,
    )["access_token"]
    user = provider.create_user_token(
        subject=str(uuid4()),
        roles=["Administrador"],
        audience=settings.jwt_user_audience,
    )["access_token"]
    path = f"/internal/v1/catalog/resources/{uuid4()}"

    with TestClient(create_app(settings)) as client:
        assert client.get(path).status_code == 401
        assert client.get(path, headers={"Authorization": f"Bearer {user}"}).status_code == 401
        assert client.get(path, headers={"Authorization": f"Bearer {wrong_scope}"}).status_code == 403
        assert client.get(path, headers={"Authorization": f"Bearer {correct}"}).status_code == 501


def test_public_user_dependency_accepts_user_and_rejects_service() -> None:
    settings = _settings()
    provider = _provider(settings)
    user = provider.create_user_token(
        subject=str(uuid4()),
        roles=["Administrador"],
        audience=settings.jwt_user_audience,
    )["access_token"]
    service = provider.create_service_token(
        client_id="mvp-oficina-ordens",
        audience=settings.jwt_user_audience,
        scopes={"resources:catalog:read"},
        expire_minutes=5,
    )["access_token"]
    app = FastAPI()
    app.state.settings = settings

    @app.get("/public", dependencies=[Depends(get_usuario_autenticado)])
    async def public_probe() -> dict:
        return {"ok": True}

    with TestClient(app) as client:
        assert client.get("/public", headers={"Authorization": f"Bearer {user}"}).status_code == 200
        assert client.get("/public", headers={"Authorization": f"Bearer {service}"}).status_code == 401


def test_provider_refuses_algorithm_other_than_hs256() -> None:
    with pytest.raises(ValueError, match="HS256"):
        JWTProvider(SECRET, 5, "RS256")

