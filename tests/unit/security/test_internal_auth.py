from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app
from app.modules.estoque.domain.entities.item_estoque import (
    ItemEstoque,
    TipoItemEstoque,
)
from app.modules.estoque.presentation.dependencies import get_item_estoque_repo
from app.shared.infra.api.dependencies.auth import get_usuario_autenticado
from app.shared.infra.security.jwt import JWTProvider
from app.shared.value_objects.id import ID

SECRET = "segredo-hs256-exclusivo-dos-testes"
CORRELATION_ID = str(uuid4())


class _MissingResourceRepository:
    async def obter_por_id(self, resource_id):
        return None


class _ResourceRepository:
    def __init__(self, resource: ItemEstoque) -> None:
        self.resource = resource

    async def obter_por_id(self, resource_id):
        assert resource_id == self.resource.id
        return self.resource


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

    app = create_app(settings)
    app.dependency_overrides[get_item_estoque_repo] = _MissingResourceRepository
    with TestClient(app) as client:
        headers = {"X-Correlation-ID": CORRELATION_ID}
        assert client.get(path, headers=headers).status_code == 401
        assert client.get(
            path,
            headers={**headers, "Authorization": f"Bearer {user}"},
        ).status_code == 401
        assert client.get(
            path,
            headers={**headers, "Authorization": f"Bearer {wrong_scope}"},
        ).status_code == 403
        assert client.get(
            path,
            headers={**headers, "Authorization": f"Bearer {correct}"},
        ).status_code == 404


def test_internal_resource_snapshot_returns_catalog_without_stock_balances() -> None:
    settings = _settings()
    resource_id = ID.generate()
    now = datetime.now(UTC)
    resource = ItemEstoque(
        id=resource_id,
        tipo=TipoItemEstoque.PECA,
        nome="Filtro de óleo",
        descricao="Filtro premium",
        codigo="FO-001",
        quantidade_disponivel=12,
        quantidade_reservada=3,
        quantidade_minima=2,
        valor_unitario=Decimal("35.90"),
        ativo=True,
        criado_em=now,
        atualizado_em=now,
    )
    token = _provider(settings).create_service_token(
        client_id="mvp-oficina-ordens",
        audience=settings.service_name,
        scopes={"resources:catalog:read"},
        expire_minutes=5,
    )["access_token"]
    app = create_app(settings)
    app.dependency_overrides[get_item_estoque_repo] = lambda: _ResourceRepository(
        resource
    )

    with TestClient(app) as client:
        response = client.get(
            f"/internal/v1/catalog/resources/{resource_id.value}",
            headers={
                "Authorization": f"Bearer {token}",
                "X-Correlation-ID": CORRELATION_ID,
            },
        )

    assert response.status_code == 200
    assert response.json() == {
        "resource_id": str(resource_id.value),
        "name": "Filtro de óleo",
        "description": "Filtro premium",
        "unit_price": "35.90",
        "version": 1,
    }


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

