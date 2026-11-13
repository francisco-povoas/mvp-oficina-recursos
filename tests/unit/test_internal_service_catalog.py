from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app
from app.modules.servicos.domain.entities.servico import Servico
from app.modules.servicos.presentation.dependencies import get_servico_repo
from app.shared.infra.security.jwt import JWTProvider
from app.shared.value_objects.id import ID


class FakeServiceRepository:
    def __init__(self, service: Servico | None) -> None:
        self.service = service

    async def obter_por_id(self, service_id: ID) -> Servico | None:
        if self.service and self.service.id == service_id:
            return self.service
        return None


def _token(settings: Settings) -> str:
    return JWTProvider(
        settings.jwt_secret_key,
        settings.jwt_access_token_expire_minutes,
        settings.jwt_algorithm,
        settings.jwt_issuer,
    ).create_service_token(
        client_id="mvp-oficina-ordens",
        audience=settings.service_name,
        scopes={"resources:catalog:read"},
        expire_minutes=5,
    )["access_token"]


def test_internal_catalog_returns_authoritative_price_time_and_activation() -> None:
    settings = Settings(environment="test", jwt_secret_key="segredo-de-teste")
    service_id = ID.generate()
    service = Servico(
        id=service_id,
        nome="Revisão",
        descricao="Revisão completa",
        valor_base=Decimal("150.00"),
        tempo_medio_minutos=60,
        ativo=False,
        criado_em=datetime.now(UTC),
        atualizado_em=datetime.now(UTC),
    )
    app = create_app(settings)
    app.dependency_overrides[get_servico_repo] = lambda: FakeServiceRepository(service)
    correlation_id = str(uuid4())

    with TestClient(app) as client:
        response = client.get(
            f"/internal/v1/catalog/services/{service_id}",
            headers={
                "Authorization": f"Bearer {_token(settings)}",
                "X-Correlation-ID": correlation_id,
            },
        )

    assert response.status_code == 200
    assert response.headers["X-Correlation-ID"] == correlation_id
    assert response.json() == {
        "service_id": str(service_id),
        "name": "Revisão",
        "description": "Revisão completa",
        "price": "150.00",
        "estimated_minutes": 60,
        "active": False,
        "version": 1,
    }


def test_internal_catalog_returns_404_without_leaking_persistence_details() -> None:
    settings = Settings(environment="test", jwt_secret_key="segredo-de-teste")
    app = create_app(settings)
    app.dependency_overrides[get_servico_repo] = lambda: FakeServiceRepository(None)

    with TestClient(app) as client:
        response = client.get(
            f"/internal/v1/catalog/services/{uuid4()}",
            headers={
                "Authorization": f"Bearer {_token(settings)}",
                "X-Correlation-ID": str(uuid4()),
            },
        )

    assert response.status_code == 404
    assert response.json()["code"] == "SERVICE_NOT_FOUND"
    assert "detail" not in response.json()
