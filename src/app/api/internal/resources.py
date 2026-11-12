from decimal import Decimal
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, ConfigDict, Field

from app.modules.estoque.domain.ports.item_estoque_repo import ItemEstoqueRepo
from app.modules.estoque.presentation.dependencies import get_item_estoque_repo
from app.modules.servicos.domain.ports.servico_repo import ServicoRepo
from app.modules.servicos.presentation.dependencies import get_servico_repo
from app.shared.infra.api.dependencies.auth import RequireServiceScopes
from app.shared.value_objects.id import ID

router = APIRouter()
ServiceRepoDep = Annotated[ServicoRepo, Depends(get_servico_repo)]
ResourceRepoDep = Annotated[ItemEstoqueRepo, Depends(get_item_estoque_repo)]


class ServiceSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid")

    service_id: UUID
    name: str
    description: str | None
    price: str = Field(pattern=r"^(0|[1-9][0-9]*)(\.[0-9]{2})$")
    estimated_minutes: int = Field(gt=0)
    active: bool
    version: int = Field(default=1, ge=1)


class ResourceSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid")

    resource_id: UUID
    name: str
    description: str | None
    unit_price: str = Field(pattern=r"^(0|[1-9][0-9]*)(\.[0-9]{2})$")
    version: int = Field(default=1, ge=1)


def _money(value: Decimal) -> str:
    return format(value.quantize(Decimal("0.01")), "f")


def _planned(reference_id: UUID) -> None:
    raise HTTPException(
        status_code=501,
        detail={
            "code": "INTEGRATION_PLANNED",
            "message": "Consulta funcional planejada para a Etapa 2",
            "reference_id": str(reference_id),
        },
    )


@router.get(
    "/catalog/services/{service_id}",
    response_model=ServiceSnapshot,
    dependencies=[Depends(RequireServiceScopes("resources:catalog:read"))],
)
async def get_service_snapshot(
    service_id: UUID,
    repository: ServiceRepoDep,
) -> ServiceSnapshot:
    service = await repository.obter_por_id(ID.from_string(str(service_id)))
    if service is None:
        raise HTTPException(
            status_code=404,
            detail={
                "code": "SERVICE_NOT_FOUND",
                "message": "Serviço do catálogo não encontrado",
            },
        )
    return ServiceSnapshot(
        service_id=service.id.value,
        name=service.nome,
        description=service.descricao,
        price=_money(service.valor_base),
        estimated_minutes=service.tempo_medio_minutos,
        active=service.ativo,
        version=1,
    )


@router.get(
    "/catalog/resources/{resource_id}",
    response_model=ResourceSnapshot,
    dependencies=[Depends(RequireServiceScopes("resources:catalog:read"))],
)
async def get_resource_snapshot(
    resource_id: UUID,
    repository: ResourceRepoDep,
) -> ResourceSnapshot:
    resource = await repository.obter_por_id(ID.from_string(str(resource_id)))
    if resource is None:
        raise HTTPException(
            status_code=404,
            detail={
                "code": "RESOURCE_NOT_FOUND",
                "message": "Recurso do catálogo não encontrado",
            },
        )
    return ResourceSnapshot(
        resource_id=resource.id.value,
        name=resource.nome,
        description=resource.descricao,
        unit_price=_money(resource.valor_unitario),
        version=1,
    )


@router.get(
    "/operations/{operation_id}",
    dependencies=[Depends(RequireServiceScopes("resources:operation:read"))],
)
async def get_operation_result(operation_id: UUID) -> None:
    _planned(operation_id)

