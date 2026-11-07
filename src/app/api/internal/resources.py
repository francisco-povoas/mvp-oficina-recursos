from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException

from app.shared.infra.api.dependencies.auth import RequireServiceScopes

router = APIRouter()


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
    dependencies=[Depends(RequireServiceScopes("resources:catalog:read"))],
)
async def get_service_snapshot(service_id: UUID) -> None:
    _planned(service_id)


@router.get(
    "/catalog/resources/{resource_id}",
    dependencies=[Depends(RequireServiceScopes("resources:catalog:read"))],
)
async def get_resource_snapshot(resource_id: UUID) -> None:
    _planned(resource_id)


@router.get(
    "/operations/{operation_id}",
    dependencies=[Depends(RequireServiceScopes("resources:operation:read"))],
)
async def get_operation_result(operation_id: UUID) -> None:
    _planned(operation_id)

