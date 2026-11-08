from fastapi import APIRouter, Depends

from app.api.internal import resources
from app.http_contract import require_correlation_id

router = APIRouter(
    prefix="/internal/v1",
    include_in_schema=True,
    dependencies=[Depends(require_correlation_id)],
)
router.include_router(resources.router, tags=["Contratos internos de Recursos"])

