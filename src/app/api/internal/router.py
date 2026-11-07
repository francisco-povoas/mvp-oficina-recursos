from fastapi import APIRouter

from app.api.internal import resources

router = APIRouter(prefix="/internal/v1", include_in_schema=True)
router.include_router(resources.router, tags=["Contratos internos de Recursos"])

