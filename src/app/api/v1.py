from fastapi import APIRouter

from app.modules.estoque.presentation.routers import item_estoque
from app.modules.servicos.presentation.routers import servico

router = APIRouter(prefix="/api/v1")
router.include_router(servico.router, prefix="/servicos", tags=["Serviços"])
router.include_router(item_estoque.router, prefix="/itens-estoque", tags=["Itens de Estoque"])
