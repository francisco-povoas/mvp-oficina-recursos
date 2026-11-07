from fastapi import FastAPI

from app.api.health import router as health_router
from app.api.internal.router import router as internal_router
from app.api.v1 import router as v1_router
from app.config import Settings, get_settings


def create_app(settings: Settings | None = None) -> FastAPI:
    resolved_settings = settings or get_settings()
    app = FastAPI(
        title="MVP Oficina — Recursos",
        version=resolved_settings.service_version,
    )
    app.state.settings = resolved_settings
    app.include_router(health_router)
    app.include_router(v1_router)
    app.include_router(internal_router)
    return app
