from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app


def test_health_identifies_recursos_service() -> None:
    app = create_app(Settings(environment="test"))

    with TestClient(app) as client:
        response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "service": "mvp-oficina-recursos",
        "version": "0.1.0",
    }
