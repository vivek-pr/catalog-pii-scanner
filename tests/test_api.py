from fastapi.testclient import TestClient

from catalog_pii_scanner.api import app


def test_healthz() -> None:
    client = TestClient(app)
    resp = client.get("/healthz")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"
