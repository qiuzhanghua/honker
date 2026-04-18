import asyncio
from fastapi.testclient import TestClient
from demo import app, emails_sent
import time


def run_test():
    with TestClient(app) as client:
        response = client.post(
            "/orders", json={"user_id": 1, "amount": 99.99, "email": "test@example.com"}
        )
        assert response.status_code == 200
        assert response.json() == {"ok": True}

        time.sleep(0.5)

        response = client.get("/stats")
        stats = response.json()
        print(stats)
        assert stats["orders"] == 1
        assert stats["emails_sent"] == 1
        print("Integration test passed!")


if __name__ == "__main__":
    run_test()
