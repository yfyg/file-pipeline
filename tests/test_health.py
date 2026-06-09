"""
Deep /health endpoint tests.

The endpoint pings Redis and the DB and returns 503 when either is down.
We don't want a shallow "always 200" endpoint fooling load balancers into
routing traffic to a degraded instance.
"""

def test_health_returns_200_when_dependencies_ok(client, monkeypatch):
    """
    Happy path. We don't have a real Redis in the test process, so patch
    Redis.ping() to succeed. DB is real (the in-process SQLite from conftest).
    """
    class FakeRedis:
        def __init__(self, *args, **kwargs): pass
        def ping(self): return True

    monkeypatch.setattr("redis.Redis", FakeRedis)

    response = client.get("/health")
    assert response.status_code == 200
    body = response.json()
    assert body["status"]         == "healthy"
    assert body["checks"]["redis"] == "ok"
    assert body["checks"]["db"]    == "ok"


def test_health_returns_503_when_redis_down(client, monkeypatch):
    """
    Redis ping raises. /health returns 503 and per-dependency detail names
    Redis as the failing one. DB still ok.
    """
    class BrokenRedis:
        def __init__(self, *args, **kwargs): pass
        def ping(self):
            raise ConnectionError("simulated Redis outage")

    monkeypatch.setattr("redis.Redis", BrokenRedis)

    response = client.get("/health")
    assert response.status_code == 503
    body = response.json()
    assert body["status"] == "unhealthy"
    assert body["checks"]["redis"].startswith("down")
    assert "simulated Redis outage" in body["checks"]["redis"]
    assert body["checks"]["db"] == "ok"


def test_health_returns_503_when_db_down(client, monkeypatch):
    """
    DB query raises. /health returns 503 and per-dependency detail names
    DB as the failing one. Redis still ok.
    """
    class FakeRedis:
        def __init__(self, *args, **kwargs): pass
        def ping(self): return True

    monkeypatch.setattr("redis.Redis", FakeRedis)

    # Force the SessionLocal's execute to blow up
    from app.models import database
    original_session = database.SessionLocal

    class BrokenSession:
        def execute(self, *a, **kw):
            raise RuntimeError("simulated DB outage")
        def close(self): pass

    monkeypatch.setattr(database, "SessionLocal", lambda: BrokenSession())

    # The endpoint imports SessionLocal inside the function, so the patch
    # takes effect on the next call
    response = client.get("/health")
    assert response.status_code == 503
    body = response.json()
    assert body["status"] == "unhealthy"
    assert body["checks"]["db"].startswith("down")
    assert "simulated DB outage" in body["checks"]["db"]
    assert body["checks"]["redis"] == "ok"
