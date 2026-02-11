import pytest

from src.config import Config


@pytest.fixture
def config(monkeypatch):
    """Create a Config with test defaults set via env vars."""
    monkeypatch.setenv("PEBBLE_API_URL", "https://test.pebble.example.com")
    monkeypatch.setenv("PEBBLE_AGENT_API_KEY", "pak_test_key_123")
    monkeypatch.setenv("GCP_PROJECT_ID", "test-project")
    monkeypatch.setenv("GCP_REGION", "us-central1")
    monkeypatch.setenv("GCP_INSTANCE_NAME", "test-instance")
    monkeypatch.setenv("DB_NAME", "testdb")
    monkeypatch.setenv("DB_IAM_USER", "sa@test-project.iam")
    # Reload the Config class so class-level attributes pick up the env vars
    import importlib
    import src.config
    importlib.reload(src.config)
    from src.config import Config as ReloadedConfig
    return ReloadedConfig()
