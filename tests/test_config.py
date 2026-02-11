"""Tests for configuration loading."""

import importlib

import src.config


def _reload_config(monkeypatch, **env_vars):
    """Set env vars and reload config module to pick up changes."""
    for key, val in env_vars.items():
        monkeypatch.setenv(key, val)
    importlib.reload(src.config)
    return src.config.Config()


class TestConfigDefaults:
    def test_default_num_workers(self, monkeypatch):
        monkeypatch.delenv("NUM_WORKERS", raising=False)
        importlib.reload(src.config)
        assert src.config.Config.NUM_WORKERS == 5

    def test_default_poll_interval(self, monkeypatch):
        monkeypatch.delenv("POLL_INTERVAL", raising=False)
        importlib.reload(src.config)
        assert src.config.Config.POLL_INTERVAL == 5

    def test_default_max_result_rows(self, monkeypatch):
        monkeypatch.delenv("MAX_RESULT_ROWS", raising=False)
        importlib.reload(src.config)
        assert src.config.Config.MAX_RESULT_ROWS == 1000

    def test_default_empty_strings(self, monkeypatch):
        monkeypatch.delenv("PEBBLE_API_URL", raising=False)
        monkeypatch.delenv("PEBBLE_AGENT_API_KEY", raising=False)
        importlib.reload(src.config)
        assert src.config.Config.PEBBLE_API_URL == ""
        assert src.config.Config.PEBBLE_AGENT_API_KEY == ""


class TestConfigCustom:
    def test_custom_num_workers(self, monkeypatch):
        config = _reload_config(monkeypatch, NUM_WORKERS="10")
        assert config.NUM_WORKERS == 10

    def test_instance_connection_name(self, monkeypatch):
        config = _reload_config(
            monkeypatch,
            GCP_PROJECT_ID="my-project",
            GCP_REGION="us-east1",
            GCP_INSTANCE_NAME="my-db",
        )
        assert config.instance_connection_name == "my-project:us-east1:my-db"
