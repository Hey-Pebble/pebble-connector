"""Tests for the Pebble agent HTTP interactions."""

import asyncio
import uuid
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest
import pytest_asyncio
from aioresponses import aioresponses

from src.agent import PebbleAgent, worker


@pytest.fixture
def agent(config):
    return PebbleAgent(config)


@pytest.fixture
def mock_aiohttp():
    with aioresponses() as m:
        yield m


class TestPollForJob:
    @pytest.mark.asyncio
    async def test_poll_returns_job(self, agent, mock_aiohttp):
        job = {"id": "job-123", "sql": "SELECT 1", "database_name": "testdb"}
        mock_aiohttp.post(
            f"{agent.config.PEBBLE_API_URL}/pebble_app/agent/poll/",
            payload={"job": job},
        )
        async with aiohttp.ClientSession() as session:
            result = await agent.poll_for_job(session)
        assert result == job

    @pytest.mark.asyncio
    async def test_poll_returns_none_when_no_job(self, agent, mock_aiohttp):
        mock_aiohttp.post(
            f"{agent.config.PEBBLE_API_URL}/pebble_app/agent/poll/",
            payload={"job": None},
        )
        async with aiohttp.ClientSession() as session:
            result = await agent.poll_for_job(session)
        assert result is None

    @pytest.mark.asyncio
    async def test_poll_auth_failure(self, agent, mock_aiohttp):
        mock_aiohttp.post(
            f"{agent.config.PEBBLE_API_URL}/pebble_app/agent/poll/",
            status=401,
        )
        async with aiohttp.ClientSession() as session:
            result = await agent.poll_for_job(session)
        assert result is None

    @pytest.mark.asyncio
    async def test_poll_server_error(self, agent, mock_aiohttp):
        mock_aiohttp.post(
            f"{agent.config.PEBBLE_API_URL}/pebble_app/agent/poll/",
            status=500,
            body="Internal Server Error",
        )
        async with aiohttp.ClientSession() as session:
            result = await agent.poll_for_job(session)
        assert result is None

    @pytest.mark.asyncio
    async def test_poll_network_error(self, agent, mock_aiohttp):
        mock_aiohttp.post(
            f"{agent.config.PEBBLE_API_URL}/pebble_app/agent/poll/",
            exception=aiohttp.ClientError("connection refused"),
        )
        async with aiohttp.ClientSession() as session:
            result = await agent.poll_for_job(session)
        assert result is None


class TestCompleteJob:
    @pytest.mark.asyncio
    async def test_complete_job_success(self, agent, mock_aiohttp):
        mock_aiohttp.post(
            f"{agent.config.PEBBLE_API_URL}/pebble_app/agent/complete/",
            payload={"status": "ok"},
        )
        results = {"columns": ["id"], "rows": [[1]], "row_count": 1}
        async with aiohttp.ClientSession() as session:
            await agent.complete_job(session, "job-123", results=results, execution_time_ms=42)

    @pytest.mark.asyncio
    async def test_complete_job_with_error(self, agent, mock_aiohttp):
        mock_aiohttp.post(
            f"{agent.config.PEBBLE_API_URL}/pebble_app/agent/complete/",
            payload={"status": "ok"},
        )
        async with aiohttp.ClientSession() as session:
            await agent.complete_job(session, "job-123", error="timeout", execution_time_ms=60000)

    @pytest.mark.asyncio
    async def test_complete_job_network_error(self, agent, mock_aiohttp):
        mock_aiohttp.post(
            f"{agent.config.PEBBLE_API_URL}/pebble_app/agent/complete/",
            exception=aiohttp.ClientError("connection refused"),
        )
        async with aiohttp.ClientSession() as session:
            # Should not raise
            await agent.complete_job(session, "job-123", error="test")


class TestExecuteQuery:
    @pytest.mark.asyncio
    async def test_execute_query_rejects_write(self, agent):
        with pytest.raises(ValueError, match="validation failed"):
            await agent.execute_query("testdb", "DROP TABLE users")


class TestSerializeValue:
    def test_none(self, agent):
        assert agent._serialize_value(None) is None

    def test_int(self, agent):
        assert agent._serialize_value(42) == 42

    def test_float(self, agent):
        assert agent._serialize_value(3.14) == 3.14

    def test_str(self, agent):
        assert agent._serialize_value("hello") == "hello"

    def test_bool(self, agent):
        assert agent._serialize_value(True) is True

    def test_datetime(self, agent):
        dt = datetime(2026, 1, 15, 10, 30)
        assert agent._serialize_value(dt) == "2026-01-15 10:30:00"

    def test_uuid(self, agent):
        u = uuid.UUID("12345678-1234-5678-1234-567812345678")
        assert agent._serialize_value(u) == "12345678-1234-5678-1234-567812345678"


class TestHeaders:
    def test_headers_without_iap(self, config):
        config.IAP_CLIENT_ID = ""
        agent = PebbleAgent(config)
        headers = agent._get_headers()
        assert "X-Pebble-Agent-Key" in headers
        assert "Authorization" not in headers

    def test_headers_with_iap(self, config):
        config.IAP_CLIENT_ID = "test-client-id.apps.googleusercontent.com"
        agent = PebbleAgent(config)
        with patch.object(agent, "_get_iap_token", return_value="mock-iap-token"):
            headers = agent._get_headers()
        assert headers["Authorization"] == "Bearer mock-iap-token"
        assert headers["X-Pebble-Agent-Key"] == config.PEBBLE_AGENT_API_KEY


class TestWorkerLoop:
    @pytest.mark.asyncio
    async def test_worker_executes_and_completes(self, agent, mock_aiohttp):
        job = {"id": "job-1", "sql": "SELECT 1", "database_name": "testdb"}
        # First poll returns a job, second raises to break the loop
        mock_aiohttp.post(
            f"{agent.config.PEBBLE_API_URL}/pebble_app/agent/poll/",
            payload={"job": job},
        )
        mock_aiohttp.post(
            f"{agent.config.PEBBLE_API_URL}/pebble_app/agent/complete/",
            payload={"status": "ok"},
        )
        mock_aiohttp.post(
            f"{agent.config.PEBBLE_API_URL}/pebble_app/agent/poll/",
            exception=KeyboardInterrupt(),
        )

        mock_results = {"columns": ["id"], "rows": [[1]], "row_count": 1, "bytes": 10, "truncated": False}
        with patch.object(agent, "execute_query", new_callable=AsyncMock, return_value=mock_results):
            with pytest.raises(KeyboardInterrupt):
                await worker(agent, 0)

    @pytest.mark.asyncio
    async def test_worker_handles_query_error(self, agent, mock_aiohttp):
        job = {"id": "job-2", "sql": "SELECT 1", "database_name": "testdb"}
        mock_aiohttp.post(
            f"{agent.config.PEBBLE_API_URL}/pebble_app/agent/poll/",
            payload={"job": job},
        )
        mock_aiohttp.post(
            f"{agent.config.PEBBLE_API_URL}/pebble_app/agent/complete/",
            payload={"status": "ok"},
        )
        mock_aiohttp.post(
            f"{agent.config.PEBBLE_API_URL}/pebble_app/agent/poll/",
            exception=KeyboardInterrupt(),
        )

        with patch.object(agent, "execute_query", new_callable=AsyncMock, side_effect=RuntimeError("db error")):
            with pytest.raises(KeyboardInterrupt):
                await worker(agent, 0)
