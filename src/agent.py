"""Pebble Database Agent - polls for query jobs and executes them via IAM auth."""

import asyncio
import json
import logging
import time
from typing import Any, Dict, Optional

import aiohttp
from google.cloud.sql.connector import Connector, IPTypes

from src.config import Config
from src.query_validator import validate_query

logger = logging.getLogger(__name__)


class PebbleAgent:
    def __init__(self, config: Config):
        self.config = config
        self.connector: Optional[Connector] = None

    async def setup(self):
        """Initialize Cloud SQL connector for IAM auth."""
        self.connector = Connector()
        logger.info(
            f"Initialized Cloud SQL connector for {self.config.instance_connection_name} "
            f"database={self.config.DB_NAME} user={self.config.DB_IAM_USER}"
        )

    async def get_connection(self):
        """Get a database connection via Cloud SQL IAM auth."""
        if not self.connector:
            raise RuntimeError("Connector not initialized")
        ip_type = getattr(IPTypes, self.config.IP_TYPE, IPTypes.PUBLIC)
        return await self.connector.connect_async(
            self.config.instance_connection_name,
            "asyncpg",
            user=self.config.DB_IAM_USER,
            db=self.config.DB_NAME,
            enable_iam_auth=True,
            ip_type=ip_type,
        )

    async def cleanup(self):
        if self.connector:
            await self.connector.close_async()

    async def poll_for_job(self, session: aiohttp.ClientSession) -> Optional[Dict[str, Any]]:
        """Poll Pebble backend for a pending query job."""
        url = f"{self.config.PEBBLE_API_URL}/pebble_app/agent/poll/"
        headers = {
            "Content-Type": "application/json",
            "X-Pebble-Agent-Key": self.config.PEBBLE_AGENT_API_KEY,
        }
        data = {"company_id": self.config.PEBBLE_COMPANY_ID}
        timeout = aiohttp.ClientTimeout(total=self.config.HTTP_TIMEOUT)

        try:
            async with session.post(url, headers=headers, json=data, timeout=timeout) as resp:
                if resp.status == 200:
                    result = await resp.json()
                    return result.get("job")
                elif resp.status == 401:
                    logger.error("Authentication failed - check PEBBLE_AGENT_API_KEY")
                    return None
                else:
                    text = await resp.text()
                    logger.warning(f"Poll returned {resp.status}: {text}")
                    return None
        except aiohttp.ClientError as e:
            logger.warning(f"Poll failed: {e}")
            return None

    async def execute_query(self, database_name: str, sql: str, timeout: int = 60) -> Dict[str, Any]:
        """Execute a query against the database."""
        is_valid, error = validate_query(sql)
        if not is_valid:
            raise ValueError(f"Query validation failed: {error}")

        conn = await asyncio.wait_for(self.get_connection(), timeout=self.config.CONNECTION_TIMEOUT)
        try:
            await conn.execute(f"SET statement_timeout = '{timeout * 1000}'")

            rows = await conn.fetch(sql)

            if not rows:
                return {"columns": [], "rows": [], "row_count": 0, "bytes": 0, "truncated": False}

            columns = list(rows[0].keys())
            result_rows = []
            total_bytes = 0
            truncated = False

            for row in rows:
                if len(result_rows) >= self.config.MAX_RESULT_ROWS:
                    truncated = True
                    break

                row_data = [self._serialize_value(row[col]) for col in columns]
                row_bytes = len(json.dumps(row_data).encode())

                if total_bytes + row_bytes > self.config.MAX_RESULT_BYTES:
                    truncated = True
                    break

                total_bytes += row_bytes
                result_rows.append(row_data)

            return {
                "columns": columns,
                "rows": result_rows,
                "row_count": len(result_rows),
                "bytes": total_bytes,
                "truncated": truncated,
            }
        finally:
            await conn.close()

    def _serialize_value(self, value: Any) -> Any:
        """Serialize a database value to JSON-compatible type."""
        if value is None:
            return None
        if isinstance(value, (int, float, str, bool)):
            return value
        return str(value)

    async def complete_job(self, session: aiohttp.ClientSession, job_id: str,
                           results: Optional[Dict] = None, error: Optional[str] = None,
                           execution_time_ms: int = 0):
        """Report job completion to Pebble."""
        url = f"{self.config.PEBBLE_API_URL}/pebble_app/agent/complete/"
        headers = {
            "Content-Type": "application/json",
            "X-Pebble-Agent-Key": self.config.PEBBLE_AGENT_API_KEY,
        }
        data: Dict[str, Any] = {
            "company_id": self.config.PEBBLE_COMPANY_ID,
            "job_id": job_id,
            "execution_time_ms": execution_time_ms,
        }
        if error:
            data["error"] = error
        elif results:
            data["results"] = results

        timeout = aiohttp.ClientTimeout(total=self.config.HTTP_TIMEOUT)
        try:
            async with session.post(url, headers=headers, json=data, timeout=timeout) as resp:
                if resp.status == 200:
                    logger.info(f"Job {job_id} completed successfully")
                else:
                    text = await resp.text()
                    logger.error(f"Failed to complete job {job_id}: {resp.status} {text}")
        except aiohttp.ClientError as e:
            logger.error(f"Failed to report job completion: {e}")


async def worker(agent: PebbleAgent, worker_id: int):
    """Single worker coroutine - polls, executes, completes in a loop."""
    logger.info(f"Worker {worker_id} started")
    consecutive_errors = 0

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                job = await agent.poll_for_job(session)

                if job:
                    logger.info(f"Worker {worker_id} claimed job {job['id']}: {job['sql'][:100]}")
                    start_time = time.time()

                    try:
                        results = await agent.execute_query(
                            job.get("database_name", ""),
                            job["sql"],
                            timeout=job.get("timeout_seconds", 60),
                        )
                        execution_time_ms = int((time.time() - start_time) * 1000)
                        await agent.complete_job(session, job["id"], results=results, execution_time_ms=execution_time_ms)
                    except Exception as e:
                        execution_time_ms = int((time.time() - start_time) * 1000)
                        logger.error(f"Worker {worker_id} query failed: {e}")
                        await agent.complete_job(session, job["id"], error=str(e), execution_time_ms=execution_time_ms)

                    consecutive_errors = 0
                else:
                    consecutive_errors = 0
                    await asyncio.sleep(agent.config.POLL_INTERVAL)

            except Exception as e:
                consecutive_errors += 1
                backoff = min(2 ** consecutive_errors, 60)
                logger.error(f"Worker {worker_id} error (retry in {backoff}s): {e}")
                await asyncio.sleep(backoff)
