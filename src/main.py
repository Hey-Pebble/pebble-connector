"""Entry point for the Pebble Database Connector."""

import asyncio
import logging
import sys

from src.agent import PebbleAgent, worker
from src.config import Config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    config = Config()

    missing = []
    if not config.PEBBLE_AGENT_API_KEY:
        missing.append("PEBBLE_AGENT_API_KEY")
    if not config.PEBBLE_COMPANY_ID:
        missing.append("PEBBLE_COMPANY_ID")
    if not config.GCP_PROJECT_ID:
        missing.append("GCP_PROJECT_ID")
    if not config.GCP_REGION:
        missing.append("GCP_REGION")
    if not config.GCP_INSTANCE_NAME:
        missing.append("GCP_INSTANCE_NAME")
    if not config.DB_NAME:
        missing.append("DB_NAME")
    if not config.DB_IAM_USER:
        missing.append("DB_IAM_USER")

    if missing:
        logger.error(f"Missing required environment variables: {', '.join(missing)}")
        sys.exit(1)

    logger.info("Starting Pebble Database Connector")
    logger.info(f"  Pebble API: {config.PEBBLE_API_URL}")
    logger.info(f"  Cloud SQL:  {config.instance_connection_name}")
    logger.info(f"  Database:   {config.DB_NAME}")
    logger.info(f"  IAM User:   {config.DB_IAM_USER}")
    logger.info(f"  Workers:    {config.NUM_WORKERS}")

    asyncio.run(run(config))


async def run(config: Config):
    agent = PebbleAgent(config)
    await agent.setup()

    try:
        workers = [worker(agent, i) for i in range(config.NUM_WORKERS)]
        await asyncio.gather(*workers)
    finally:
        await agent.cleanup()


if __name__ == "__main__":
    main()
