import os


class Config:
    # Pebble backend
    PEBBLE_API_URL = os.getenv("PEBBLE_API_URL", "")
    PEBBLE_AGENT_API_KEY = os.getenv("PEBBLE_AGENT_API_KEY", "")
    PEBBLE_COMPANY_ID = os.getenv("PEBBLE_COMPANY_ID", "")

    # GCP Cloud SQL connection (IAM auth - no password needed)
    GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "")
    GCP_REGION = os.getenv("GCP_REGION", "")
    GCP_INSTANCE_NAME = os.getenv("GCP_INSTANCE_NAME", "")
    DB_NAME = os.getenv("DB_NAME", "")
    DB_IAM_USER = os.getenv("DB_IAM_USER", "")  # IAM service account email

    # Connection settings
    IP_TYPE = os.getenv("IP_TYPE", "PRIVATE")  # PRIVATE (default for VPC), PUBLIC, or PSC

    # Worker settings
    NUM_WORKERS = int(os.getenv("NUM_WORKERS", "2"))
    POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "5"))

    @property
    def instance_connection_name(self) -> str:
        """Cloud SQL instance connection name: project:region:instance"""
        return f"{self.GCP_PROJECT_ID}:{self.GCP_REGION}:{self.GCP_INSTANCE_NAME}"
