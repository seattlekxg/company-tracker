"""Configuration and company list for the tracker."""

import os
from dataclasses import dataclass
from typing import Optional

from dotenv import load_dotenv

# Load environment variables from .env file if present
load_dotenv()


@dataclass
class Company:
    """Represents a company to track."""
    name: str
    ticker: Optional[str] = None
    keywords: Optional[list[str]] = None

    def get_search_terms(self) -> list[str]:
        """Get all search terms for this company."""
        terms = [self.name]
        if self.keywords:
            terms.extend(self.keywords)
        return terms


# Default list of companies to track
# Focus: Data center infrastructure and power management companies
COMPANIES = [
    Company(
        name="Eaton",
        ticker="ETN",
        keywords=["Eaton Corporation", "Eaton data center", "Eaton UPS", "Eaton power distribution"]
    ),
    Company(
        name="Schneider Electric",
        ticker="SBGSY",
        keywords=["Schneider Electric data center", "Schneider UPS", "APC data center", "EcoStruxure"]
    ),
    Company(
        name="Cummins",
        ticker="CMI",
        keywords=["Cummins data center", "Cummins generator", "Cummins power generation"]
    ),
    Company(
        name="Caterpillar",
        ticker="CAT",
        keywords=["Caterpillar data center", "Cat generator", "Caterpillar power systems"]
    ),
]


class Config:
    """Application configuration loaded from environment variables."""

    def __init__(self):
        self.newsapi_key = os.getenv("NEWSAPI_KEY", "")
        self.anthropic_api_key = os.getenv("ANTHROPIC_API_KEY", "")
        self.sendgrid_api_key = os.getenv("SENDGRID_API_KEY", "")
        self.email_to = os.getenv("EMAIL_TO", "")
        self.email_from = os.getenv("EMAIL_FROM", "")

        # Database path
        self.db_path = os.getenv(
            "DB_PATH",
            os.path.join(os.path.dirname(__file__), "..", "data", "tracker.db")
        )

    def validate(self) -> list[str]:
        """Validate that required configuration is present.

        Returns:
            List of missing configuration keys.
        """
        missing = []
        if not self.newsapi_key:
            missing.append("NEWSAPI_KEY")
        if not self.anthropic_api_key:
            missing.append("ANTHROPIC_API_KEY")
        if not self.sendgrid_api_key:
            missing.append("SENDGRID_API_KEY")
        if not self.email_to:
            missing.append("EMAIL_TO")
        if not self.email_from:
            missing.append("EMAIL_FROM")
        return missing


# Global config instance
config = Config()
