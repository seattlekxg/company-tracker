"""SEC filings integration using sec-api.io."""

import time
from datetime import datetime, timedelta
from typing import Optional

import requests

from .config import Company, config


class SECFiling:
    """Represents an SEC filing."""

    def __init__(
        self,
        company_id: int,
        ticker: str,
        company_name: str,
        form_type: str,
        filed_at: datetime,
        accession_number: str,
        filing_url: str,
        description: Optional[str] = None,
        document_url: Optional[str] = None,
    ):
        self.id = None
        self.company_id = company_id
        self.ticker = ticker
        self.company_name = company_name
        self.form_type = form_type
        self.filed_at = filed_at
        self.accession_number = accession_number
        self.filing_url = filing_url
        self.description = description
        self.document_url = document_url
        self.content_summary = None  # Will be populated by AI analysis


class SECFetcher:
    """Fetches SEC filings from sec-api.io."""

    BASE_URL = "https://api.sec-api.io"

    # Key filing types to monitor
    FILING_TYPES = [
        "10-K",      # Annual report
        "10-Q",      # Quarterly report
        "8-K",       # Current report (material events)
        "10-K/A",    # Amended annual report
        "10-Q/A",    # Amended quarterly report
        "8-K/A",     # Amended current report
    ]

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or config.sec_api_key
        if not self.api_key:
            raise ValueError("SEC API key is required")

    def fetch_company_filings(
        self,
        company: Company,
        company_id: int,
        days_back: int = 7
    ) -> list[SECFiling]:
        """Fetch recent SEC filings for a company.

        Args:
            company: Company to fetch filings for.
            company_id: Database ID of the company.
            days_back: How many days back to search (default 7).

        Returns:
            List of SECFiling objects.
        """
        if not company.ticker:
            print(f"No ticker symbol for {company.name}, skipping SEC filings")
            return []

        filings = []
        from_date = datetime.utcnow() - timedelta(days=days_back)

        # Build the query for sec-api.io
        query = {
            "query": {
                "query_string": {
                    "query": f'ticker:{company.ticker} AND formType:({" OR ".join(self.FILING_TYPES)})'
                }
            },
            "from": "0",
            "size": "10",
            "sort": [{"filedAt": {"order": "desc"}}]
        }

        headers = {
            "Authorization": self.api_key,
            "Content-Type": "application/json"
        }

        try:
            response = requests.post(
                self.BASE_URL,
                json=query,
                headers=headers,
                timeout=30
            )
            response.raise_for_status()
            data = response.json()

            for filing_data in data.get("filings", []):
                filed_at_str = filing_data.get("filedAt", "")
                try:
                    filed_at = datetime.fromisoformat(filed_at_str.replace("Z", "+00:00"))
                except ValueError:
                    filed_at = datetime.utcnow()

                # Only include filings from the specified time period
                if filed_at < from_date:
                    continue

                filing = SECFiling(
                    company_id=company_id,
                    ticker=filing_data.get("ticker", company.ticker),
                    company_name=filing_data.get("companyName", company.name),
                    form_type=filing_data.get("formType", "Unknown"),
                    filed_at=filed_at,
                    accession_number=filing_data.get("accessionNo", ""),
                    filing_url=filing_data.get("linkToFilingDetails", ""),
                    description=filing_data.get("description"),
                    document_url=filing_data.get("linkToTxt"),
                )
                filings.append(filing)

        except requests.exceptions.RequestException as e:
            print(f"Error fetching SEC filings for {company.name}: {e}")

        return filings

    def fetch_filing_content(self, filing: SECFiling, max_chars: int = 50000) -> Optional[str]:
        """Fetch the text content of an SEC filing.

        Args:
            filing: The filing to fetch content for.
            max_chars: Maximum characters to retrieve.

        Returns:
            Filing text content or None.
        """
        if not filing.document_url:
            return None

        try:
            response = requests.get(filing.document_url, timeout=60)
            response.raise_for_status()
            content = response.text[:max_chars]
            return content
        except requests.exceptions.RequestException as e:
            print(f"Error fetching filing content: {e}")
            return None

    def fetch_all_companies(
        self,
        companies: list[Company],
        company_ids: dict[str, int],
        days_back: int = 7,
        rate_limit_delay: float = 0.5
    ) -> dict[str, list[SECFiling]]:
        """Fetch SEC filings for all companies.

        Args:
            companies: List of companies to fetch filings for.
            company_ids: Dict mapping company name to database ID.
            days_back: How many days back to search.
            rate_limit_delay: Delay between API calls in seconds.

        Returns:
            Dict mapping company name to list of filings.
        """
        all_filings = {}

        for company in companies:
            company_id = company_ids.get(company.name)
            if not company_id:
                print(f"No database ID for company: {company.name}")
                continue

            print(f"Fetching SEC filings for {company.name}...")
            filings = self.fetch_company_filings(company, company_id, days_back)
            all_filings[company.name] = filings
            print(f"  Found {len(filings)} recent filings")

            if rate_limit_delay > 0:
                time.sleep(rate_limit_delay)

        return all_filings
