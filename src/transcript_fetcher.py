"""Financial Modeling Prep API integration for fetching earnings call transcripts."""

import time
from datetime import date, datetime
from typing import Optional

import requests

from .config import Company, config
from .storage import EarningsTranscript


class TranscriptFetcher:
    """Fetches earnings call transcripts from Financial Modeling Prep API."""

    BASE_URL = "https://financialmodelingprep.com/api/v3/earning_call_transcript"

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or config.fmp_api_key

    def _get_quarter_string(self, year: int, quarter: int) -> str:
        """Convert year and quarter to string format like 'Q4 2025'."""
        return f"Q{quarter} {year}"

    def _parse_transcript_date(self, date_str: str) -> Optional[date]:
        """Parse date string from FMP API."""
        if not date_str:
            return None
        try:
            # FMP returns dates in various formats
            for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d"]:
                try:
                    return datetime.strptime(date_str, fmt).date()
                except ValueError:
                    continue
            return None
        except Exception:
            return None

    def fetch_company_transcripts(
        self,
        company: Company,
        company_id: int,
        quarters_back: int = 2
    ) -> list[EarningsTranscript]:
        """Fetch recent earnings call transcripts for a company.

        Args:
            company: Company to fetch transcripts for.
            company_id: Database ID of the company.
            quarters_back: How many quarters of transcripts to fetch (default 2).

        Returns:
            List of EarningsTranscript objects.
        """
        if not self.api_key:
            print(f"  Skipping {company.name}: FMP_API_KEY not configured")
            return []

        if not company.ticker:
            print(f"  Skipping {company.name}: No ticker (private company)")
            return []

        transcripts = []
        url = f"{self.BASE_URL}/{company.ticker}"

        params = {
            "apikey": self.api_key
        }

        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            if isinstance(data, dict) and data.get("Error Message"):
                print(f"  FMP API error for {company.name}: {data.get('Error Message')}")
                return transcripts

            if not isinstance(data, list):
                return transcripts

            # Get the most recent transcripts (up to quarters_back)
            for transcript_data in data[:quarters_back]:
                quarter = transcript_data.get("quarter")
                year = transcript_data.get("year")
                content = transcript_data.get("content", "")
                transcript_date_str = transcript_data.get("date", "")

                if not quarter or not year:
                    continue

                quarter_str = self._get_quarter_string(year, quarter)
                transcript_date = self._parse_transcript_date(transcript_date_str)

                transcripts.append(EarningsTranscript(
                    id=None,
                    company_id=company_id,
                    ticker=company.ticker,
                    quarter=quarter_str,
                    transcript_date=transcript_date,
                    transcript_text=content,
                    content_summary=None  # Will be filled by summarizer
                ))

        except requests.exceptions.RequestException as e:
            print(f"  Error fetching transcripts for {company.name}: {e}")

        return transcripts

    def fetch_all_companies(
        self,
        companies: list[Company],
        company_ids: dict[str, int],
        quarters_back: int = 2,
        rate_limit_delay: float = 0.5
    ) -> dict[str, list[EarningsTranscript]]:
        """Fetch transcripts for all companies.

        Args:
            companies: List of companies to fetch transcripts for.
            company_ids: Dict mapping company name to database ID.
            quarters_back: How many quarters of transcripts to fetch.
            rate_limit_delay: Delay between API calls in seconds.

        Returns:
            Dict mapping company name to list of transcripts.
        """
        if not self.api_key:
            print("FMP_API_KEY not configured - skipping transcript fetching")
            return {}

        all_transcripts = {}

        for company in companies:
            company_id = company_ids.get(company.name)
            if not company_id:
                print(f"No database ID for company: {company.name}")
                continue

            if not company.ticker:
                continue  # Skip private companies silently

            print(f"  Fetching transcripts for {company.name} ({company.ticker})...")
            transcripts = self.fetch_company_transcripts(
                company, company_id, quarters_back
            )
            all_transcripts[company.name] = transcripts

            if transcripts:
                print(f"    Found {len(transcripts)} transcript(s)")
            else:
                print(f"    No transcripts found")

            # Rate limit to avoid hitting API limits
            if rate_limit_delay > 0:
                time.sleep(rate_limit_delay)

        return all_transcripts
