"""API Ninjas integration for fetching earnings call transcripts."""

import time
from datetime import date, datetime
from typing import Optional

import requests

from .config import Company, config
from .storage import EarningsTranscript


class TranscriptFetcher:
    """Fetches earnings call transcripts from API Ninjas."""

    BASE_URL = "https://api.api-ninjas.com/v1/earningstranscript"

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or config.ninjas_api_key

    def _get_quarter_string(self, year: int, quarter: int) -> str:
        """Convert year and quarter to string format like 'Q4 2025'."""
        return f"Q{quarter} {year}"

    def _parse_transcript_date(self, date_str: str) -> Optional[date]:
        """Parse date string from API Ninjas."""
        if not date_str:
            return None
        try:
            # API Ninjas returns dates in YYYY-MM-DD format
            return datetime.strptime(date_str, "%Y-%m-%d").date()
        except (ValueError, TypeError):
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
            print(f"  Skipping {company.name}: NINJAS_API_KEY not configured")
            return []

        if not company.ticker:
            print(f"  Skipping {company.name}: No ticker (private company)")
            return []

        transcripts = []
        headers = {
            "X-Api-Key": self.api_key
        }

        # Calculate which quarters to fetch
        today = date.today()
        current_year = today.year
        current_quarter = (today.month - 1) // 3 + 1

        quarters_to_fetch = []
        year, quarter = current_year, current_quarter
        for _ in range(quarters_back):
            quarters_to_fetch.append((year, quarter))
            quarter -= 1
            if quarter == 0:
                quarter = 4
                year -= 1

        for year, quarter in quarters_to_fetch:
            params = {
                "ticker": company.ticker,
                "year": year,
                "quarter": quarter
            }

            try:
                response = requests.get(
                    self.BASE_URL,
                    headers=headers,
                    params=params,
                    timeout=30
                )

                if response.status_code == 404:
                    # No transcript for this quarter
                    continue
                elif response.status_code == 400:
                    # Invalid request (might be unsupported ticker)
                    continue
                elif response.status_code == 403:
                    print(f"  API Ninjas error: Access forbidden (check API key)")
                    return transcripts

                response.raise_for_status()
                data = response.json()

                # API returns a single transcript object
                if data and data.get("transcript"):
                    quarter_str = self._get_quarter_string(
                        data.get("year", year),
                        data.get("quarter", quarter)
                    )
                    transcript_date = self._parse_transcript_date(data.get("date"))

                    transcripts.append(EarningsTranscript(
                        id=None,
                        company_id=company_id,
                        ticker=company.ticker,
                        quarter=quarter_str,
                        transcript_date=transcript_date,
                        transcript_text=data.get("transcript"),
                        content_summary=None  # Will be filled by summarizer
                    ))

            except requests.exceptions.RequestException as e:
                print(f"  Error fetching transcript for {company.name} Q{quarter} {year}: {e}")

            # Small delay between quarter requests
            time.sleep(0.2)

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
            print("NINJAS_API_KEY not configured - skipping transcript fetching")
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
