"""SEC filings integration using the free SEC EDGAR API."""

import time
from datetime import datetime, timedelta
from typing import Optional

import requests

from .config import Company


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
    """Fetches SEC filings from the free SEC EDGAR API."""

    # SEC EDGAR API endpoints
    SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
    TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
    FILING_BASE_URL = "https://www.sec.gov/Archives/edgar/data"

    # Key filing types to monitor
    # Domestic companies: 10-K (annual), 10-Q (quarterly), 8-K (current events)
    # Foreign private issuers: 20-F (annual), 6-K (current reports)
    FILING_TYPES = [
        "10-K", "10-Q", "8-K",
        "10-K/A", "10-Q/A", "8-K/A",
        "20-F", "20-F/A",
        "6-K", "6-K/A",
    ]

    # Required User-Agent header for SEC EDGAR API
    HEADERS = {
        "User-Agent": "CompanyTracker/1.0 (contact@example.com)",
        "Accept-Encoding": "gzip, deflate",
    }

    def __init__(self):
        self._ticker_to_cik: Optional[dict[str, str]] = None

    def _load_ticker_map(self) -> dict[str, str]:
        """Load the ticker to CIK mapping from SEC."""
        if self._ticker_to_cik is not None:
            return self._ticker_to_cik

        try:
            response = requests.get(
                self.TICKERS_URL,
                headers=self.HEADERS,
                timeout=30
            )
            response.raise_for_status()
            data = response.json()

            # Build ticker -> CIK mapping
            # The JSON format is: {"0": {"cik_str": "...", "ticker": "...", "title": "..."}, ...}
            self._ticker_to_cik = {}
            for entry in data.values():
                ticker = entry.get("ticker", "").upper()
                cik = str(entry.get("cik_str", ""))
                if ticker and cik:
                    self._ticker_to_cik[ticker] = cik

            return self._ticker_to_cik

        except requests.exceptions.RequestException as e:
            print(f"Error loading SEC ticker map: {e}")
            self._ticker_to_cik = {}
            return self._ticker_to_cik

    def _get_cik(self, ticker: str) -> Optional[str]:
        """Get CIK for a ticker symbol."""
        ticker_map = self._load_ticker_map()
        return ticker_map.get(ticker.upper())

    def fetch_company_filings(
        self,
        company: Company,
        company_id: int,
        days_back: int = 30
    ) -> list[SECFiling]:
        """Fetch recent SEC filings for a company.

        Args:
            company: Company to fetch filings for.
            company_id: Database ID of the company.
            days_back: How many days back to search (default 30).

        Returns:
            List of SECFiling objects.
        """
        if not company.ticker:
            print(f"  No ticker symbol for {company.name}, skipping SEC filings")
            return []

        # Look up CIK for this ticker
        cik = self._get_cik(company.ticker)
        if not cik:
            print(f"  No SEC CIK found for {company.ticker} ({company.name})")
            return []

        filings = []
        from_date = datetime.utcnow() - timedelta(days=days_back)

        try:
            # Pad CIK to 10 digits as required by SEC API
            cik_padded = cik.zfill(10)
            url = self.SUBMISSIONS_URL.format(cik=cik_padded)

            response = requests.get(url, headers=self.HEADERS, timeout=30)
            response.raise_for_status()
            data = response.json()

            company_name = data.get("name", company.name)
            recent_filings = data.get("filings", {}).get("recent", {})

            # Extract parallel arrays
            form_types = recent_filings.get("form", [])
            filing_dates = recent_filings.get("filingDate", [])
            accession_numbers = recent_filings.get("accessionNumber", [])
            primary_documents = recent_filings.get("primaryDocument", [])

            # Process each filing
            for i, form_type in enumerate(form_types):
                if form_type not in self.FILING_TYPES:
                    continue

                # Parse filing date
                try:
                    filed_at = datetime.strptime(filing_dates[i], "%Y-%m-%d")
                except (ValueError, IndexError):
                    continue

                # Only include filings from the specified time period
                if filed_at < from_date:
                    continue

                accession = accession_numbers[i] if i < len(accession_numbers) else ""
                primary_doc = primary_documents[i] if i < len(primary_documents) else ""

                # Build URLs
                accession_no_dashes = accession.replace("-", "")
                filing_url = f"{self.FILING_BASE_URL}/{cik}/{accession_no_dashes}/{accession}-index.htm"
                document_url = None
                if primary_doc:
                    document_url = f"{self.FILING_BASE_URL}/{cik}/{accession_no_dashes}/{primary_doc}"

                filing = SECFiling(
                    company_id=company_id,
                    ticker=company.ticker,
                    company_name=company_name,
                    form_type=form_type,
                    filed_at=filed_at,
                    accession_number=accession,
                    filing_url=filing_url,
                    description=f"{form_type} filed on {filing_dates[i]}",
                    document_url=document_url,
                )
                filings.append(filing)

        except requests.exceptions.RequestException as e:
            print(f"  Error fetching SEC filings for {company.name}: {e}")

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
            response = requests.get(
                filing.document_url,
                headers=self.HEADERS,
                timeout=60
            )
            response.raise_for_status()
            content = response.text[:max_chars]
            return content
        except requests.exceptions.RequestException as e:
            print(f"  Error fetching filing content: {e}")
            return None

    def fetch_all_companies(
        self,
        companies: list[Company],
        company_ids: dict[str, int],
        days_back: int = 30,
        rate_limit_delay: float = 0.2
    ) -> dict[str, list[SECFiling]]:
        """Fetch SEC filings for all companies.

        Args:
            companies: List of companies to fetch filings for.
            company_ids: Dict mapping company name to database ID.
            days_back: How many days back to search.
            rate_limit_delay: Delay between API calls in seconds (SEC limit: 10 req/sec).

        Returns:
            Dict mapping company name to list of filings.
        """
        all_filings = {}

        # Pre-load ticker map once
        print("  Loading SEC ticker database...")
        self._load_ticker_map()

        for company in companies:
            company_id = company_ids.get(company.name)
            if not company_id:
                print(f"  No database ID for company: {company.name}")
                continue

            print(f"  Fetching SEC filings for {company.name}...")
            filings = self.fetch_company_filings(company, company_id, days_back)
            all_filings[company.name] = filings

            if filings:
                print(f"    Found {len(filings)} recent filings")

            # Respect SEC rate limits (max 10 requests per second)
            if rate_limit_delay > 0:
                time.sleep(rate_limit_delay)

        return all_filings
