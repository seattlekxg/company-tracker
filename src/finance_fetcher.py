"""Yahoo Finance integration for fetching stock data."""

from datetime import date
from typing import Optional

import yfinance as yf

from .config import Company
from .storage import FinancialSnapshot


class FinanceFetcher:
    """Fetches financial data using yfinance."""

    def fetch_company_data(
        self,
        company: Company,
        company_id: int
    ) -> Optional[FinancialSnapshot]:
        """Fetch current financial data for a company.

        Args:
            company: Company to fetch data for.
            company_id: Database ID of the company.

        Returns:
            FinancialSnapshot or None if data couldn't be fetched.
        """
        if not company.ticker:
            print(f"No ticker symbol for {company.name}, skipping financial data")
            return None

        try:
            ticker = yf.Ticker(company.ticker)
            info = ticker.info

            # Get current price and change
            current_price = info.get("currentPrice") or info.get("regularMarketPrice")
            previous_close = info.get("previousClose") or info.get("regularMarketPreviousClose")

            change_percent = None
            if current_price and previous_close and previous_close > 0:
                change_percent = ((current_price - previous_close) / previous_close) * 100

            # Build raw data dict with additional info
            raw_data = {
                "currency": info.get("currency"),
                "exchange": info.get("exchange"),
                "sector": info.get("sector"),
                "industry": info.get("industry"),
                "shortName": info.get("shortName"),
                "dayHigh": info.get("dayHigh"),
                "dayLow": info.get("dayLow"),
                "fiftyDayAverage": info.get("fiftyDayAverage"),
                "twoHundredDayAverage": info.get("twoHundredDayAverage"),
                "trailingPE": info.get("trailingPE"),
                "forwardPE": info.get("forwardPE"),
                "dividendYield": info.get("dividendYield"),
                "beta": info.get("beta"),
            }

            # Remove None values
            raw_data = {k: v for k, v in raw_data.items() if v is not None}

            return FinancialSnapshot(
                id=None,
                company_id=company_id,
                date=date.today(),
                price=current_price,
                change_percent=change_percent,
                volume=info.get("volume") or info.get("regularMarketVolume"),
                market_cap=info.get("marketCap"),
                high_52w=info.get("fiftyTwoWeekHigh"),
                low_52w=info.get("fiftyTwoWeekLow"),
                raw_data=raw_data if raw_data else None
            )

        except Exception as e:
            print(f"Error fetching financial data for {company.name} ({company.ticker}): {e}")
            return None

    def fetch_all_companies(
        self,
        companies: list[Company],
        company_ids: dict[str, int]
    ) -> dict[str, Optional[FinancialSnapshot]]:
        """Fetch financial data for all companies.

        Args:
            companies: List of companies to fetch data for.
            company_ids: Dict mapping company name to database ID.

        Returns:
            Dict mapping company name to FinancialSnapshot (or None).
        """
        snapshots = {}

        for company in companies:
            company_id = company_ids.get(company.name)
            if not company_id:
                print(f"No database ID for company: {company.name}")
                continue

            print(f"Fetching financial data for {company.name}...")
            snapshot = self.fetch_company_data(company, company_id)
            snapshots[company.name] = snapshot

            if snapshot:
                print(f"  Price: ${snapshot.price:.2f} ({snapshot.change_percent:+.2f}%)"
                      if snapshot.price and snapshot.change_percent
                      else "  Data retrieved")
            else:
                print("  No data available")

        return snapshots
