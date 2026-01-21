"""Fetches upcoming company events (earnings dates)."""

import time
from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional

import requests
import yfinance as yf

from .config import Company, config


@dataclass
class UpcomingEvent:
    """Represents an upcoming company event."""
    company_name: str
    event_date: Optional[date]
    description: str
    event_type: str  # 'earnings', 'other'
    source: str = "unknown"  # 'ninjas', 'yahoo', 'none'


class EventsFetcher:
    """Fetches upcoming earnings dates for companies."""

    NINJAS_URL = "https://api.api-ninjas.com/v1/earningscalendar"

    def __init__(self, api_key: Optional[str] = None):
        self.ninjas_api_key = api_key or config.ninjas_api_key

    def _fetch_from_ninjas(self, company: Company) -> Optional[date]:
        """Try to fetch earnings date from API Ninjas.

        Args:
            company: Company to fetch for.

        Returns:
            Earnings date if found, None otherwise.
        """
        if not self.ninjas_api_key or not company.ticker:
            return None

        headers = {"X-Api-Key": self.ninjas_api_key}
        params = {"ticker": company.ticker}

        try:
            response = requests.get(
                self.NINJAS_URL,
                headers=headers,
                params=params,
                timeout=30
            )

            if response.status_code == 404:
                return None
            elif response.status_code == 403:
                print(f"    API Ninjas: Access forbidden")
                return None

            response.raise_for_status()
            data = response.json()

            # API returns a list of earnings events
            if data and isinstance(data, list):
                today = date.today()
                for event in data:
                    date_str = event.get("date")
                    if date_str:
                        try:
                            event_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                            # Return the first future date
                            if event_date >= today:
                                return event_date
                        except ValueError:
                            continue

        except requests.exceptions.RequestException as e:
            print(f"    API Ninjas error: {e}")

        return None

    def _fetch_from_yahoo(self, company: Company) -> Optional[date]:
        """Try to fetch earnings date from Yahoo Finance (fallback).

        Args:
            company: Company to fetch for.

        Returns:
            Earnings date if found, None otherwise.
        """
        if not company.ticker:
            return None

        try:
            ticker = yf.Ticker(company.ticker)

            # Try get_earnings_dates() method
            try:
                earnings_df = ticker.get_earnings_dates(limit=4)
                if earnings_df is not None and not earnings_df.empty:
                    today = date.today()
                    for earnings_timestamp in earnings_df.index:
                        if hasattr(earnings_timestamp, 'date'):
                            earnings_date = earnings_timestamp.date()
                            if earnings_date >= today:
                                return earnings_date
            except Exception:
                pass

            # Try calendar property
            try:
                calendar = ticker.calendar
                if calendar is not None:
                    if isinstance(calendar, dict):
                        earnings_date = calendar.get('Earnings Date')
                        if earnings_date:
                            if isinstance(earnings_date, list) and len(earnings_date) > 0:
                                earnings_date = earnings_date[0]
                            if hasattr(earnings_date, 'date'):
                                earnings_date = earnings_date.date()
                            if earnings_date and earnings_date >= date.today():
                                return earnings_date
            except Exception:
                pass

        except Exception:
            pass

        return None

    def fetch_company_events(self, company: Company) -> UpcomingEvent:
        """Fetch the next earnings date for a company.

        Uses API Ninjas as primary source, Yahoo Finance as fallback.

        Args:
            company: Company to fetch events for.

        Returns:
            The next upcoming earnings event, or a placeholder if unavailable.
        """
        if not company.ticker:
            return UpcomingEvent(
                company_name=company.name,
                event_date=None,
                description="Private company - no earnings data",
                event_type="other",
                source="none"
            )

        # Try API Ninjas first (primary source)
        earnings_date = self._fetch_from_ninjas(company)
        if earnings_date:
            return UpcomingEvent(
                company_name=company.name,
                event_date=earnings_date,
                description="Quarterly Earnings Release",
                event_type="earnings",
                source="ninjas"
            )

        # Fall back to Yahoo Finance
        print(f"    Trying Yahoo Finance fallback...")
        earnings_date = self._fetch_from_yahoo(company)
        if earnings_date:
            return UpcomingEvent(
                company_name=company.name,
                event_date=earnings_date,
                description="Quarterly Earnings Release",
                event_type="earnings",
                source="yahoo"
            )

        return UpcomingEvent(
            company_name=company.name,
            event_date=None,
            description="No upcoming earnings scheduled",
            event_type="other",
            source="none"
        )

    def fetch_all_companies(
        self,
        companies: list[Company],
        rate_limit_delay: float = 0.5
    ) -> dict[str, UpcomingEvent]:
        """Fetch upcoming events for all companies.

        Args:
            companies: List of companies.
            rate_limit_delay: Delay between API calls in seconds.

        Returns:
            Dict mapping company name to their next upcoming event.
        """
        events = {}

        for company in companies:
            print(f"Fetching upcoming events for {company.name}...")
            event = self.fetch_company_events(company)
            events[company.name] = event

            if event and event.event_date:
                print(f"  Next: {event.description} on {event.event_date} (via {event.source})")
            else:
                print(f"  No upcoming events found")

            # Rate limit to avoid hitting API limits
            if rate_limit_delay > 0:
                time.sleep(rate_limit_delay)

        return events
