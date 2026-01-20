"""Fetches upcoming company events (earnings dates from Yahoo Finance)."""

from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional

import yfinance as yf

from .config import Company


@dataclass
class UpcomingEvent:
    """Represents an upcoming company event."""
    company_name: str
    event_date: Optional[date]
    description: str
    event_type: str  # 'earnings', 'other'


class EventsFetcher:
    """Fetches upcoming earnings dates for companies."""

    def fetch_company_events(self, company: Company) -> Optional[UpcomingEvent]:
        """Fetch the next earnings date for a company.

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
                event_type="other"
            )

        try:
            ticker = yf.Ticker(company.ticker)

            # Try the newer get_earnings_dates() method first (yfinance 0.2.36+)
            try:
                earnings_df = ticker.get_earnings_dates(limit=4)
                if earnings_df is not None and not earnings_df.empty:
                    # The index contains the earnings dates as Timestamps
                    today = date.today()
                    for earnings_timestamp in earnings_df.index:
                        # Convert to date
                        if hasattr(earnings_timestamp, 'date'):
                            earnings_date = earnings_timestamp.date()
                        else:
                            continue

                        # Return the first future date
                        if earnings_date >= today:
                            return UpcomingEvent(
                                company_name=company.name,
                                event_date=earnings_date,
                                description="Quarterly Earnings Release",
                                event_type="earnings"
                            )
            except Exception:
                pass  # Fall through to try calendar method

            # Fallback to calendar property (older method)
            try:
                calendar = ticker.calendar
                if calendar is not None:
                    # Handle both dict and DataFrame formats
                    if isinstance(calendar, dict):
                        # Newer yfinance versions may return a dict
                        earnings_date = calendar.get('Earnings Date')
                        if earnings_date:
                            if isinstance(earnings_date, list) and len(earnings_date) > 0:
                                earnings_date = earnings_date[0]
                            if hasattr(earnings_date, 'date'):
                                earnings_date = earnings_date.date()
                            if earnings_date and earnings_date >= date.today():
                                return UpcomingEvent(
                                    company_name=company.name,
                                    event_date=earnings_date,
                                    description="Quarterly Earnings Release",
                                    event_type="earnings"
                                )
                    elif hasattr(calendar, 'empty') and not calendar.empty:
                        # DataFrame format
                        if 'Earnings Date' in calendar.index:
                            earnings_dates = calendar.loc['Earnings Date']
                            if earnings_dates is not None:
                                if hasattr(earnings_dates, 'iloc'):
                                    earnings_date = earnings_dates.iloc[0]
                                else:
                                    earnings_date = earnings_dates
                                if earnings_date is not None:
                                    if hasattr(earnings_date, 'date'):
                                        earnings_date = earnings_date.date()
                                    if earnings_date and earnings_date >= date.today():
                                        return UpcomingEvent(
                                            company_name=company.name,
                                            event_date=earnings_date,
                                            description="Quarterly Earnings Release",
                                            event_type="earnings"
                                        )
            except Exception:
                pass  # Calendar not available

        except Exception as e:
            print(f"  Error fetching earnings for {company.name}: {e}")
            return UpcomingEvent(
                company_name=company.name,
                event_date=None,
                description="Unable to fetch earnings data",
                event_type="other"
            )

        return UpcomingEvent(
            company_name=company.name,
            event_date=None,
            description="No upcoming earnings scheduled",
            event_type="other"
        )

    def fetch_all_companies(
        self,
        companies: list[Company]
    ) -> dict[str, UpcomingEvent]:
        """Fetch upcoming events for all companies.

        Args:
            companies: List of companies.

        Returns:
            Dict mapping company name to their next upcoming event.
        """
        events = {}

        for company in companies:
            print(f"Fetching upcoming events for {company.name}...")
            event = self.fetch_company_events(company)
            events[company.name] = event

            if event and event.event_date:
                print(f"  Next: {event.description} on {event.event_date}")
            else:
                print(f"  No upcoming events found")

        return events
