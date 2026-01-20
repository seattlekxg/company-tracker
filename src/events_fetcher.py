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
            calendar = ticker.calendar

            # Check for earnings date
            if calendar is not None and not calendar.empty:
                # calendar can be a DataFrame with earnings dates
                if 'Earnings Date' in calendar.index:
                    earnings_dates = calendar.loc['Earnings Date']
                    if earnings_dates is not None:
                        # Could be a single date or range
                        if hasattr(earnings_dates, 'iloc'):
                            earnings_date = earnings_dates.iloc[0]
                        else:
                            earnings_date = earnings_dates

                        if earnings_date is not None:
                            # Convert to date if it's a Timestamp
                            if hasattr(earnings_date, 'date'):
                                earnings_date = earnings_date.date()
                            elif isinstance(earnings_date, str):
                                try:
                                    earnings_date = datetime.strptime(earnings_date, "%Y-%m-%d").date()
                                except ValueError:
                                    earnings_date = None

                            if earnings_date and earnings_date >= date.today():
                                return UpcomingEvent(
                                    company_name=company.name,
                                    event_date=earnings_date,
                                    description="Quarterly Earnings Release",
                                    event_type="earnings"
                                )

        except Exception as e:
            print(f"  Error fetching calendar for {company.name}: {e}")
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
