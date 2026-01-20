"""Fetches upcoming company events (earnings, SEC filings, etc.)."""

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Optional

import yfinance as yf

from .config import Company


@dataclass
class UpcomingEvent:
    """Represents an upcoming company event."""
    company_name: str
    event_date: Optional[date]
    description: str
    event_type: str  # 'earnings', 'sec_filing', 'dividend', 'other'


class EventsFetcher:
    """Fetches upcoming events for companies."""

    # Typical SEC filing deadlines (days after period end)
    # Large accelerated filers: 10-K (60 days), 10-Q (40 days)
    SEC_10K_DEADLINE_DAYS = 60
    SEC_10Q_DEADLINE_DAYS = 40

    def _get_next_quarter_end(self, from_date: date = None) -> date:
        """Get the next fiscal quarter end date."""
        if from_date is None:
            from_date = date.today()

        year = from_date.year
        month = from_date.month

        # Quarter end months: March, June, September, December
        quarter_ends = [
            date(year, 3, 31),
            date(year, 6, 30),
            date(year, 9, 30),
            date(year, 12, 31),
        ]

        for qe in quarter_ends:
            if qe > from_date:
                return qe

        # Next year's Q1
        return date(year + 1, 3, 31)

    def _estimate_next_10q_date(self, fiscal_year_end_month: int = 12) -> tuple[date, str]:
        """Estimate the next 10-Q filing deadline.

        Returns:
            Tuple of (estimated date, quarter description)
        """
        today = date.today()

        # Determine fiscal quarters based on fiscal year end
        # Most companies have Dec fiscal year end
        if fiscal_year_end_month == 12:
            quarters = [
                (3, 31, "Q1"),   # Q1 ends March 31
                (6, 30, "Q2"),   # Q2 ends June 30
                (9, 30, "Q3"),   # Q3 ends September 30
            ]
        else:
            # Simplified: assume calendar quarters for other fiscal years
            quarters = [
                (3, 31, "Q1"),
                (6, 30, "Q2"),
                (9, 30, "Q3"),
            ]

        year = today.year

        for month, day, quarter in quarters:
            quarter_end = date(year, month, day)
            filing_deadline = quarter_end + timedelta(days=self.SEC_10Q_DEADLINE_DAYS)

            if filing_deadline > today:
                return filing_deadline, f"{quarter} {year} 10-Q Filing"

        # Check next year's Q1
        quarter_end = date(year + 1, 3, 31)
        filing_deadline = quarter_end + timedelta(days=self.SEC_10Q_DEADLINE_DAYS)
        return filing_deadline, f"Q1 {year + 1} 10-Q Filing"

    def _estimate_next_10k_date(self, fiscal_year_end_month: int = 12) -> tuple[date, str]:
        """Estimate the next 10-K filing deadline.

        Returns:
            Tuple of (estimated date, description)
        """
        today = date.today()
        year = today.year

        # 10-K is due 60 days after fiscal year end
        if fiscal_year_end_month == 12:
            fiscal_year_end = date(year, 12, 31)
            if fiscal_year_end + timedelta(days=self.SEC_10K_DEADLINE_DAYS) < today:
                fiscal_year_end = date(year + 1, 12, 31)
        else:
            fiscal_year_end = date(year, fiscal_year_end_month, 28)  # Approximate
            if fiscal_year_end + timedelta(days=self.SEC_10K_DEADLINE_DAYS) < today:
                fiscal_year_end = date(year + 1, fiscal_year_end_month, 28)

        filing_deadline = fiscal_year_end + timedelta(days=self.SEC_10K_DEADLINE_DAYS)
        fy_year = fiscal_year_end.year

        return filing_deadline, f"FY{fy_year} 10-K Annual Report"

    def fetch_company_events(self, company: Company) -> Optional[UpcomingEvent]:
        """Fetch the next upcoming event for a company.

        Prioritizes: Earnings > SEC Filing deadlines

        Args:
            company: Company to fetch events for.

        Returns:
            The next upcoming event, or None.
        """
        if not company.ticker:
            return UpcomingEvent(
                company_name=company.name,
                event_date=None,
                description="No ticker - unable to fetch events",
                event_type="other"
            )

        events = []

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
                                events.append(UpcomingEvent(
                                    company_name=company.name,
                                    event_date=earnings_date,
                                    description="Quarterly Earnings Release",
                                    event_type="earnings"
                                ))

        except Exception as e:
            print(f"  Error fetching calendar for {company.name}: {e}")

        # Add estimated SEC filing deadlines
        try:
            # Estimate next 10-Q
            q_date, q_desc = self._estimate_next_10q_date()
            events.append(UpcomingEvent(
                company_name=company.name,
                event_date=q_date,
                description=q_desc,
                event_type="sec_filing"
            ))

            # Estimate next 10-K
            k_date, k_desc = self._estimate_next_10k_date()
            events.append(UpcomingEvent(
                company_name=company.name,
                event_date=k_date,
                description=k_desc,
                event_type="sec_filing"
            ))

        except Exception as e:
            print(f"  Error estimating SEC dates for {company.name}: {e}")

        # Return the soonest event
        if events:
            # Filter out events without dates and sort by date
            dated_events = [e for e in events if e.event_date is not None]
            if dated_events:
                dated_events.sort(key=lambda x: x.event_date)
                return dated_events[0]

        return UpcomingEvent(
            company_name=company.name,
            event_date=None,
            description="No upcoming events found",
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
