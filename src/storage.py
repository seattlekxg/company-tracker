"""SQLite database operations for the tracker."""

import json
import os
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional

from .config import COMPANIES, Company, config


@dataclass
class NewsArticle:
    """Represents a news article."""
    id: Optional[int]
    company_id: int
    title: str
    description: Optional[str]
    source: Optional[str]
    url: str
    published_at: Optional[datetime]
    fetched_at: Optional[datetime] = None


@dataclass
class FinancialSnapshot:
    """Represents a financial data snapshot."""
    id: Optional[int]
    company_id: int
    date: date
    price: Optional[float]
    change_percent: Optional[float]
    volume: Optional[int]
    market_cap: Optional[float]
    high_52w: Optional[float]
    low_52w: Optional[float]
    raw_data: Optional[dict] = None


@dataclass
class DailySummary:
    """Represents a daily AI-generated summary."""
    id: Optional[int]
    date: date
    summary_text: str
    email_sent: bool = False
    created_at: Optional[datetime] = None


@dataclass
class SECFilingRecord:
    """Represents an SEC filing record in the database."""
    id: Optional[int]
    company_id: int
    form_type: str
    filed_at: datetime
    accession_number: str
    filing_url: str
    description: Optional[str]
    content_summary: Optional[str]
    fetched_at: Optional[datetime] = None


class Storage:
    """SQLite storage handler."""

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or config.db_path
        # Ensure the data directory exists
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        """Get a database connection."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        """Initialize the database schema."""
        conn = self._get_connection()
        cursor = conn.cursor()

        # Create tables
        cursor.executescript("""
            CREATE TABLE IF NOT EXISTS companies (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                ticker TEXT,
                keywords TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS news_articles (
                id INTEGER PRIMARY KEY,
                company_id INTEGER REFERENCES companies(id),
                title TEXT NOT NULL,
                description TEXT,
                source TEXT,
                url TEXT UNIQUE,
                published_at TIMESTAMP,
                fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS financial_snapshots (
                id INTEGER PRIMARY KEY,
                company_id INTEGER REFERENCES companies(id),
                date DATE,
                price REAL,
                change_percent REAL,
                volume INTEGER,
                market_cap REAL,
                high_52w REAL,
                low_52w REAL,
                raw_data TEXT,
                UNIQUE(company_id, date)
            );

            CREATE TABLE IF NOT EXISTS daily_summaries (
                id INTEGER PRIMARY KEY,
                date DATE UNIQUE,
                summary_text TEXT,
                email_sent BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS sec_filings (
                id INTEGER PRIMARY KEY,
                company_id INTEGER REFERENCES companies(id),
                form_type TEXT NOT NULL,
                filed_at TIMESTAMP,
                accession_number TEXT UNIQUE,
                filing_url TEXT,
                description TEXT,
                content_summary TEXT,
                fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE INDEX IF NOT EXISTS idx_news_company ON news_articles(company_id);
            CREATE INDEX IF NOT EXISTS idx_news_published ON news_articles(published_at);
            CREATE INDEX IF NOT EXISTS idx_financial_date ON financial_snapshots(date);
            CREATE INDEX IF NOT EXISTS idx_sec_company ON sec_filings(company_id);
            CREATE INDEX IF NOT EXISTS idx_sec_filed ON sec_filings(filed_at);
        """)

        conn.commit()
        conn.close()

    def sync_companies(self, companies: list[Company]) -> dict[str, int]:
        """Sync companies from config to database.

        Returns:
            Dict mapping company name to database ID.
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        company_ids = {}

        for company in companies:
            # Check if company exists
            cursor.execute(
                "SELECT id FROM companies WHERE name = ?",
                (company.name,)
            )
            row = cursor.fetchone()

            if row:
                company_ids[company.name] = row["id"]
                # Update keywords if changed
                cursor.execute(
                    "UPDATE companies SET ticker = ?, keywords = ? WHERE id = ?",
                    (
                        company.ticker,
                        json.dumps(company.keywords) if company.keywords else None,
                        row["id"]
                    )
                )
            else:
                # Insert new company
                cursor.execute(
                    "INSERT INTO companies (name, ticker, keywords) VALUES (?, ?, ?)",
                    (
                        company.name,
                        company.ticker,
                        json.dumps(company.keywords) if company.keywords else None
                    )
                )
                company_ids[company.name] = cursor.lastrowid

        conn.commit()
        conn.close()
        return company_ids

    def save_article(self, article: NewsArticle) -> bool:
        """Save a news article if it doesn't already exist.

        Returns:
            True if article was saved, False if it was a duplicate.
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute(
                """
                INSERT INTO news_articles
                    (company_id, title, description, source, url, published_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    article.company_id,
                    article.title,
                    article.description,
                    article.source,
                    article.url,
                    article.published_at.isoformat() if article.published_at else None
                )
            )
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            # Duplicate URL
            return False
        finally:
            conn.close()

    def save_financial_snapshot(self, snapshot: FinancialSnapshot) -> bool:
        """Save a financial snapshot for a company on a given date.

        Returns:
            True if snapshot was saved/updated.
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            INSERT OR REPLACE INTO financial_snapshots
                (company_id, date, price, change_percent, volume,
                 market_cap, high_52w, low_52w, raw_data)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                snapshot.company_id,
                snapshot.date.isoformat(),
                snapshot.price,
                snapshot.change_percent,
                snapshot.volume,
                snapshot.market_cap,
                snapshot.high_52w,
                snapshot.low_52w,
                json.dumps(snapshot.raw_data) if snapshot.raw_data else None
            )
        )
        conn.commit()
        conn.close()
        return True

    def get_articles_by_date(
        self,
        target_date: date,
        company_id: Optional[int] = None
    ) -> list[NewsArticle]:
        """Get articles fetched on a specific date."""
        conn = self._get_connection()
        cursor = conn.cursor()

        if company_id:
            cursor.execute(
                """
                SELECT * FROM news_articles
                WHERE date(fetched_at) = ? AND company_id = ?
                ORDER BY published_at DESC
                """,
                (target_date.isoformat(), company_id)
            )
        else:
            cursor.execute(
                """
                SELECT * FROM news_articles
                WHERE date(fetched_at) = ?
                ORDER BY published_at DESC
                """,
                (target_date.isoformat(),)
            )

        articles = []
        for row in cursor.fetchall():
            articles.append(NewsArticle(
                id=row["id"],
                company_id=row["company_id"],
                title=row["title"],
                description=row["description"],
                source=row["source"],
                url=row["url"],
                published_at=datetime.fromisoformat(row["published_at"])
                    if row["published_at"] else None,
                fetched_at=datetime.fromisoformat(row["fetched_at"])
                    if row["fetched_at"] else None
            ))

        conn.close()
        return articles

    def get_financial_snapshot(
        self,
        company_id: int,
        target_date: date
    ) -> Optional[FinancialSnapshot]:
        """Get financial snapshot for a company on a specific date."""
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT * FROM financial_snapshots
            WHERE company_id = ? AND date = ?
            """,
            (company_id, target_date.isoformat())
        )

        row = cursor.fetchone()
        conn.close()

        if not row:
            return None

        return FinancialSnapshot(
            id=row["id"],
            company_id=row["company_id"],
            date=date.fromisoformat(row["date"]),
            price=row["price"],
            change_percent=row["change_percent"],
            volume=row["volume"],
            market_cap=row["market_cap"],
            high_52w=row["high_52w"],
            low_52w=row["low_52w"],
            raw_data=json.loads(row["raw_data"]) if row["raw_data"] else None
        )

    def get_company_by_name(self, name: str) -> Optional[dict]:
        """Get company info by name."""
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute(
            "SELECT * FROM companies WHERE name = ?",
            (name,)
        )
        row = cursor.fetchone()
        conn.close()

        if row:
            return dict(row)
        return None

    def save_daily_summary(self, summary: DailySummary) -> int:
        """Save a daily summary."""
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            INSERT OR REPLACE INTO daily_summaries (date, summary_text, email_sent)
            VALUES (?, ?, ?)
            """,
            (summary.date.isoformat(), summary.summary_text, summary.email_sent)
        )

        summary_id = cursor.lastrowid
        conn.commit()
        conn.close()
        return summary_id

    def mark_summary_email_sent(self, target_date: date):
        """Mark a summary as having been emailed."""
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute(
            "UPDATE daily_summaries SET email_sent = TRUE WHERE date = ?",
            (target_date.isoformat(),)
        )

        conn.commit()
        conn.close()

    def get_daily_summary(self, target_date: date) -> Optional[DailySummary]:
        """Get the daily summary for a specific date."""
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute(
            "SELECT * FROM daily_summaries WHERE date = ?",
            (target_date.isoformat(),)
        )

        row = cursor.fetchone()
        conn.close()

        if not row:
            return None

        return DailySummary(
            id=row["id"],
            date=date.fromisoformat(row["date"]),
            summary_text=row["summary_text"],
            email_sent=bool(row["email_sent"]),
            created_at=datetime.fromisoformat(row["created_at"])
                if row["created_at"] else None
        )

    def save_sec_filing(self, filing) -> bool:
        """Save an SEC filing if it doesn't already exist.

        Args:
            filing: SECFiling object from sec_fetcher.

        Returns:
            True if filing was saved, False if it was a duplicate.
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute(
                """
                INSERT INTO sec_filings
                    (company_id, form_type, filed_at, accession_number,
                     filing_url, description, content_summary)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    filing.company_id,
                    filing.form_type,
                    filing.filed_at.isoformat() if filing.filed_at else None,
                    filing.accession_number,
                    filing.filing_url,
                    filing.description,
                    filing.content_summary
                )
            )
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            # Duplicate accession number
            return False
        finally:
            conn.close()

    def get_sec_filings_by_date(
        self,
        target_date: date,
        company_id: Optional[int] = None
    ) -> list[SECFilingRecord]:
        """Get SEC filings fetched on a specific date."""
        conn = self._get_connection()
        cursor = conn.cursor()

        if company_id:
            cursor.execute(
                """
                SELECT * FROM sec_filings
                WHERE date(fetched_at) = ? AND company_id = ?
                ORDER BY filed_at DESC
                """,
                (target_date.isoformat(), company_id)
            )
        else:
            cursor.execute(
                """
                SELECT * FROM sec_filings
                WHERE date(fetched_at) = ?
                ORDER BY filed_at DESC
                """,
                (target_date.isoformat(),)
            )

        filings = []
        for row in cursor.fetchall():
            filings.append(SECFilingRecord(
                id=row["id"],
                company_id=row["company_id"],
                form_type=row["form_type"],
                filed_at=datetime.fromisoformat(row["filed_at"])
                    if row["filed_at"] else None,
                accession_number=row["accession_number"],
                filing_url=row["filing_url"],
                description=row["description"],
                content_summary=row["content_summary"],
                fetched_at=datetime.fromisoformat(row["fetched_at"])
                    if row["fetched_at"] else None
            ))

        conn.close()
        return filings
