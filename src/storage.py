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


@dataclass
class EarningsTranscript:
    """Represents an earnings call transcript."""
    id: Optional[int]
    company_id: int
    ticker: str
    quarter: str  # e.g., "Q4 2025"
    transcript_date: date
    transcript_text: Optional[str]
    content_summary: Optional[str]  # AI analysis
    fetched_at: Optional[datetime] = None


@dataclass
class HyperscalerAnnouncement:
    """Represents a hyperscaler data center announcement."""
    id: Optional[int]
    hyperscaler: str  # "AWS", "Google Cloud", etc.
    title: str
    description: Optional[str]
    url: str
    published_at: Optional[datetime]
    content_summary: Optional[str]  # AI analysis
    fetched_at: Optional[datetime] = None


@dataclass
class PEDatacenterAnnouncement:
    """Represents a Private Equity data center investment announcement."""
    id: Optional[int]
    pe_firm: str  # "Blackstone", "KKR", etc.
    title: str
    description: Optional[str]
    url: str
    published_at: Optional[datetime]
    content_summary: Optional[str]  # AI analysis
    fetched_at: Optional[datetime] = None


class Storage:
    """SQLite storage handler."""

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or config.db_path
        # Ensure the data directory exists
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self._init_db()
        self._migrate_db()

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

            CREATE TABLE IF NOT EXISTS earnings_transcripts (
                id INTEGER PRIMARY KEY,
                company_id INTEGER REFERENCES companies(id),
                ticker TEXT NOT NULL,
                quarter TEXT NOT NULL,
                transcript_date DATE,
                transcript_text TEXT,
                content_summary TEXT,
                fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(company_id, quarter)
            );

            CREATE TABLE IF NOT EXISTS hyperscaler_announcements (
                id INTEGER PRIMARY KEY,
                hyperscaler TEXT NOT NULL,
                title TEXT NOT NULL,
                description TEXT,
                url TEXT UNIQUE,
                published_at TIMESTAMP,
                content_summary TEXT,
                fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS pe_datacenter_announcements (
                id INTEGER PRIMARY KEY,
                pe_firm TEXT NOT NULL,
                title TEXT NOT NULL,
                description TEXT,
                url TEXT UNIQUE,
                published_at TIMESTAMP,
                content_summary TEXT,
                fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE INDEX IF NOT EXISTS idx_news_company ON news_articles(company_id);
            CREATE INDEX IF NOT EXISTS idx_news_published ON news_articles(published_at);
            CREATE INDEX IF NOT EXISTS idx_financial_date ON financial_snapshots(date);
            CREATE INDEX IF NOT EXISTS idx_sec_company ON sec_filings(company_id);
            CREATE INDEX IF NOT EXISTS idx_sec_filed ON sec_filings(filed_at);
            CREATE INDEX IF NOT EXISTS idx_transcripts_company ON earnings_transcripts(company_id);
            CREATE INDEX IF NOT EXISTS idx_transcripts_date ON earnings_transcripts(transcript_date);
            CREATE INDEX IF NOT EXISTS idx_hyperscaler_published ON hyperscaler_announcements(published_at);
            CREATE INDEX IF NOT EXISTS idx_pe_datacenter_published ON pe_datacenter_announcements(published_at);
        """)

        conn.commit()
        conn.close()

    def _migrate_db(self):
        """Run database migrations for existing databases."""
        conn = self._get_connection()
        cursor = conn.cursor()

        # Check if emailed_at column exists in news_articles
        cursor.execute("PRAGMA table_info(news_articles)")
        columns = [row["name"] for row in cursor.fetchall()]

        if "emailed_at" not in columns:
            print("  Migrating database: adding emailed_at columns...")
            # Add emailed_at column to all content tables
            # Mark all existing items as emailed to avoid re-sending old content
            cursor.executescript("""
                ALTER TABLE news_articles ADD COLUMN emailed_at TIMESTAMP DEFAULT NULL;
                ALTER TABLE sec_filings ADD COLUMN emailed_at TIMESTAMP DEFAULT NULL;
                ALTER TABLE earnings_transcripts ADD COLUMN emailed_at TIMESTAMP DEFAULT NULL;
                ALTER TABLE hyperscaler_announcements ADD COLUMN emailed_at TIMESTAMP DEFAULT NULL;

                UPDATE news_articles SET emailed_at = fetched_at WHERE emailed_at IS NULL;
                UPDATE sec_filings SET emailed_at = fetched_at WHERE emailed_at IS NULL;
                UPDATE earnings_transcripts SET emailed_at = fetched_at WHERE emailed_at IS NULL;
                UPDATE hyperscaler_announcements SET emailed_at = fetched_at WHERE emailed_at IS NULL;
            """)
            conn.commit()
            print("  Migration complete: existing items marked as emailed")

        # Check if emailed_at column exists in pe_datacenter_announcements (new table)
        cursor.execute("PRAGMA table_info(pe_datacenter_announcements)")
        pe_columns = [row["name"] for row in cursor.fetchall()]

        if pe_columns and "emailed_at" not in pe_columns:
            print("  Migrating database: adding emailed_at to pe_datacenter_announcements...")
            cursor.executescript("""
                ALTER TABLE pe_datacenter_announcements ADD COLUMN emailed_at TIMESTAMP DEFAULT NULL;
                UPDATE pe_datacenter_announcements SET emailed_at = fetched_at WHERE emailed_at IS NULL;
            """)
            conn.commit()
            print("  PE datacenter migration complete")

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

    def save_earnings_transcript(self, transcript: EarningsTranscript) -> bool:
        """Save an earnings transcript if it doesn't already exist.

        Returns:
            True if transcript was saved, False if it was a duplicate.
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute(
                """
                INSERT INTO earnings_transcripts
                    (company_id, ticker, quarter, transcript_date,
                     transcript_text, content_summary)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    transcript.company_id,
                    transcript.ticker,
                    transcript.quarter,
                    transcript.transcript_date.isoformat()
                        if transcript.transcript_date else None,
                    transcript.transcript_text,
                    transcript.content_summary
                )
            )
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            # Duplicate company_id + quarter
            return False
        finally:
            conn.close()

    def get_transcripts_by_date(
        self,
        target_date: date,
        company_id: Optional[int] = None
    ) -> list[EarningsTranscript]:
        """Get earnings transcripts fetched on a specific date."""
        conn = self._get_connection()
        cursor = conn.cursor()

        if company_id:
            cursor.execute(
                """
                SELECT * FROM earnings_transcripts
                WHERE date(fetched_at) = ? AND company_id = ?
                ORDER BY transcript_date DESC
                """,
                (target_date.isoformat(), company_id)
            )
        else:
            cursor.execute(
                """
                SELECT * FROM earnings_transcripts
                WHERE date(fetched_at) = ?
                ORDER BY transcript_date DESC
                """,
                (target_date.isoformat(),)
            )

        transcripts = []
        for row in cursor.fetchall():
            transcripts.append(EarningsTranscript(
                id=row["id"],
                company_id=row["company_id"],
                ticker=row["ticker"],
                quarter=row["quarter"],
                transcript_date=date.fromisoformat(row["transcript_date"])
                    if row["transcript_date"] else None,
                transcript_text=row["transcript_text"],
                content_summary=row["content_summary"],
                fetched_at=datetime.fromisoformat(row["fetched_at"])
                    if row["fetched_at"] else None
            ))

        conn.close()
        return transcripts

    def save_hyperscaler_announcement(
        self,
        announcement: HyperscalerAnnouncement
    ) -> bool:
        """Save a hyperscaler announcement if it doesn't already exist.

        Returns:
            True if announcement was saved, False if it was a duplicate.
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute(
                """
                INSERT INTO hyperscaler_announcements
                    (hyperscaler, title, description, url,
                     published_at, content_summary)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    announcement.hyperscaler,
                    announcement.title,
                    announcement.description,
                    announcement.url,
                    announcement.published_at.isoformat()
                        if announcement.published_at else None,
                    announcement.content_summary
                )
            )
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            # Duplicate URL
            return False
        finally:
            conn.close()

    def get_hyperscaler_announcements_by_date(
        self,
        target_date: date
    ) -> list[HyperscalerAnnouncement]:
        """Get hyperscaler announcements fetched on a specific date."""
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT * FROM hyperscaler_announcements
            WHERE date(fetched_at) = ?
            ORDER BY published_at DESC
            """,
            (target_date.isoformat(),)
        )

        announcements = []
        for row in cursor.fetchall():
            announcements.append(HyperscalerAnnouncement(
                id=row["id"],
                hyperscaler=row["hyperscaler"],
                title=row["title"],
                description=row["description"],
                url=row["url"],
                published_at=datetime.fromisoformat(row["published_at"])
                    if row["published_at"] else None,
                content_summary=row["content_summary"],
                fetched_at=datetime.fromisoformat(row["fetched_at"])
                    if row["fetched_at"] else None
            ))

        conn.close()
        return announcements

    def save_pe_announcement(
        self,
        announcement: PEDatacenterAnnouncement
    ) -> bool:
        """Save a PE data center announcement if it doesn't already exist.

        Returns:
            True if announcement was saved, False if it was a duplicate.
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute(
                """
                INSERT INTO pe_datacenter_announcements
                    (pe_firm, title, description, url,
                     published_at, content_summary)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    announcement.pe_firm,
                    announcement.title,
                    announcement.description,
                    announcement.url,
                    announcement.published_at.isoformat()
                        if announcement.published_at else None,
                    announcement.content_summary
                )
            )
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            # Duplicate URL
            return False
        finally:
            conn.close()

    def get_pe_announcements_by_date(
        self,
        target_date: date
    ) -> list[PEDatacenterAnnouncement]:
        """Get PE data center announcements fetched on a specific date."""
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT * FROM pe_datacenter_announcements
            WHERE date(fetched_at) = ?
            ORDER BY published_at DESC
            """,
            (target_date.isoformat(),)
        )

        announcements = []
        for row in cursor.fetchall():
            announcements.append(PEDatacenterAnnouncement(
                id=row["id"],
                pe_firm=row["pe_firm"],
                title=row["title"],
                description=row["description"],
                url=row["url"],
                published_at=datetime.fromisoformat(row["published_at"])
                    if row["published_at"] else None,
                content_summary=row["content_summary"],
                fetched_at=datetime.fromisoformat(row["fetched_at"])
                    if row["fetched_at"] else None
            ))

        conn.close()
        return announcements

    # ========== Unsent Item Query Methods ==========

    def get_unsent_articles(self, company_id: int) -> list[NewsArticle]:
        """Get news articles that haven't been emailed yet for a company."""
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT * FROM news_articles
            WHERE company_id = ? AND emailed_at IS NULL
            ORDER BY published_at DESC
            """,
            (company_id,)
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

    def get_unsent_sec_filings(self, company_id: int) -> list[SECFilingRecord]:
        """Get SEC filings that haven't been emailed yet for a company."""
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT * FROM sec_filings
            WHERE company_id = ? AND emailed_at IS NULL
            ORDER BY filed_at DESC
            """,
            (company_id,)
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

    def get_unsent_transcripts(self, company_id: int) -> list[EarningsTranscript]:
        """Get earnings transcripts that haven't been emailed yet for a company."""
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT * FROM earnings_transcripts
            WHERE company_id = ? AND emailed_at IS NULL
            ORDER BY transcript_date DESC
            """,
            (company_id,)
        )

        transcripts = []
        for row in cursor.fetchall():
            transcripts.append(EarningsTranscript(
                id=row["id"],
                company_id=row["company_id"],
                ticker=row["ticker"],
                quarter=row["quarter"],
                transcript_date=date.fromisoformat(row["transcript_date"])
                    if row["transcript_date"] else None,
                transcript_text=row["transcript_text"],
                content_summary=row["content_summary"],
                fetched_at=datetime.fromisoformat(row["fetched_at"])
                    if row["fetched_at"] else None
            ))

        conn.close()
        return transcripts

    def get_unsent_hyperscaler_announcements(self) -> list[HyperscalerAnnouncement]:
        """Get hyperscaler announcements that haven't been emailed yet."""
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT * FROM hyperscaler_announcements
            WHERE emailed_at IS NULL
            ORDER BY published_at DESC
            """
        )

        announcements = []
        for row in cursor.fetchall():
            announcements.append(HyperscalerAnnouncement(
                id=row["id"],
                hyperscaler=row["hyperscaler"],
                title=row["title"],
                description=row["description"],
                url=row["url"],
                published_at=datetime.fromisoformat(row["published_at"])
                    if row["published_at"] else None,
                content_summary=row["content_summary"],
                fetched_at=datetime.fromisoformat(row["fetched_at"])
                    if row["fetched_at"] else None
            ))

        conn.close()
        return announcements

    def get_unsent_pe_announcements(self) -> list[PEDatacenterAnnouncement]:
        """Get PE data center announcements that haven't been emailed yet."""
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT * FROM pe_datacenter_announcements
            WHERE emailed_at IS NULL
            ORDER BY published_at DESC
            """
        )

        announcements = []
        for row in cursor.fetchall():
            announcements.append(PEDatacenterAnnouncement(
                id=row["id"],
                pe_firm=row["pe_firm"],
                title=row["title"],
                description=row["description"],
                url=row["url"],
                published_at=datetime.fromisoformat(row["published_at"])
                    if row["published_at"] else None,
                content_summary=row["content_summary"],
                fetched_at=datetime.fromisoformat(row["fetched_at"])
                    if row["fetched_at"] else None
            ))

        conn.close()
        return announcements

    # ========== Mark As Emailed Methods ==========

    def mark_articles_emailed(self, article_ids: list[int]):
        """Mark news articles as emailed."""
        if not article_ids:
            return
        conn = self._get_connection()
        cursor = conn.cursor()
        placeholders = ",".join("?" * len(article_ids))
        cursor.execute(
            f"""
            UPDATE news_articles
            SET emailed_at = CURRENT_TIMESTAMP
            WHERE id IN ({placeholders})
            """,
            article_ids
        )
        conn.commit()
        conn.close()

    def mark_sec_filings_emailed(self, filing_ids: list[int]):
        """Mark SEC filings as emailed."""
        if not filing_ids:
            return
        conn = self._get_connection()
        cursor = conn.cursor()
        placeholders = ",".join("?" * len(filing_ids))
        cursor.execute(
            f"""
            UPDATE sec_filings
            SET emailed_at = CURRENT_TIMESTAMP
            WHERE id IN ({placeholders})
            """,
            filing_ids
        )
        conn.commit()
        conn.close()

    def mark_transcripts_emailed(self, transcript_ids: list[int]):
        """Mark earnings transcripts as emailed."""
        if not transcript_ids:
            return
        conn = self._get_connection()
        cursor = conn.cursor()
        placeholders = ",".join("?" * len(transcript_ids))
        cursor.execute(
            f"""
            UPDATE earnings_transcripts
            SET emailed_at = CURRENT_TIMESTAMP
            WHERE id IN ({placeholders})
            """,
            transcript_ids
        )
        conn.commit()
        conn.close()

    def mark_hyperscaler_announcements_emailed(self, announcement_ids: list[int]):
        """Mark hyperscaler announcements as emailed."""
        if not announcement_ids:
            return
        conn = self._get_connection()
        cursor = conn.cursor()
        placeholders = ",".join("?" * len(announcement_ids))
        cursor.execute(
            f"""
            UPDATE hyperscaler_announcements
            SET emailed_at = CURRENT_TIMESTAMP
            WHERE id IN ({placeholders})
            """,
            announcement_ids
        )
        conn.commit()
        conn.close()

    def mark_pe_announcements_emailed(self, announcement_ids: list[int]):
        """Mark PE data center announcements as emailed."""
        if not announcement_ids:
            return
        conn = self._get_connection()
        cursor = conn.cursor()
        placeholders = ",".join("?" * len(announcement_ids))
        cursor.execute(
            f"""
            UPDATE pe_datacenter_announcements
            SET emailed_at = CURRENT_TIMESTAMP
            WHERE id IN ({placeholders})
            """,
            announcement_ids
        )
        conn.commit()
        conn.close()

    # ========== Date Range Query Methods (for Weekly Summary) ==========

    def get_articles_in_range(
        self,
        company_id: int,
        start_date: date,
        end_date: date
    ) -> list[NewsArticle]:
        """Get news articles within a date range for a company."""
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT * FROM news_articles
            WHERE company_id = ?
              AND date(fetched_at) >= ?
              AND date(fetched_at) <= ?
            ORDER BY published_at DESC
            """,
            (company_id, start_date.isoformat(), end_date.isoformat())
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

    def get_sec_filings_in_range(
        self,
        company_id: int,
        start_date: date,
        end_date: date
    ) -> list[SECFilingRecord]:
        """Get SEC filings within a date range for a company."""
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT * FROM sec_filings
            WHERE company_id = ?
              AND date(fetched_at) >= ?
              AND date(fetched_at) <= ?
            ORDER BY filed_at DESC
            """,
            (company_id, start_date.isoformat(), end_date.isoformat())
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

    def get_transcripts_in_range(
        self,
        company_id: int,
        start_date: date,
        end_date: date
    ) -> list[EarningsTranscript]:
        """Get earnings transcripts within a date range for a company."""
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT * FROM earnings_transcripts
            WHERE company_id = ?
              AND date(fetched_at) >= ?
              AND date(fetched_at) <= ?
            ORDER BY transcript_date DESC
            """,
            (company_id, start_date.isoformat(), end_date.isoformat())
        )

        transcripts = []
        for row in cursor.fetchall():
            transcripts.append(EarningsTranscript(
                id=row["id"],
                company_id=row["company_id"],
                ticker=row["ticker"],
                quarter=row["quarter"],
                transcript_date=date.fromisoformat(row["transcript_date"])
                    if row["transcript_date"] else None,
                transcript_text=row["transcript_text"],
                content_summary=row["content_summary"],
                fetched_at=datetime.fromisoformat(row["fetched_at"])
                    if row["fetched_at"] else None
            ))

        conn.close()
        return transcripts

    def get_hyperscaler_announcements_in_range(
        self,
        start_date: date,
        end_date: date
    ) -> list[HyperscalerAnnouncement]:
        """Get hyperscaler announcements within a date range."""
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT * FROM hyperscaler_announcements
            WHERE date(fetched_at) >= ?
              AND date(fetched_at) <= ?
            ORDER BY published_at DESC
            """,
            (start_date.isoformat(), end_date.isoformat())
        )

        announcements = []
        for row in cursor.fetchall():
            announcements.append(HyperscalerAnnouncement(
                id=row["id"],
                hyperscaler=row["hyperscaler"],
                title=row["title"],
                description=row["description"],
                url=row["url"],
                published_at=datetime.fromisoformat(row["published_at"])
                    if row["published_at"] else None,
                content_summary=row["content_summary"],
                fetched_at=datetime.fromisoformat(row["fetched_at"])
                    if row["fetched_at"] else None
            ))

        conn.close()
        return announcements

    def get_pe_announcements_in_range(
        self,
        start_date: date,
        end_date: date
    ) -> list[PEDatacenterAnnouncement]:
        """Get PE data center announcements within a date range."""
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT * FROM pe_datacenter_announcements
            WHERE date(fetched_at) >= ?
              AND date(fetched_at) <= ?
            ORDER BY published_at DESC
            """,
            (start_date.isoformat(), end_date.isoformat())
        )

        announcements = []
        for row in cursor.fetchall():
            announcements.append(PEDatacenterAnnouncement(
                id=row["id"],
                pe_firm=row["pe_firm"],
                title=row["title"],
                description=row["description"],
                url=row["url"],
                published_at=datetime.fromisoformat(row["published_at"])
                    if row["published_at"] else None,
                content_summary=row["content_summary"],
                fetched_at=datetime.fromisoformat(row["fetched_at"])
                    if row["fetched_at"] else None
            ))

        conn.close()
        return announcements
