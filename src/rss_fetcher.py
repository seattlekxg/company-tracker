"""RSS feed fetcher for company press releases and news."""

import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

import feedparser
import requests

from .config import Company, config
from .storage import NewsArticle


@dataclass
class RSSFeed:
    """Represents an RSS feed configuration."""
    company_name: str
    feed_url: str
    language: str = "en"  # ISO language code
    feed_name: str = ""   # e.g., "Press Releases", "Newsroom"


# Configured RSS feeds for tracked companies
# These are direct company feeds and wire service feeds
COMPANY_RSS_FEEDS = [
    # === GlobeNewswire Company Feeds (verified working) ===
    RSSFeed(
        company_name="ABB",
        feed_url="https://www.globenewswire.com/RssFeed/subjectcode/25-Data%20Center",
        language="en",
        feed_name="GlobeNewswire Data Center"
    ),
    RSSFeed(
        company_name="Schneider Electric",
        feed_url="https://www.globenewswire.com/RssFeed/subjectcode/25-Data%20Center",
        language="en",
        feed_name="GlobeNewswire Data Center"
    ),

    # === PR Newswire Feeds ===
    RSSFeed(
        company_name="Eaton",
        feed_url="https://www.prnewswire.com/rss/news-releases-list.rss",
        language="en",
        feed_name="PR Newswire"
    ),
    RSSFeed(
        company_name="Vertiv",
        feed_url="https://www.prnewswire.com/rss/news-releases-list.rss",
        language="en",
        feed_name="PR Newswire"
    ),
    RSSFeed(
        company_name="Cummins",
        feed_url="https://www.prnewswire.com/rss/news-releases-list.rss",
        language="en",
        feed_name="PR Newswire"
    ),
    RSSFeed(
        company_name="Johnson Controls",
        feed_url="https://www.prnewswire.com/rss/news-releases-list.rss",
        language="en",
        feed_name="PR Newswire"
    ),
    RSSFeed(
        company_name="Carrier",
        feed_url="https://www.prnewswire.com/rss/news-releases-list.rss",
        language="en",
        feed_name="PR Newswire"
    ),
    RSSFeed(
        company_name="Trane",
        feed_url="https://www.prnewswire.com/rss/news-releases-list.rss",
        language="en",
        feed_name="PR Newswire"
    ),
    RSSFeed(
        company_name="Caterpillar",
        feed_url="https://www.prnewswire.com/rss/news-releases-list.rss",
        language="en",
        feed_name="PR Newswire"
    ),
    RSSFeed(
        company_name="Corning",
        feed_url="https://www.prnewswire.com/rss/news-releases-list.rss",
        language="en",
        feed_name="PR Newswire"
    ),

    # === Data Center Industry Feeds (verified working) ===
    RSSFeed(
        company_name="_industry",
        feed_url="https://www.datacenterdynamics.com/en/rss/",
        language="en",
        feed_name="DatacenterDynamics"
    ),
    RSSFeed(
        company_name="_industry",
        feed_url="https://www.globenewswire.com/RssFeed/subjectcode/25-Data%20Center",
        language="en",
        feed_name="GlobeNewswire Data Center"
    ),
]


class RSSFetcher:
    """Fetches news from RSS feeds."""

    # User agent for HTTP requests
    USER_AGENT = "CompanyTracker/1.0 (RSS Feed Reader)"

    def __init__(self, translator=None):
        """Initialize the RSS fetcher.

        Args:
            translator: Optional Summarizer instance for translating non-English content.
        """
        self.translator = translator
        self._feed_cache = {}

    def _fetch_feed(self, feed: RSSFeed) -> list[dict]:
        """Fetch and parse an RSS feed.

        Args:
            feed: RSS feed configuration.

        Returns:
            List of parsed feed entries.
        """
        try:
            # Set custom user agent
            feedparser.USER_AGENT = self.USER_AGENT

            # Parse the feed
            parsed = feedparser.parse(feed.feed_url)

            if parsed.bozo and not parsed.entries:
                # Feed parsing error with no entries
                print(f"    Warning: Could not parse feed {feed.feed_name}: {parsed.bozo_exception}")
                return []

            return parsed.entries

        except Exception as e:
            print(f"    Error fetching feed {feed.feed_name}: {e}")
            return []

    def _parse_date(self, entry: dict) -> Optional[datetime]:
        """Parse the publication date from a feed entry.

        Args:
            entry: Feed entry dictionary.

        Returns:
            Parsed datetime or None.
        """
        # Try different date fields
        for date_field in ['published_parsed', 'updated_parsed', 'created_parsed']:
            if date_field in entry and entry[date_field]:
                try:
                    return datetime(*entry[date_field][:6], tzinfo=ZoneInfo("UTC"))
                except (TypeError, ValueError):
                    continue

        # Try string parsing
        for date_field in ['published', 'updated', 'created']:
            if date_field in entry and entry[date_field]:
                try:
                    # feedparser sometimes provides struct_time
                    from email.utils import parsedate_to_datetime
                    return parsedate_to_datetime(entry[date_field])
                except (TypeError, ValueError):
                    continue

        return None

    def _translate_content(self, text: str, source_lang: str) -> str:
        """Translate non-English content using Claude.

        Args:
            text: Text to translate.
            source_lang: Source language code.

        Returns:
            Translated text or original if translation fails.
        """
        if not self.translator or source_lang == "en" or not text:
            return text

        try:
            # Use Claude for translation
            prompt = f"""Translate the following {source_lang} text to English.
Provide only the translation, no explanations.

Text: {text}

Translation:"""

            message = self.translator.client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=500,
                messages=[{"role": "user", "content": prompt}]
            )

            translated = ""
            for block in message.content:
                if block.type == "text":
                    translated += block.text

            return translated.strip() if translated else text

        except Exception as e:
            print(f"    Translation error: {e}")
            return text

    def _article_mentions_company(self, entry: dict, company: Company) -> bool:
        """Check if an RSS entry mentions the company.

        Args:
            entry: Feed entry dictionary.
            company: Company to check for.

        Returns:
            True if the article mentions the company.
        """
        # Build search terms
        search_terms = [company.name.lower()]
        if company.ticker:
            search_terms.append(company.ticker.lower())
        if company.keywords:
            search_terms.extend([kw.lower() for kw in company.keywords[:3]])

        # Check title and description
        title = entry.get('title', '').lower()
        description = entry.get('summary', entry.get('description', '')).lower()
        content = f"{title} {description}"

        return any(term in content for term in search_terms)

    def fetch_company_news(
        self,
        company: Company,
        company_id: int,
        hours_back: int = 72
    ) -> list[NewsArticle]:
        """Fetch news from RSS feeds for a company.

        Args:
            company: Company to fetch news for.
            company_id: Database ID of the company.
            hours_back: How many hours back to include articles.

        Returns:
            List of NewsArticle objects.
        """
        articles = []
        cutoff_time = datetime.now(ZoneInfo("UTC")) - timedelta(hours=hours_back)

        # Find feeds for this company
        company_feeds = [f for f in COMPANY_RSS_FEEDS if f.company_name == company.name]

        if not company_feeds:
            return articles

        for feed in company_feeds:
            print(f"    Checking {feed.feed_name}...")
            entries = self._fetch_feed(feed)

            for entry in entries:
                # Filter: only include articles that mention this company
                if not self._article_mentions_company(entry, company):
                    continue

                # Parse publication date
                pub_date = self._parse_date(entry)

                # Skip old articles
                if pub_date and pub_date < cutoff_time:
                    continue

                # Get article URL
                url = entry.get('link', '')
                if not url:
                    continue

                # Get title and description
                title = entry.get('title', 'No title')
                description = entry.get('summary', entry.get('description', ''))

                # Clean HTML from description
                if description:
                    import re
                    description = re.sub(r'<[^>]+>', '', description)
                    description = description[:500]  # Truncate

                # Translate if needed
                if feed.language != "en":
                    title = self._translate_content(title, feed.language)
                    if description:
                        description = self._translate_content(description, feed.language)

                articles.append(NewsArticle(
                    id=None,
                    company_id=company_id,
                    title=title,
                    description=description,
                    source=feed.feed_name,
                    url=url,
                    published_at=pub_date
                ))

        return articles

    def fetch_industry_news(self, hours_back: int = 72) -> list[dict]:
        """Fetch general data center industry news.

        Args:
            hours_back: How many hours back to include articles.

        Returns:
            List of article dictionaries.
        """
        articles = []
        cutoff_time = datetime.now(ZoneInfo("UTC")) - timedelta(hours=hours_back)

        # Find industry feeds
        industry_feeds = [f for f in COMPANY_RSS_FEEDS if f.company_name == "_industry"]

        for feed in industry_feeds:
            print(f"  Fetching {feed.feed_name}...")
            entries = self._fetch_feed(feed)

            for entry in entries[:20]:  # Limit industry news
                pub_date = self._parse_date(entry)

                if pub_date and pub_date < cutoff_time:
                    continue

                url = entry.get('link', '')
                if not url:
                    continue

                title = entry.get('title', 'No title')
                description = entry.get('summary', '')

                if description:
                    import re
                    description = re.sub(r'<[^>]+>', '', description)
                    description = description[:500]

                articles.append({
                    'title': title,
                    'description': description,
                    'url': url,
                    'source': feed.feed_name,
                    'published_at': pub_date
                })

        return articles

    def fetch_all_companies(
        self,
        companies: list[Company],
        company_ids: dict[str, int],
        hours_back: int = 72,
        rate_limit_delay: float = 0.5
    ) -> dict[str, list[NewsArticle]]:
        """Fetch RSS news for all companies.

        Args:
            companies: List of companies.
            company_ids: Dict mapping company name to database ID.
            hours_back: How many hours back to search.
            rate_limit_delay: Delay between feed fetches.

        Returns:
            Dict mapping company name to list of articles.
        """
        all_articles = {}

        for company in companies:
            company_id = company_ids.get(company.name)
            if not company_id:
                continue

            # Check if we have feeds for this company
            has_feeds = any(f.company_name == company.name for f in COMPANY_RSS_FEEDS)
            if not has_feeds:
                all_articles[company.name] = []
                continue

            print(f"  Fetching RSS feeds for {company.name}...")
            articles = self.fetch_company_news(company, company_id, hours_back)
            all_articles[company.name] = articles

            if articles:
                print(f"    Found {len(articles)} articles from RSS feeds")

            if rate_limit_delay > 0:
                time.sleep(rate_limit_delay)

        return all_articles
