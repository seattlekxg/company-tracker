"""NewsAPI integration for fetching company news."""

import time
from datetime import datetime, timedelta
from typing import Optional

import requests

from .config import Company, config
from .storage import NewsArticle


class NewsFetcher:
    """Fetches news articles from NewsAPI."""

    BASE_URL = "https://newsapi.org/v2/everything"

    # Data center and infrastructure context terms to ensure relevance
    DATA_CENTER_CONTEXT = [
        "data center", "datacenter", "data centre",
        "hyperscaler", "cloud infrastructure",
        "power distribution", "UPS", "uninterruptible power",
        "cooling system", "HVAC", "precision cooling",
        "electrical infrastructure", "power management",
        "generator", "backup power",
        "AI infrastructure", "GPU cluster",
        "colocation", "colo facility",
        "megawatt", "MW capacity"
    ]

    # Business/financial context as secondary filter
    BUSINESS_CONTEXT = [
        "contract", "deal", "partnership", "expansion",
        "quarterly", "earnings", "revenue",
        "acquisition", "order", "backlog"
    ]

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or config.newsapi_key
        if not self.api_key:
            raise ValueError("NewsAPI key is required")

    def _build_query(self, company: Company) -> str:
        """Build a search query that filters for data center-relevant articles.

        Args:
            company: Company to build query for.

        Returns:
            NewsAPI query string.
        """
        # Use company name + ticker for better precision
        search_terms = [company.name]
        if company.ticker:
            search_terms.append(company.ticker)

        # Add the most specific keyword if available (usually contains "data center")
        if company.keywords:
            for kw in company.keywords:
                if "data center" in kw.lower():
                    search_terms.append(kw)
                    break

        company_part = " OR ".join(f'"{term}"' for term in search_terms[:3])

        # Require data center / infrastructure context
        # Use a subset to keep query length manageable for NewsAPI
        dc_context = [
            "data center", "datacenter", "power distribution",
            "UPS", "cooling", "generator", "infrastructure",
            "hyperscaler", "cloud", "AI infrastructure"
        ]
        context_part = " OR ".join(f'"{term}"' for term in dc_context[:6])

        # Combine: (company terms) AND (data center context)
        query = f"({company_part}) AND ({context_part})"

        return query

    def fetch_company_news(
        self,
        company: Company,
        company_id: int,
        hours_back: int = 24
    ) -> list[NewsArticle]:
        """Fetch news articles for a company from the last N hours.

        Args:
            company: Company to search for.
            company_id: Database ID of the company.
            hours_back: How many hours back to search (default 24).

        Returns:
            List of NewsArticle objects.
        """
        articles = []
        from_date = datetime.utcnow() - timedelta(hours=hours_back)

        # Build search query with business context filtering
        query = self._build_query(company)

        params = {
            "q": query,
            "from": from_date.strftime("%Y-%m-%dT%H:%M:%S"),
            "sortBy": "publishedAt",
            "language": "en",
            "pageSize": 10,  # Limit results per company
            "apiKey": self.api_key
        }

        # Debug: print query for troubleshooting
        print(f"    Query: {query[:50]}...")

        try:
            response = requests.get(self.BASE_URL, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            if data.get("status") != "ok":
                print(f"    NewsAPI error: {data.get('code')} - {data.get('message', 'Unknown error')}")
                return articles

            total_results = data.get("totalResults", 0)
            if total_results == 0:
                print(f"    No results from API")

            for article_data in data.get("articles", []):
                # Skip articles without URL
                url = article_data.get("url")
                if not url:
                    continue

                published_at = None
                if article_data.get("publishedAt"):
                    try:
                        published_at = datetime.fromisoformat(
                            article_data["publishedAt"].replace("Z", "+00:00")
                        )
                    except ValueError:
                        pass

                articles.append(NewsArticle(
                    id=None,
                    company_id=company_id,
                    title=article_data.get("title", "No title"),
                    description=article_data.get("description"),
                    source=article_data.get("source", {}).get("name"),
                    url=url,
                    published_at=published_at
                ))

        except requests.exceptions.RequestException as e:
            print(f"Error fetching news for {company.name}: {e}")

        return articles

    def fetch_all_companies(
        self,
        companies: list[Company],
        company_ids: dict[str, int],
        hours_back: int = 24,
        rate_limit_delay: float = 1.0
    ) -> dict[str, list[NewsArticle]]:
        """Fetch news for all companies.

        Args:
            companies: List of companies to fetch news for.
            company_ids: Dict mapping company name to database ID.
            hours_back: How many hours back to search.
            rate_limit_delay: Delay between API calls in seconds.

        Returns:
            Dict mapping company name to list of articles.
        """
        all_articles = {}

        for company in companies:
            company_id = company_ids.get(company.name)
            if not company_id:
                print(f"No database ID for company: {company.name}")
                continue

            print(f"Fetching news for {company.name}...")
            articles = self.fetch_company_news(company, company_id, hours_back)
            all_articles[company.name] = articles
            print(f"  Found {len(articles)} articles")

            # Rate limit to avoid hitting API limits
            if rate_limit_delay > 0:
                time.sleep(rate_limit_delay)

        return all_articles
