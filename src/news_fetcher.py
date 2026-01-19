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

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or config.newsapi_key
        if not self.api_key:
            raise ValueError("NewsAPI key is required")

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

        # Build search query from company name and keywords
        search_terms = company.get_search_terms()
        # Use OR to combine search terms
        query = " OR ".join(f'"{term}"' for term in search_terms[:3])  # Limit to 3 terms

        params = {
            "q": query,
            "from": from_date.strftime("%Y-%m-%dT%H:%M:%S"),
            "sortBy": "publishedAt",
            "language": "en",
            "pageSize": 10,  # Limit results per company
            "apiKey": self.api_key
        }

        try:
            response = requests.get(self.BASE_URL, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            if data.get("status") != "ok":
                print(f"NewsAPI error for {company.name}: {data.get('message', 'Unknown error')}")
                return articles

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
