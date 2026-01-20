"""NewsAPI integration for fetching hyperscaler data center announcements."""

import time
from datetime import datetime, timedelta
from typing import Optional

import requests

from .config import config
from .storage import HyperscalerAnnouncement


# Hyperscalers to track for data center expansion news
HYPERSCALERS = [
    {
        "name": "AWS",
        "keywords": ["AWS data center", "Amazon Web Services data center"]
    },
    {
        "name": "Google Cloud",
        "keywords": ["Google data center", "Google Cloud data center"]
    },
    {
        "name": "Microsoft Azure",
        "keywords": ["Microsoft data center", "Azure data center"]
    },
    {
        "name": "Meta",
        "keywords": ["Meta data center", "Facebook data center"]
    },
    {
        "name": "Oracle Cloud",
        "keywords": ["Oracle data center", "Oracle Cloud Infrastructure"]
    },
]

# Additional keywords to filter for expansion/construction news
EXPANSION_KEYWORDS = [
    "expansion", "construction", "build", "building", "new",
    "investment", "billion", "million", "MW", "megawatt",
    "announce", "plan", "develop", "campus"
]


class HyperscalerFetcher:
    """Fetches hyperscaler data center announcements from NewsAPI."""

    BASE_URL = "https://newsapi.org/v2/everything"

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or config.newsapi_key
        if not self.api_key:
            raise ValueError("NewsAPI key is required")

    def _is_expansion_related(self, title: str, description: str) -> bool:
        """Check if article is related to data center expansion/construction."""
        text = f"{title} {description or ''}".lower()
        return any(keyword.lower() in text for keyword in EXPANSION_KEYWORDS)

    def fetch_announcements(
        self,
        hours_back: int = 72
    ) -> list[HyperscalerAnnouncement]:
        """Fetch hyperscaler data center expansion announcements.

        Args:
            hours_back: How many hours back to search (default 72).

        Returns:
            List of HyperscalerAnnouncement objects.
        """
        announcements = []
        from_date = datetime.utcnow() - timedelta(hours=hours_back)
        seen_urls = set()

        for hyperscaler in HYPERSCALERS:
            # Build search query from keywords
            query = " OR ".join(f'"{kw}"' for kw in hyperscaler["keywords"])

            params = {
                "q": query,
                "from": from_date.strftime("%Y-%m-%dT%H:%M:%S"),
                "sortBy": "publishedAt",
                "language": "en",
                "pageSize": 20,  # More results to filter through
                "apiKey": self.api_key
            }

            try:
                response = requests.get(self.BASE_URL, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()

                if data.get("status") != "ok":
                    print(f"  NewsAPI error for {hyperscaler['name']}: "
                          f"{data.get('message', 'Unknown error')}")
                    continue

                for article_data in data.get("articles", []):
                    url = article_data.get("url")
                    if not url or url in seen_urls:
                        continue

                    title = article_data.get("title", "")
                    description = article_data.get("description", "")

                    # Filter for expansion-related articles
                    if not self._is_expansion_related(title, description):
                        continue

                    seen_urls.add(url)

                    published_at = None
                    if article_data.get("publishedAt"):
                        try:
                            published_at = datetime.fromisoformat(
                                article_data["publishedAt"].replace("Z", "+00:00")
                            )
                        except ValueError:
                            pass

                    announcements.append(HyperscalerAnnouncement(
                        id=None,
                        hyperscaler=hyperscaler["name"],
                        title=title or "No title",
                        description=description,
                        url=url,
                        published_at=published_at,
                        content_summary=None  # Will be filled by summarizer
                    ))

                print(f"  {hyperscaler['name']}: Found "
                      f"{sum(1 for a in announcements if a.hyperscaler == hyperscaler['name'])} "
                      f"expansion-related articles")

            except requests.exceptions.RequestException as e:
                print(f"  Error fetching news for {hyperscaler['name']}: {e}")

            # Brief delay between hyperscaler searches
            time.sleep(0.5)

        return announcements
