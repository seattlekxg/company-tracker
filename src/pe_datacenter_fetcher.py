"""NewsAPI integration for fetching Private Equity data center investment news."""

import time
from datetime import datetime, timedelta
from typing import Optional

import requests

from .config import config


# Private Equity firms with significant data center businesses
PE_DATACENTER_FIRMS = [
    {
        "name": "Blackstone",
        "keywords": ["Blackstone data center", "Blackstone QTS"],
        "portfolio": "QTS Data Centers"
    },
    {
        "name": "KKR",
        "keywords": ["KKR data center", "KKR CyrusOne"],
        "portfolio": "CyrusOne"
    },
    {
        "name": "DigitalBridge",
        "keywords": ["DigitalBridge data center", "Digital Bridge data center",
                     "DigitalBridge Vantage", "DataBank data center"],
        "portfolio": "Vantage Data Centers, DataBank"
    },
    {
        "name": "Brookfield",
        "keywords": ["Brookfield data center", "Brookfield Infrastructure data center"],
        "portfolio": "Data4, Cyxtera"
    },
    {
        "name": "Stonepeak",
        "keywords": ["Stonepeak data center", "Stonepeak infrastructure"],
        "portfolio": "Cologix, Lumen data centers"
    },
    {
        "name": "GIP",
        "keywords": ["Global Infrastructure Partners data center", "GIP data center"],
        "portfolio": "CyrusOne (former)"
    },
    {
        "name": "TPG",
        "keywords": ["TPG data center", "TPG Rise data center"],
        "portfolio": "Various"
    },
    {
        "name": "EQT",
        "keywords": ["EQT data center", "EQT Partners data center"],
        "portfolio": "EdgeConneX"
    },
    {
        "name": "Silver Lake",
        "keywords": ["Silver Lake data center"],
        "portfolio": "Various tech infrastructure"
    },
    {
        "name": "Apollo",
        "keywords": ["Apollo Global data center", "Apollo data center"],
        "portfolio": "Various"
    },
    {
        "name": "Carlyle",
        "keywords": ["Carlyle data center", "Carlyle Group data center"],
        "portfolio": "Various"
    },
]

# Keywords that indicate data center investment activity
INVESTMENT_KEYWORDS = [
    "acquisition", "acquire", "investment", "invest",
    "billion", "million", "deal", "purchase", "buy",
    "expansion", "develop", "build", "construction",
    "partnership", "joint venture", "stake",
    "data center", "datacenter", "colocation", "colo",
    "hyperscale", "campus", "facility"
]


class PEDatacenterAnnouncement:
    """Represents a PE firm data center announcement."""

    def __init__(
        self,
        id: Optional[int],
        pe_firm: str,
        title: str,
        description: Optional[str],
        url: str,
        published_at: Optional[datetime],
        content_summary: Optional[str] = None
    ):
        self.id = id
        self.pe_firm = pe_firm
        self.title = title
        self.description = description
        self.url = url
        self.published_at = published_at
        self.content_summary = content_summary


class PEDatacenterFetcher:
    """Fetches Private Equity data center investment news from NewsAPI."""

    BASE_URL = "https://newsapi.org/v2/everything"

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or config.newsapi_key
        if not self.api_key:
            raise ValueError("NewsAPI key is required")

    def _is_datacenter_investment(self, title: str, description: str) -> bool:
        """Check if article is related to data center investment activity."""
        text = f"{title} {description or ''}".lower()

        # Must mention data center AND investment activity
        has_datacenter = any(term in text for term in
                            ["data center", "datacenter", "data centre",
                             "colocation", "colo facility", "hyperscale"])
        has_investment = any(term in text for term in
                            ["acquisition", "acquire", "investment", "invest",
                             "billion", "million", "deal", "purchase", "buy",
                             "expansion", "develop", "build", "construction",
                             "partnership", "joint venture", "stake"])

        return has_datacenter or has_investment

    def fetch_announcements(
        self,
        hours_back: int = 72
    ) -> list[PEDatacenterAnnouncement]:
        """Fetch PE firm data center investment announcements.

        Args:
            hours_back: How many hours back to search (default 72).

        Returns:
            List of PEDatacenterAnnouncement objects.
        """
        announcements = []
        from_date = datetime.utcnow() - timedelta(hours=hours_back)
        seen_urls = set()

        for pe_firm in PE_DATACENTER_FIRMS:
            # Build search query - require data center context
            keyword_part = " OR ".join(f'"{kw}"' for kw in pe_firm["keywords"])
            query = f"({keyword_part})"

            params = {
                "q": query,
                "from": from_date.strftime("%Y-%m-%dT%H:%M:%S"),
                "sortBy": "publishedAt",
                "language": "en",
                "pageSize": 15,
                "apiKey": self.api_key
            }

            try:
                response = requests.get(self.BASE_URL, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()

                if data.get("status") != "ok":
                    print(f"    NewsAPI error for {pe_firm['name']}: "
                          f"{data.get('message', 'Unknown error')}")
                    continue

                firm_count = 0
                for article_data in data.get("articles", []):
                    url = article_data.get("url")
                    if not url or url in seen_urls:
                        continue

                    title = article_data.get("title", "")
                    description = article_data.get("description", "")

                    # Filter for data center investment articles
                    if not self._is_datacenter_investment(title, description):
                        continue

                    seen_urls.add(url)
                    firm_count += 1

                    published_at = None
                    if article_data.get("publishedAt"):
                        try:
                            published_at = datetime.fromisoformat(
                                article_data["publishedAt"].replace("Z", "+00:00")
                            )
                        except ValueError:
                            pass

                    announcements.append(PEDatacenterAnnouncement(
                        id=None,
                        pe_firm=pe_firm["name"],
                        title=title or "No title",
                        description=description,
                        url=url,
                        published_at=published_at,
                        content_summary=None
                    ))

                if firm_count > 0:
                    print(f"    {pe_firm['name']}: Found {firm_count} data center articles")

            except requests.exceptions.RequestException as e:
                print(f"    Error fetching news for {pe_firm['name']}: {e}")

            # Brief delay between searches
            time.sleep(0.5)

        return announcements
