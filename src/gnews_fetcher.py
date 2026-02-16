"""GNews API integration for international news coverage."""

import time
from datetime import datetime, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

import requests

from .config import Company, config
from .storage import NewsArticle


# Language configurations for companies
# Maps company names to their preferred search languages
COMPANY_LANGUAGES = {
    # German companies
    "Siemens": ["en", "de"],
    "STULZ": ["en", "de"],
    # French companies
    "Schneider Electric": ["en", "fr"],
    "Legrand": ["en", "fr"],
    # Swedish companies (now owned by Samsung but still Swedish operations)
    "FläktGroup": ["en", "sv", "de"],
    # Swiss companies
    "ABB": ["en", "de", "fr"],
    # Japanese companies
    "Daikin": ["en"],
    # US companies - English only
    "Eaton": ["en"],
    "Vertiv": ["en"],
    "Cummins": ["en"],
    "Caterpillar": ["en"],
    "Carrier": ["en"],
    "Trane": ["en"],
    "Johnson Controls": ["en"],
    "Corning": ["en"],
    "Wesco": ["en"],
}

# Language names for logging
LANGUAGE_NAMES = {
    "en": "English",
    "de": "German",
    "fr": "French",
    "sv": "Swedish",
    "ja": "Japanese",
}


class GNewsFetcher:
    """Fetches news from GNews API with multi-language support."""

    BASE_URL = "https://gnews.io/api/v4/search"

    # Data center context terms for query building
    DATA_CENTER_TERMS = [
        "data center",
        "datacenter",
        "rechenzentrum",      # German
        "centre de données",  # French
        "datacentraler",      # Swedish
    ]

    def __init__(self, api_key: Optional[str] = None, translator=None):
        """Initialize the GNews fetcher.

        Args:
            api_key: GNews API key.
            translator: Optional Summarizer instance for translating content.
        """
        self.api_key = api_key or config.gnews_api_key
        self.translator = translator
        if not self.api_key:
            raise ValueError("GNews API key is required")

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
            lang_name = LANGUAGE_NAMES.get(source_lang, source_lang)
            prompt = f"""Translate the following {lang_name} text to English.
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
            print(f"      Translation error: {e}")
            return text

    def _build_query(self, company: Company, language: str) -> str:
        """Build a search query for a company in a specific language.

        Args:
            company: Company to search for.
            language: Language code (en, de, fr, sv).

        Returns:
            Search query string.
        """
        # Use company name and ticker
        company_terms = [f'"{company.name}"']
        if company.ticker:
            company_terms.append(company.ticker)

        # Add data center context based on language
        if language == "de":
            context = '("rechenzentrum" OR "data center" OR "datacenter")'
        elif language == "fr":
            context = '("centre de données" OR "data center" OR "datacenter")'
        elif language == "sv":
            context = '("datacentraler" OR "data center" OR "datacenter")'
        else:
            context = '("data center" OR "datacenter" OR "infrastructure" OR "power" OR "cooling")'

        # Build query: company AND data center context
        company_part = " OR ".join(company_terms)
        query = f"({company_part}) AND {context}"

        return query

    def _fetch_news(
        self,
        query: str,
        language: str,
        max_results: int = 10,
        days_back: int = 7
    ) -> list[dict]:
        """Fetch news from GNews API.

        Args:
            query: Search query.
            language: Language code.
            max_results: Maximum number of results.
            days_back: How many days back to search.

        Returns:
            List of article dictionaries from API.
        """
        from_date = (datetime.utcnow() - timedelta(days=days_back)).strftime("%Y-%m-%dT%H:%M:%SZ")

        params = {
            "q": query,
            "lang": language,
            "max": max_results,
            "from": from_date,
            "apikey": self.api_key,
        }

        try:
            response = requests.get(self.BASE_URL, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            if "errors" in data:
                print(f"      GNews API error: {data['errors']}")
                return []

            return data.get("articles", [])

        except requests.exceptions.RequestException as e:
            print(f"      Error fetching from GNews: {e}")
            return []

    def fetch_company_news(
        self,
        company: Company,
        company_id: int,
        hours_back: int = 72
    ) -> list[NewsArticle]:
        """Fetch news for a company from GNews in multiple languages.

        Args:
            company: Company to fetch news for.
            company_id: Database ID of the company.
            hours_back: How many hours back to search.

        Returns:
            List of NewsArticle objects.
        """
        articles = []
        seen_urls = set()
        days_back = max(1, hours_back // 24)

        # Get languages for this company
        languages = COMPANY_LANGUAGES.get(company.name, ["en"])

        for lang in languages:
            lang_name = LANGUAGE_NAMES.get(lang, lang)
            print(f"    Searching in {lang_name}...")

            query = self._build_query(company, lang)
            results = self._fetch_news(query, lang, max_results=5, days_back=days_back)

            for article_data in results:
                url = article_data.get("url", "")
                if not url or url in seen_urls:
                    continue
                seen_urls.add(url)

                # Parse publication date
                pub_date = None
                pub_str = article_data.get("publishedAt", "")
                if pub_str:
                    try:
                        pub_date = datetime.fromisoformat(pub_str.replace("Z", "+00:00"))
                    except ValueError:
                        pass

                # Get title and description
                title = article_data.get("title", "No title")
                description = article_data.get("description", "")

                # Translate non-English content
                if lang != "en":
                    title = self._translate_content(title, lang)
                    if description:
                        description = self._translate_content(description, lang)

                source = article_data.get("source", {}).get("name", f"GNews ({lang_name})")

                articles.append(NewsArticle(
                    id=None,
                    company_id=company_id,
                    title=title,
                    description=description,
                    source=source,
                    url=url,
                    published_at=pub_date
                ))

        return articles

    def fetch_all_companies(
        self,
        companies: list[Company],
        company_ids: dict[str, int],
        hours_back: int = 72,
        rate_limit_delay: float = 1.0
    ) -> dict[str, list[NewsArticle]]:
        """Fetch news for all companies from GNews.

        Args:
            companies: List of companies.
            company_ids: Dict mapping company name to database ID.
            hours_back: How many hours back to search.
            rate_limit_delay: Delay between API calls (GNews has rate limits).

        Returns:
            Dict mapping company name to list of articles.
        """
        all_articles = {}

        for company in companies:
            company_id = company_ids.get(company.name)
            if not company_id:
                continue

            # Check if company has language config (skip if not configured)
            if company.name not in COMPANY_LANGUAGES:
                all_articles[company.name] = []
                continue

            print(f"  Fetching GNews for {company.name}...")
            articles = self.fetch_company_news(company, company_id, hours_back)
            all_articles[company.name] = articles

            if articles:
                print(f"    Found {len(articles)} articles")

            # Rate limiting - GNews free tier is limited
            if rate_limit_delay > 0:
                time.sleep(rate_limit_delay)

        return all_articles

    def fetch_industry_news(
        self,
        hours_back: int = 72,
        max_results: int = 10
    ) -> list[dict]:
        """Fetch general data center industry news.

        Args:
            hours_back: How many hours back to search.
            max_results: Maximum number of results.

        Returns:
            List of article dictionaries.
        """
        days_back = max(1, hours_back // 24)
        query = '"data center" OR "datacenter" OR "hyperscaler" OR "cloud infrastructure"'

        print("  Fetching data center industry news...")
        results = self._fetch_news(query, "en", max_results=max_results, days_back=days_back)

        articles = []
        for article_data in results:
            pub_date = None
            pub_str = article_data.get("publishedAt", "")
            if pub_str:
                try:
                    pub_date = datetime.fromisoformat(pub_str.replace("Z", "+00:00"))
                except ValueError:
                    pass

            articles.append({
                "title": article_data.get("title", "No title"),
                "description": article_data.get("description", ""),
                "url": article_data.get("url", ""),
                "source": article_data.get("source", {}).get("name", "GNews"),
                "published_at": pub_date
            })

        print(f"    Found {len(articles)} industry articles")
        return articles
