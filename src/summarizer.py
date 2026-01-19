"""Claude API integration for generating summaries."""

from datetime import date
from typing import Optional

import anthropic

from .config import Company, config
from .storage import FinancialSnapshot, NewsArticle


class Summarizer:
    """Generates summaries using Claude API."""

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or config.anthropic_api_key
        if not self.api_key:
            raise ValueError("Anthropic API key is required")
        self.client = anthropic.Anthropic(api_key=self.api_key)

    def _format_company_data(
        self,
        company: Company,
        articles: list[NewsArticle],
        snapshot: Optional[FinancialSnapshot]
    ) -> str:
        """Format company data for the prompt."""
        lines = [f"\n## {company.name}"]

        if company.ticker:
            lines[0] += f" ({company.ticker})"

        # Financial data
        if snapshot:
            lines.append("\n### Financial Data")
            if snapshot.price:
                price_line = f"- Current Price: ${snapshot.price:.2f}"
                if snapshot.change_percent is not None:
                    direction = "up" if snapshot.change_percent >= 0 else "down"
                    price_line += f" ({direction} {abs(snapshot.change_percent):.2f}%)"
                lines.append(price_line)

            if snapshot.market_cap:
                cap_str = self._format_large_number(snapshot.market_cap)
                lines.append(f"- Market Cap: {cap_str}")

            if snapshot.high_52w and snapshot.low_52w:
                lines.append(f"- 52-Week Range: ${snapshot.low_52w:.2f} - ${snapshot.high_52w:.2f}")

            if snapshot.volume:
                vol_str = self._format_large_number(snapshot.volume)
                lines.append(f"- Volume: {vol_str}")
        else:
            lines.append("\n### Financial Data")
            lines.append("- No financial data available")

        # News articles
        lines.append(f"\n### News ({len(articles)} articles)")
        if articles:
            for article in articles[:5]:  # Limit to 5 articles per company
                source = f" ({article.source})" if article.source else ""
                lines.append(f"- {article.title}{source}")
                if article.description:
                    # Truncate long descriptions
                    desc = article.description[:200]
                    if len(article.description) > 200:
                        desc += "..."
                    lines.append(f"  {desc}")
        else:
            lines.append("- No recent news articles")

        return "\n".join(lines)

    def _format_large_number(self, num: float) -> str:
        """Format large numbers with B/M/K suffixes."""
        if num >= 1_000_000_000_000:
            return f"${num / 1_000_000_000_000:.2f}T"
        elif num >= 1_000_000_000:
            return f"${num / 1_000_000_000:.2f}B"
        elif num >= 1_000_000:
            return f"${num / 1_000_000:.2f}M"
        elif num >= 1_000:
            return f"${num / 1_000:.2f}K"
        else:
            return f"${num:.2f}"

    def generate_summary(
        self,
        companies: list[Company],
        articles_by_company: dict[str, list[NewsArticle]],
        snapshots_by_company: dict[str, Optional[FinancialSnapshot]],
        target_date: date
    ) -> str:
        """Generate an AI summary of all company data.

        Args:
            companies: List of companies.
            articles_by_company: Dict mapping company name to articles.
            snapshots_by_company: Dict mapping company name to financial snapshot.
            target_date: The date for this summary.

        Returns:
            Generated summary text.
        """
        # Build the data section
        data_sections = []
        for company in companies:
            articles = articles_by_company.get(company.name, [])
            snapshot = snapshots_by_company.get(company.name)
            data_sections.append(
                self._format_company_data(company, articles, snapshot)
            )

        combined_data = "\n".join(data_sections)

        prompt = f"""You are a financial analyst assistant. Below is today's ({target_date.strftime('%B %d, %Y')}) news and financial data for several companies.

Please create a professional daily briefing summary with the following structure:

1. **Executive Summary** (2-3 sentences): Highlight the most significant developments across all companies.

2. **Market Movers**: List any companies with notable price movements (>3% change) or significant news.

3. **Company Highlights**: For each company with noteworthy updates, provide a brief 1-2 sentence summary of:
   - Key news developments
   - Stock performance context
   - Any notable patterns or concerns

4. **Watch List**: Any items that warrant continued monitoring.

Focus on actionable insights and material information. Be concise but comprehensive. If there's no significant news for a company, you can skip or briefly note "No significant updates."

---

# Today's Data

{combined_data}

---

Please generate the daily briefing summary:"""

        try:
            message = self.client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=2000,
                messages=[
                    {"role": "user", "content": prompt}
                ]
            )

            # Extract text from response
            summary = ""
            for block in message.content:
                if block.type == "text":
                    summary += block.text

            return summary

        except Exception as e:
            print(f"Error generating summary: {e}")
            return f"Error generating summary: {str(e)}"
