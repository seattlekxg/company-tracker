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
        snapshot: Optional[FinancialSnapshot],
        filings: list = None
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

        # SEC Filings
        if filings:
            lines.append(f"\n### SEC Filings ({len(filings)} recent filings)")
            for filing in filings[:5]:  # Limit to 5 filings per company
                filed_date = filing.filed_at.strftime("%Y-%m-%d") if filing.filed_at else "Unknown date"
                lines.append(f"- **{filing.form_type}** (Filed: {filed_date})")
                if filing.description:
                    lines.append(f"  Description: {filing.description[:200]}")
                if filing.content_summary:
                    lines.append(f"  AI Analysis: {filing.content_summary}")
                lines.append(f"  URL: {filing.filing_url}")
        else:
            lines.append("\n### SEC Filings")
            lines.append("- No recent SEC filings")

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

    def analyze_sec_filing(self, filing, filing_content: str) -> str:
        """Analyze an SEC filing for data center-related content.

        Args:
            filing: The SECFiling object.
            filing_content: The text content of the filing.

        Returns:
            AI-generated summary focused on data center relevance.
        """
        # Truncate content to avoid token limits
        max_content_length = 30000
        if len(filing_content) > max_content_length:
            filing_content = filing_content[:max_content_length] + "\n...[truncated]"

        prompt = f"""Analyze this SEC {filing.form_type} filing from {filing.company_name} ({filing.ticker}).

Focus specifically on:
1. Any mentions of data centers, data center infrastructure, or related products
2. Revenue or business segments related to power management, UPS systems, generators, cooling systems, or electrical infrastructure for data centers
3. Significant business changes, risks, or opportunities related to data center markets
4. Any partnerships, contracts, or expansions in the data center space
5. Forward-looking statements about data center business

If there is NO data center-related content, simply state "No significant data center-related content found."

Be concise - provide a 2-4 sentence summary of the most relevant findings.

Filing Content:
{filing_content}

Summary:"""

        try:
            message = self.client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=500,
                messages=[
                    {"role": "user", "content": prompt}
                ]
            )

            summary = ""
            for block in message.content:
                if block.type == "text":
                    summary += block.text

            return summary.strip()

        except Exception as e:
            print(f"Error analyzing SEC filing: {e}")
            return "Error analyzing filing content"

    def generate_summary(
        self,
        companies: list[Company],
        articles_by_company: dict[str, list[NewsArticle]],
        snapshots_by_company: dict[str, Optional[FinancialSnapshot]],
        filings_by_company: dict[str, list] = None,
        target_date: date = None
    ) -> str:
        """Generate an AI summary of all company data.

        Args:
            companies: List of companies.
            articles_by_company: Dict mapping company name to articles.
            snapshots_by_company: Dict mapping company name to financial snapshot.
            filings_by_company: Dict mapping company name to SEC filings.
            target_date: The date for this summary.

        Returns:
            Generated summary text.
        """
        if target_date is None:
            target_date = date.today()

        if filings_by_company is None:
            filings_by_company = {}

        # Build the data section
        data_sections = []
        for company in companies:
            articles = articles_by_company.get(company.name, [])
            snapshot = snapshots_by_company.get(company.name)
            filings = filings_by_company.get(company.name, [])
            data_sections.append(
                self._format_company_data(company, articles, snapshot, filings)
            )

        combined_data = "\n".join(data_sections)

        prompt = f"""You are a financial analyst assistant specializing in data center infrastructure companies. Below is today's ({target_date.strftime('%B %d, %Y')}) news, financial data, and SEC filings for companies in the data center power and infrastructure space.

Please create a professional daily briefing summary with the following structure:

1. **Executive Summary** (2-3 sentences): Highlight the most significant developments across all companies, with special attention to data center-related news and SEC disclosures.

2. **SEC Filing Highlights**: Summarize any important SEC filings, especially those with data center-related content. Note any material changes, risk factors, or business developments disclosed.

3. **Market Movers**: List any companies with notable price movements (>3% change) or significant news.

4. **Company Highlights**: For each company with noteworthy updates, provide a brief 1-2 sentence summary of:
   - Key news developments (especially data center related)
   - SEC filing insights
   - Stock performance context
   - Any notable patterns or concerns

5. **Data Center Market Insights**: Any trends or patterns relevant to the data center infrastructure market based on today's information.

6. **Watch List**: Any items that warrant continued monitoring.

Focus on actionable insights and material information, particularly as it relates to data center products and markets. Be concise but comprehensive. If there's no significant news for a company, you can skip or briefly note "No significant updates."

---

# Today's Data

{combined_data}

---

Please generate the daily briefing summary:"""

        try:
            message = self.client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=3000,
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
