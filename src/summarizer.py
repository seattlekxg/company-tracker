"""Claude API integration for generating summaries."""

import time
from datetime import date
from typing import Optional

import anthropic
from anthropic.types.message_create_params import MessageCreateParamsNonStreaming
from anthropic.types.messages.batch_create_params import Request

from .config import MODEL_HAIKU, MODEL_SONNET, Company, config
from .storage import FinancialSnapshot, NewsArticle


class Summarizer:
    """Generates summaries using Claude API."""

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or config.anthropic_api_key
        if not self.api_key:
            raise ValueError("Anthropic API key is required")
        self.client = anthropic.Anthropic(api_key=self.api_key)

    # ========== Helper: extract text from response ==========

    @staticmethod
    def _extract_text(message) -> str:
        """Extract text content from an API response message."""
        parts = []
        for block in message.content:
            if block.type == "text":
                parts.append(block.text)
        return "".join(parts)

    # ========== Batch Helpers ==========

    def submit_batch(self, requests: list[Request]) -> str:
        """Submit a batch of requests to the Anthropic Batch API.

        Args:
            requests: List of Request objects for the batch.

        Returns:
            The batch ID string.
        """
        batch = self.client.messages.batches.create(requests=requests)
        return batch.id

    def poll_batch(self, batch_id: str, poll_interval: int = 30) -> None:
        """Poll a batch until it finishes processing.

        Args:
            batch_id: The batch ID to poll.
            poll_interval: Seconds between poll attempts.
        """
        while True:
            batch = self.client.messages.batches.retrieve(batch_id)
            if batch.processing_status == "ended":
                print(f"  Batch {batch_id} complete: "
                      f"{batch.request_counts.succeeded} succeeded, "
                      f"{batch.request_counts.errored} errored")
                return
            print(f"  Batch {batch_id}: {batch.processing_status} "
                  f"({batch.request_counts.processing} processing)")
            time.sleep(poll_interval)

    def get_batch_results(self, batch_id: str) -> dict[str, str]:
        """Retrieve results from a completed batch.

        Args:
            batch_id: The batch ID to retrieve results from.

        Returns:
            Dict mapping custom_id to response text for succeeded results.
        """
        results = {}
        for result in self.client.messages.batches.results(batch_id):
            if result.result.type == "succeeded":
                text = self._extract_text(result.result.message)
                results[result.custom_id] = text
            else:
                print(f"    Batch item {result.custom_id} failed: {result.result.type}")
        return results

    # ========== 1. Filter Relevant Articles ==========

    def _build_filter_params(
        self, company_name: str, articles: list[NewsArticle]
    ) -> dict:
        """Build API call parameters for article filtering."""
        articles_text = ""
        for i, article in enumerate(articles):
            title = article.title or "No title"
            desc = (article.description or "")[:200]
            articles_text += f"\n{i+1}. Title: {title}\n   Description: {desc}\n"

        system_text = (
            f"You are filtering news articles for {company_name}, a company in the data center infrastructure space.\n\n"
            f"Review the articles and identify which ones are RELEVANT to:\n"
            f"- {company_name}'s business in data centers, power systems, cooling, or infrastructure\n"
            f"- Data center industry news involving {company_name}\n"
            f"- {company_name}'s products/services for data centers (UPS, generators, cooling, power distribution)\n"
            f"- Financial news specifically about {company_name} (earnings, contracts, partnerships)\n\n"
            f"EXCLUDE articles that:\n"
            f"- Mention \"{company_name}\" but are about unrelated topics (politics, sports, other industries)\n"
            f"- Are about different companies or people with similar names\n"
            f"- Have no clear connection to data center infrastructure business\n\n"
            f"Return ONLY the numbers of relevant articles as a comma-separated list.\n"
            f"If no articles are relevant, return \"NONE\".\n"
            f"Example response: 1, 3, 5"
        )

        return {
            "model": MODEL_HAIKU,
            "max_tokens": 100,
            "system": [
                {
                    "type": "text",
                    "text": system_text,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            "messages": [
                {"role": "user", "content": f"Articles to review:\n{articles_text}"}
            ],
        }

    def _parse_filter_response(
        self, response_text: str, articles: list[NewsArticle]
    ) -> list[NewsArticle]:
        """Parse the filter response into a list of relevant articles."""
        response_text = response_text.strip().upper()

        if response_text == "NONE" or not response_text:
            return []

        try:
            relevant_indices = [
                int(n.strip()) - 1
                for n in response_text.replace(" ", "").split(",")
                if n.strip().isdigit()
            ]
            return [
                articles[i] for i in relevant_indices
                if 0 <= i < len(articles)
            ]
        except (ValueError, IndexError):
            return articles

    def filter_relevant_articles(
        self,
        company_name: str,
        articles: list[NewsArticle]
    ) -> list[NewsArticle]:
        """Filter articles to only those relevant to data center infrastructure."""
        if not articles:
            return []

        try:
            params = self._build_filter_params(company_name, articles)
            message = self.client.messages.create(**params)
            response_text = self._extract_text(message)
            return self._parse_filter_response(response_text, articles)
        except Exception as e:
            print(f"    Error filtering articles: {e}")
            return articles

    # ========== 2. Analyze SEC Filing ==========

    def _build_sec_filing_params(self, filing, filing_content: str) -> dict:
        """Build API call parameters for SEC filing analysis."""
        max_content_length = 30000
        if len(filing_content) > max_content_length:
            filing_content = filing_content[:max_content_length] + "\n...[truncated]"

        system_text = (
            f"You are analyzing SEC {filing.form_type} filings from {filing.company_name} ({filing.ticker}).\n\n"
            "Focus specifically on:\n"
            "1. Any mentions of data centers, data center infrastructure, or related products\n"
            "2. Revenue or business segments related to power management, UPS systems, generators, "
            "cooling systems, or electrical infrastructure for data centers\n"
            "3. Significant business changes, risks, or opportunities related to data center markets\n"
            "4. Any partnerships, contracts, or expansions in the data center space\n"
            "5. Forward-looking statements about data center business\n\n"
            'If there is NO data center-related content, simply state "No significant data center-related content found."\n\n'
            "Be concise - provide a 2-4 sentence summary of the most relevant findings."
        )

        return {
            "model": MODEL_SONNET,
            "max_tokens": 500,
            "system": [
                {
                    "type": "text",
                    "text": system_text,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            "messages": [
                {"role": "user", "content": f"Filing Content:\n{filing_content}\n\nSummary:"}
            ],
        }

    @staticmethod
    def _parse_sec_filing_response(response_text: str) -> str:
        """Parse SEC filing analysis response."""
        return response_text.strip()

    def analyze_sec_filing(self, filing, filing_content: str) -> str:
        """Analyze an SEC filing for data center-related content."""
        try:
            params = self._build_sec_filing_params(filing, filing_content)
            message = self.client.messages.create(**params)
            return self._parse_sec_filing_response(self._extract_text(message))
        except Exception as e:
            print(f"Error analyzing SEC filing: {e}")
            return "Error analyzing filing content"

    # ========== 3. Analyze Earnings Transcript ==========

    def _build_transcript_params(self, transcript, content: str) -> dict:
        """Build API call parameters for earnings transcript analysis."""
        max_content_length = 30000
        if len(content) > max_content_length:
            content = content[:max_content_length] + "\n...[truncated]"

        system_text = (
            f"You are analyzing earnings call transcripts from {transcript.ticker} ({transcript.quarter}).\n\n"
            "Focus specifically on extracting signals relevant to data center infrastructure supply chain:\n"
            "1. **Backlog and Orders**: Any mentions of order backlog, bookings, or pipeline for data center products\n"
            "2. **Lead Times**: Discussion of delivery lead times, production capacity, or supply constraints\n"
            "3. **Capacity Utilization**: Factory utilization rates, production ramp-up, or capacity expansion plans\n"
            "4. **Data Center Demand**: Commentary on hyperscaler demand, data center market trends, or AI infrastructure needs\n"
            "5. **Guidance and Outlook**: Forward-looking statements about data center business segments\n\n"
            'If there is NO relevant data center supply chain content, simply state '
            '"No significant data center supply chain signals found."\n\n'
            "Be concise - provide a 2-4 sentence summary of the most actionable supply chain insights."
        )

        return {
            "model": MODEL_SONNET,
            "max_tokens": 500,
            "system": [
                {
                    "type": "text",
                    "text": system_text,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            "messages": [
                {"role": "user", "content": f"Transcript Content:\n{content}\n\nSummary:"}
            ],
        }

    @staticmethod
    def _parse_transcript_response(response_text: str) -> str:
        """Parse transcript analysis response."""
        return response_text.strip()

    def analyze_earnings_transcript(self, transcript, content: str) -> str:
        """Analyze an earnings call transcript for data center-related insights."""
        try:
            params = self._build_transcript_params(transcript, content)
            message = self.client.messages.create(**params)
            return self._parse_transcript_response(self._extract_text(message))
        except Exception as e:
            print(f"Error analyzing transcript: {e}")
            return "Error analyzing transcript content"

    # ========== MW Tag Parsing ==========

    def _parse_mw_tags(self, text: str) -> dict:
        """Parse CAPACITY_MW and TARGET_YEAR tags from AI response text."""
        import re

        capacity_mw = None
        target_year = None
        summary_lines = []

        for line in text.strip().split("\n"):
            stripped = line.strip()
            mw_match = re.match(r"^CAPACITY_MW:\s*(.+)$", stripped, re.IGNORECASE)
            year_match = re.match(r"^TARGET_YEAR:\s*(.+)$", stripped, re.IGNORECASE)

            if mw_match:
                val = mw_match.group(1).strip()
                if val.upper() != "UNKNOWN":
                    try:
                        capacity_mw = float(val.replace(",", ""))
                    except ValueError:
                        pass
            elif year_match:
                val = year_match.group(1).strip()
                if val.upper() != "UNKNOWN":
                    try:
                        target_year = int(val)
                    except ValueError:
                        pass
            else:
                summary_lines.append(line)

        return {
            "summary": "\n".join(summary_lines).strip(),
            "capacity_mw": capacity_mw,
            "target_year": target_year,
        }

    # ========== 4. Analyze Hyperscaler Announcement ==========

    _HYPERSCALER_SYSTEM = (
        "You are analyzing hyperscaler data center announcements.\n\n"
        "Extract key details relevant to data center infrastructure suppliers:\n"
        "1. **Location**: Where is the data center being built or expanded?\n"
        "2. **Scale**: What is the size (MW, square footage, investment amount)?\n"
        "3. **Timeline**: When is construction expected and completion planned?\n"
        "4. **Supplier Impact**: Which types of suppliers might benefit (power, cooling, connectivity)?\n\n"
        "Be concise - provide a 2-3 sentence summary with the most relevant facts for infrastructure suppliers.\n\n"
        "After your summary, on separate lines, output these two structured tags:\n"
        "CAPACITY_MW: <total MW of datacenter capacity announced, as a number, or UNKNOWN if not mentioned>\n"
        "TARGET_YEAR: <year when capacity is expected to be operational, as a 4-digit year, or UNKNOWN if not mentioned>"
    )

    def _build_hyperscaler_params(self, announcement) -> dict:
        """Build API call parameters for hyperscaler announcement analysis."""
        content = f"Title: {announcement.title}\n"
        if announcement.description:
            content += f"Description: {announcement.description}"

        return {
            "model": MODEL_SONNET,
            "max_tokens": 350,
            "system": [
                {
                    "type": "text",
                    "text": f"This is a {announcement.hyperscaler} announcement.\n\n" + self._HYPERSCALER_SYSTEM,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            "messages": [
                {"role": "user", "content": f"Article:\n{content}\n\nSummary:"}
            ],
        }

    def _parse_hyperscaler_response(self, response_text: str) -> dict:
        """Parse hyperscaler analysis response."""
        return self._parse_mw_tags(response_text)

    def analyze_hyperscaler_announcement(self, announcement) -> dict:
        """Analyze a hyperscaler data center announcement."""
        try:
            params = self._build_hyperscaler_params(announcement)
            message = self.client.messages.create(**params)
            return self._parse_hyperscaler_response(self._extract_text(message))
        except Exception as e:
            print(f"Error analyzing hyperscaler announcement: {e}")
            return {"summary": "Error analyzing announcement", "capacity_mw": None, "target_year": None}

    # ========== 5. Analyze PE Announcement ==========

    _PE_SYSTEM = (
        "You are analyzing Private Equity data center investment announcements.\n\n"
        "Extract key details relevant to data center infrastructure market:\n"
        "1. **Deal Type**: Is this an acquisition, expansion, new investment, or partnership?\n"
        "2. **Target/Portfolio**: What data center assets or companies are involved?\n"
        "3. **Scale**: What is the deal value, capacity (MW), or size?\n"
        "4. **Location**: Where are the assets located?\n"
        "5. **Market Signal**: What does this indicate about PE firm appetite for data center investments?\n\n"
        "Be concise - provide a 2-3 sentence summary with the most relevant facts for understanding PE activity in data centers.\n\n"
        "After your summary, on separate lines, output these two structured tags:\n"
        "CAPACITY_MW: <total MW of datacenter capacity announced, as a number, or UNKNOWN if not mentioned>\n"
        "TARGET_YEAR: <year when capacity is expected to be operational, as a 4-digit year, or UNKNOWN if not mentioned>"
    )

    def _build_pe_params(self, announcement) -> dict:
        """Build API call parameters for PE announcement analysis."""
        content = f"Title: {announcement.title}\n"
        if announcement.description:
            content += f"Description: {announcement.description}"

        return {
            "model": MODEL_SONNET,
            "max_tokens": 350,
            "system": [
                {
                    "type": "text",
                    "text": f"This is a {announcement.pe_firm} announcement.\n\n" + self._PE_SYSTEM,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            "messages": [
                {"role": "user", "content": f"Article:\n{content}\n\nSummary:"}
            ],
        }

    def _parse_pe_response(self, response_text: str) -> dict:
        """Parse PE analysis response."""
        return self._parse_mw_tags(response_text)

    def analyze_pe_announcement(self, announcement) -> dict:
        """Analyze a Private Equity data center investment announcement."""
        try:
            params = self._build_pe_params(announcement)
            message = self.client.messages.create(**params)
            return self._parse_pe_response(self._extract_text(message))
        except Exception as e:
            print(f"Error analyzing PE announcement: {e}")
            return {"summary": "Error analyzing announcement", "capacity_mw": None, "target_year": None}

    # ========== 6. Extract MW from Summary ==========

    _MW_EXTRACTION_SYSTEM = (
        "You extract structured data from data center announcement summaries.\n\n"
        "From the given summary, extract:\n"
        "1. The total MW of datacenter capacity announced (if mentioned)\n"
        "2. The target year when the capacity is expected to be operational (if mentioned)\n\n"
        "Respond with exactly two lines:\n"
        "CAPACITY_MW: <number or UNKNOWN>\n"
        "TARGET_YEAR: <4-digit year or UNKNOWN>"
    )

    def _build_mw_extraction_params(self, content_summary: str) -> dict:
        """Build API call parameters for MW extraction."""
        return {
            "model": MODEL_HAIKU,
            "max_tokens": 50,
            "system": [
                {
                    "type": "text",
                    "text": self._MW_EXTRACTION_SYSTEM,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            "messages": [
                {"role": "user", "content": f"Summary:\n{content_summary}"}
            ],
        }

    def _parse_mw_extraction_response(self, response_text: str) -> dict:
        """Parse MW extraction response."""
        parsed = self._parse_mw_tags(response_text)
        return {
            "capacity_mw": parsed["capacity_mw"],
            "target_year": parsed["target_year"],
        }

    def extract_mw_from_summary(self, content_summary: str) -> dict:
        """Extract MW capacity and target year from an existing summary text."""
        try:
            params = self._build_mw_extraction_params(content_summary)
            message = self.client.messages.create(**params)
            return self._parse_mw_extraction_response(self._extract_text(message))
        except Exception as e:
            print(f"Error extracting MW from summary: {e}")
            return {"capacity_mw": None, "target_year": None}

    # ========== Non-Batchable Methods ==========

    def _format_company_data(
        self,
        company: Company,
        articles: list[NewsArticle],
        snapshot: Optional[FinancialSnapshot],
        filings: list = None,
        transcripts: list = None
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
            for filing in filings[:5]:
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

        # Earnings Call Transcripts
        if transcripts:
            lines.append(f"\n### Earnings Call Transcripts ({len(transcripts)} recent)")
            for transcript in transcripts[:2]:
                lines.append(f"- **{transcript.quarter}** ({transcript.ticker})")
                if transcript.content_summary:
                    lines.append(f"  AI Analysis: {transcript.content_summary}")
        else:
            lines.append("\n### Earnings Call Transcripts")
            lines.append("- No recent transcripts")

        # News articles
        lines.append(f"\n### News ({len(articles)} articles)")
        if articles:
            for article in articles[:5]:
                source = f" ({article.source})" if article.source else ""
                lines.append(f"- {article.title}{source}")
                if article.description:
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

    def generate_seasonal_summary(
        self,
        quarter: str,
        companies: list[Company],
        transcripts_by_company: dict[str, list],
        filings_by_company: dict[str, list],
        articles_by_company: dict[str, list[NewsArticle]],
        snapshots_start: dict[str, Optional[FinancialSnapshot]],
        snapshots_end: dict[str, Optional[FinancialSnapshot]],
        hyperscaler_announcements: list = None,
        pe_announcements: list = None
    ) -> dict:
        """Generate a structured seasonal earnings summary for PPT generation."""
        if hyperscaler_announcements is None:
            hyperscaler_announcements = []
        if pe_announcements is None:
            pe_announcements = []

        # Build comprehensive data context for the AI
        data_sections = []
        for company in companies:
            if not company.ticker:
                continue
            lines = [f"\n## {company.name} ({company.ticker})"]

            transcripts = transcripts_by_company.get(company.name, [])
            if transcripts:
                lines.append("\n### Earnings Call Insights")
                for t in transcripts:
                    if t.content_summary:
                        lines.append(f"- {t.content_summary}")
            else:
                lines.append("\n### Earnings Call: No transcript available")

            filings = filings_by_company.get(company.name, [])
            if filings:
                lines.append(f"\n### SEC Filings ({len(filings)})")
                for f in filings[:3]:
                    if f.content_summary:
                        lines.append(f"- {f.form_type}: {f.content_summary}")

            start_snap = snapshots_start.get(company.name)
            end_snap = snapshots_end.get(company.name)
            if start_snap and start_snap.price and end_snap and end_snap.price:
                change = ((end_snap.price - start_snap.price) / start_snap.price) * 100
                lines.append(f"\n### Stock: ${start_snap.price:.2f} -> ${end_snap.price:.2f} ({change:+.1f}%)")

            articles = articles_by_company.get(company.name, [])
            if articles:
                lines.append(f"\n### Key News ({len(articles)} articles)")
                for a in articles[:5]:
                    lines.append(f"- {a.title}")

            data_sections.append("\n".join(lines))

        combined_data = "\n".join(data_sections)

        hyperscaler_context = ""
        if hyperscaler_announcements:
            hyperscaler_context = "\n\n# Hyperscaler Data Center Activity\n"
            for ann in hyperscaler_announcements[:15]:
                hyperscaler_context += f"\n## {ann.hyperscaler}\n"
                hyperscaler_context += f"- **{ann.title}**\n"
                if ann.content_summary:
                    hyperscaler_context += f"  Analysis: {ann.content_summary}\n"

        pe_context = ""
        if pe_announcements:
            pe_context = "\n\n# Private Equity Data Center Activity\n"
            for ann in pe_announcements[:15]:
                pe_context += f"\n## {ann.pe_firm}\n"
                pe_context += f"- **{ann.title}**\n"
                if ann.content_summary:
                    pe_context += f"  Analysis: {ann.content_summary}\n"

        system_text = (
            f"You are a senior financial analyst creating a {quarter} earnings season summary "
            "for data center infrastructure companies.\n\n"
            "Please create a comprehensive, structured analysis with the following EXACT sections. "
            "Each section should be formatted with bullet points for clarity.\n\n"
            "IMPORTANT: Return your analysis in the following exact format with these section headers:\n\n"
            "===EXECUTIVE_SUMMARY===\n"
            f"Write 5-8 bullet points providing a high-level overview of the {quarter} earnings season. Cover:\n"
            "- Overall earnings quality across tracked companies\n"
            "- Key demand trends (especially hyperscaler/AI infrastructure demand)\n"
            "- Supply chain conditions (lead times, capacity, backlogs)\n"
            "- Notable stock market performance patterns\n"
            "- Any significant corporate actions or strategic shifts\n\n"
            "===SECTOR_THEMES===\n"
            "Write 6-10 bullet points identifying the major themes across all earnings calls. Focus on:\n"
            "- Common demand drivers mentioned across multiple companies\n"
            "- Shared supply chain challenges or improvements\n"
            "- Pricing trends and margin dynamics\n"
            "- Technology/product evolution themes\n"
            "- Geographic expansion patterns\n"
            "- Competitive dynamics\n\n"
            "===COMPANY_HIGHLIGHTS===\n"
            "For each company that reported earnings, write a section formatted as:\n"
            "### Company Name\n"
            "- 3-5 bullet points covering: earnings call key takeaways, stock reaction, SEC filing insights, and any notable news\n\n"
            "===HYPERSCALER_SUMMARY===\n"
            "Write 5-8 bullet points summarizing hyperscaler data center activity during the season:\n"
            "- Major expansion announcements and their scale\n"
            "- Geographic focus areas\n"
            "- Implications for infrastructure suppliers\n"
            "- Demand signals and capacity planning\n\n"
            "===PE_SUMMARY===\n"
            "Write 4-6 bullet points summarizing Private Equity activity:\n"
            "- Key deals and investments\n"
            "- Asset types being targeted\n"
            "- Deal sizes and valuations\n"
            "- Market appetite signals\n\n"
            "===OUTLOOK===\n"
            "Write 5-8 bullet points on forward guidance themes:\n"
            "- Consensus outlook from earnings calls\n"
            "- Key risks and uncertainties\n"
            "- Growth drivers for next quarter\n"
            "- Items to watch going forward"
        )

        user_content = (
            f"# {quarter} Earnings Season Data\n\n"
            f"{combined_data}\n"
            f"{hyperscaler_context}\n"
            f"{pe_context}\n\n"
            "---\n\n"
            "Please generate the analysis now:"
        )

        try:
            message = self.client.messages.create(
                model=MODEL_SONNET,
                max_tokens=6000,
                system=[
                    {
                        "type": "text",
                        "text": system_text,
                        "cache_control": {"type": "ephemeral"},
                    }
                ],
                messages=[{"role": "user", "content": user_content}]
            )

            response_text = self._extract_text(message)

            result = {
                "executive_summary": "",
                "sector_themes": "",
                "company_highlights": {},
                "hyperscaler_summary": "",
                "pe_summary": "",
                "outlook": ""
            }

            sections = {
                "===EXECUTIVE_SUMMARY===": "executive_summary",
                "===SECTOR_THEMES===": "sector_themes",
                "===COMPANY_HIGHLIGHTS===": "company_highlights_raw",
                "===HYPERSCALER_SUMMARY===": "hyperscaler_summary",
                "===PE_SUMMARY===": "pe_summary",
                "===OUTLOOK===": "outlook",
            }

            for marker, key in sections.items():
                start_idx = response_text.find(marker)
                if start_idx == -1:
                    continue

                content_start = start_idx + len(marker)

                end_idx = len(response_text)
                for other_marker in sections:
                    if other_marker == marker:
                        continue
                    other_idx = response_text.find(other_marker, content_start)
                    if other_idx != -1 and other_idx < end_idx:
                        end_idx = other_idx

                section_content = response_text[content_start:end_idx].strip()

                if key == "company_highlights_raw":
                    result["company_highlights"] = self._parse_company_highlights(
                        section_content
                    )
                else:
                    result[key] = section_content

            return result

        except Exception as e:
            print(f"Error generating seasonal summary: {e}")
            return {
                "executive_summary": f"Error generating summary: {str(e)}",
                "sector_themes": "",
                "company_highlights": {},
                "hyperscaler_summary": "",
                "pe_summary": "",
                "outlook": ""
            }

    def _parse_company_highlights(self, raw_text: str) -> dict:
        """Parse company highlights section into per-company dict."""
        highlights = {}
        current_company = None
        current_lines = []

        for line in raw_text.split("\n"):
            stripped = line.strip()
            if stripped.startswith("### "):
                if current_company:
                    highlights[current_company] = "\n".join(current_lines).strip()
                current_company = stripped[4:].strip()
                current_lines = []
            elif current_company:
                current_lines.append(line)

        if current_company:
            highlights[current_company] = "\n".join(current_lines).strip()

        return highlights

    def generate_summary(
        self,
        companies: list[Company],
        articles_by_company: dict[str, list[NewsArticle]],
        snapshots_by_company: dict[str, Optional[FinancialSnapshot]],
        filings_by_company: dict[str, list] = None,
        transcripts_by_company: dict[str, list] = None,
        hyperscaler_announcements: list = None,
        target_date: date = None,
        is_weekly: bool = False,
        week_start_date: date = None,
        pe_announcements: list = None
    ) -> str:
        """Generate an AI summary of all company data."""
        if target_date is None:
            target_date = date.today()

        if filings_by_company is None:
            filings_by_company = {}

        if transcripts_by_company is None:
            transcripts_by_company = {}

        if hyperscaler_announcements is None:
            hyperscaler_announcements = []

        if pe_announcements is None:
            pe_announcements = []

        # Build the data section
        data_sections = []
        for company in companies:
            articles = articles_by_company.get(company.name, [])
            snapshot = snapshots_by_company.get(company.name)
            filings = filings_by_company.get(company.name, [])
            transcripts = transcripts_by_company.get(company.name, [])
            data_sections.append(
                self._format_company_data(company, articles, snapshot, filings, transcripts)
            )

        combined_data = "\n".join(data_sections)

        # Build hyperscaler section
        hyperscaler_section = ""
        if hyperscaler_announcements:
            hyperscaler_section = "\n\n# Hyperscaler Data Center Activity\n"
            for ann in hyperscaler_announcements[:10]:
                hyperscaler_section += f"\n## {ann.hyperscaler}\n"
                hyperscaler_section += f"- **{ann.title}**\n"
                if ann.content_summary:
                    hyperscaler_section += f"  Analysis: {ann.content_summary}\n"

        # Build PE data center section
        pe_section = ""
        if pe_announcements:
            pe_section = "\n\n# Private Equity Data Center Activity\n"
            for ann in pe_announcements[:10]:
                pe_section += f"\n## {ann.pe_firm}\n"
                pe_section += f"- **{ann.title}**\n"
                if ann.content_summary:
                    pe_section += f"  Analysis: {ann.content_summary}\n"

        # Generate different prompts for daily vs weekly summaries
        if is_weekly:
            date_range_str = f"{week_start_date.strftime('%B %d')} - {target_date.strftime('%B %d, %Y')}"
            system_text = (
                "You are a financial analyst assistant specializing in data center infrastructure companies. "
                "Create a professional WEEKLY SUMMARY with the following structure:\n\n"
                "1. **Week in Review** (3-4 sentences)\n"
                "2. **Hyperscaler Demand Signals**\n"
                "3. **Private Equity Activity**\n"
                "4. **Supplier Capacity Signals**\n"
                "5. **SEC Filing Summary**\n"
                "6. **Weekly Market Performance**\n"
                "7. **Company-by-Company Highlights**\n"
                "8. **Data Center Market Trends**\n"
                "9. **Looking Ahead**\n\n"
                "Focus on synthesizing the week's information into actionable insights. "
                "Identify patterns and connections across different data sources. Be comprehensive but organized."
            )
            user_content = (
                f"Below is this week's ({date_range_str}) aggregated news, financial data, "
                "SEC filings, and earnings call insights.\n\n"
                f"# This Week's Data ({date_range_str})\n\n"
                f"{combined_data}\n"
                f"{hyperscaler_section}\n"
                f"{pe_section}\n\n"
                "---\n\nPlease generate the weekly summary:"
            )
        else:
            system_text = (
                "You are a financial analyst assistant specializing in data center infrastructure companies. "
                "Create a professional daily briefing summary with the following structure:\n\n"
                "1. **Executive Summary** (2-3 sentences)\n"
                "2. **Hyperscaler Demand Signals**\n"
                "3. **Private Equity Activity**\n"
                "4. **Supplier Capacity Signals**\n"
                "5. **SEC Filing Highlights**\n"
                "6. **Market Movers**\n"
                "7. **Company Highlights**\n"
                "8. **Data Center Market Insights**\n\n"
                "Focus on actionable insights and material information, particularly as it relates to "
                "data center products and markets. Be concise but comprehensive."
            )
            user_content = (
                f"Below is today's ({target_date.strftime('%B %d, %Y')}) news, financial data, "
                "SEC filings, and earnings call insights.\n\n"
                f"# Today's Data\n\n"
                f"{combined_data}\n"
                f"{hyperscaler_section}\n"
                f"{pe_section}\n\n"
                "---\n\nPlease generate the daily briefing summary:"
            )

        try:
            message = self.client.messages.create(
                model=MODEL_SONNET,
                max_tokens=3000,
                system=[
                    {
                        "type": "text",
                        "text": system_text,
                        "cache_control": {"type": "ephemeral"},
                    }
                ],
                messages=[{"role": "user", "content": user_content}]
            )

            return self._extract_text(message)

        except Exception as e:
            print(f"Error generating summary: {e}")
            return f"Error generating summary: {str(e)}"
