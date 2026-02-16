"""Resend email sending integration."""

import os
from datetime import date
from typing import Optional

import resend

from .config import Company, config
from .storage import FinancialSnapshot, NewsArticle


class EmailSender:
    """Sends email digests via Resend."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        from_email: Optional[str] = None,
        to_email: Optional[str] = None
    ):
        self.api_key = api_key or config.resend_api_key
        self.from_email = from_email or config.email_from
        self.to_email = to_email or config.email_to

        if not self.api_key:
            raise ValueError("Resend API key is required")
        if not self.from_email:
            raise ValueError("From email address is required")
        if not self.to_email:
            raise ValueError("To email address is required")

        resend.api_key = self.api_key

    def _load_template(self) -> str:
        """Load the HTML email template."""
        template_path = os.path.join(
            os.path.dirname(__file__),
            "..",
            "templates",
            "email_template.html"
        )

        if os.path.exists(template_path):
            with open(template_path, "r") as f:
                return f.read()

        # Fallback to basic template
        return """
<!DOCTYPE html>
<html>
<head>
    <style>
        body { font-family: Arial, sans-serif; line-height: 1.6; color: #333; }
        .container { max-width: 600px; margin: 0 auto; padding: 20px; }
        .header { background: #1a365d; color: white; padding: 20px; border-radius: 8px 8px 0 0; }
        .content { background: #f7fafc; padding: 20px; border: 1px solid #e2e8f0; }
        .company-card { background: white; padding: 15px; margin: 10px 0; border-radius: 8px; border-left: 4px solid #3182ce; }
        .price-up { color: #38a169; }
        .price-down { color: #e53e3e; }
        .footer { text-align: center; color: #718096; font-size: 12px; padding: 20px; }
        h1 { margin: 0; }
        h2 { color: #2d3748; border-bottom: 2px solid #3182ce; padding-bottom: 5px; }
        h3 { color: #4a5568; margin: 10px 0; }
        ul { padding-left: 20px; }
        a { color: #3182ce; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Daily Company Briefing</h1>
            <p>{{DATE}}</p>
        </div>
        <div class="content">
            {{SUMMARY}}
            {{COMPANY_DETAILS}}
        </div>
        <div class="footer">
            <p>This report was generated automatically by Company Tracker.</p>
        </div>
    </div>
</body>
</html>
"""

    def _format_company_html(
        self,
        company: Company,
        articles: list[NewsArticle],
        snapshot: Optional[FinancialSnapshot],
        filings: list = None,
        transcripts: list = None
    ) -> str:
        """Format a company's data as HTML."""
        html = f'<div class="company-card">'
        html += f'<h3>{company.name}'
        if company.ticker:
            html += f' ({company.ticker})'
        html += '</h3>'

        # Financial info
        if snapshot and snapshot.price:
            price_class = "price-up" if (snapshot.change_percent or 0) >= 0 else "price-down"
            arrow = "+" if (snapshot.change_percent or 0) >= 0 else ""
            html += f'<p><strong>Price:</strong> ${snapshot.price:.2f} '
            if snapshot.change_percent is not None:
                html += f'<span class="{price_class}">({arrow}{snapshot.change_percent:.2f}%)</span>'
            html += '</p>'

            if snapshot.market_cap:
                cap = snapshot.market_cap
                if cap >= 1_000_000_000_000:
                    cap_str = f"${cap / 1_000_000_000_000:.2f}T"
                elif cap >= 1_000_000_000:
                    cap_str = f"${cap / 1_000_000_000:.2f}B"
                else:
                    cap_str = f"${cap / 1_000_000:.2f}M"
                html += f'<p><strong>Market Cap:</strong> {cap_str}</p>'

        # Earnings Call Transcripts
        if transcripts:
            html += f'<p><strong>Earnings Transcripts:</strong> {len(transcripts)} recent</p>'
            html += '<ul>'
            for transcript in transcripts[:2]:  # Top 2 transcripts
                html += f'<li><strong>{transcript.quarter}</strong>'
                if transcript.content_summary:
                    summary_preview = transcript.content_summary[:200]
                    if len(transcript.content_summary) > 200:
                        summary_preview += "..."
                    html += f'<br><em style="font-size: 0.9em; color: #666;">{summary_preview}</em>'
                html += '</li>'
            html += '</ul>'

        # SEC Filings
        if filings:
            html += f'<p><strong>SEC Filings:</strong> {len(filings)} recent filing(s)</p>'
            html += '<ul>'
            for filing in filings[:3]:  # Top 3 filings
                filed_date = filing.filed_at.strftime("%Y-%m-%d") if filing.filed_at else ""
                html += f'<li><a href="{filing.filing_url}">{filing.form_type}</a> ({filed_date})'
                if filing.content_summary:
                    summary_preview = filing.content_summary[:150]
                    if len(filing.content_summary) > 150:
                        summary_preview += "..."
                    html += f'<br><em style="font-size: 0.9em; color: #666;">{summary_preview}</em>'
                html += '</li>'
            html += '</ul>'

        # News
        html += f'<p><strong>News:</strong> {len(articles)} article(s)</p>'
        if articles:
            html += '<ul>'
            for article in articles[:3]:  # Top 3 articles
                html += f'<li><a href="{article.url}">{article.title}</a>'
                if article.source:
                    html += f' <em>({article.source})</em>'
                html += '</li>'
            html += '</ul>'

        html += '</div>'
        return html

    def _format_hyperscaler_section(self, announcements: list) -> str:
        """Format hyperscaler announcements as an HTML section."""
        if not announcements:
            return ""

        html = '''
        <h2>Hyperscaler Activity</h2>
        <p style="color: #666; margin-bottom: 15px;">Recent data center expansion announcements from major cloud providers</p>
        '''

        # Group by hyperscaler
        by_hyperscaler = {}
        for ann in announcements:
            if ann.hyperscaler not in by_hyperscaler:
                by_hyperscaler[ann.hyperscaler] = []
            by_hyperscaler[ann.hyperscaler].append(ann)

        for hyperscaler, anns in by_hyperscaler.items():
            html += f'''
            <div style="background: #f0f9ff; padding: 15px; margin: 10px 0; border-radius: 8px; border-left: 4px solid #0ea5e9;">
                <h3 style="margin: 0 0 10px 0; color: #0369a1;">{hyperscaler}</h3>
                <ul style="margin: 0; padding-left: 20px;">
            '''
            for ann in anns[:3]:  # Limit to 3 per hyperscaler
                html += f'<li><a href="{ann.url}">{ann.title}</a>'
                if ann.content_summary:
                    summary_preview = ann.content_summary[:200]
                    if len(ann.content_summary) > 200:
                        summary_preview += "..."
                    html += f'<br><em style="font-size: 0.9em; color: #666;">{summary_preview}</em>'
                html += '</li>'
            html += '''
                </ul>
            </div>
            '''

        return html

    def _format_pe_section(self, announcements: list) -> str:
        """Format PE data center announcements as an HTML section."""
        if not announcements:
            return ""

        html = '''
        <h2>Private Equity Data Center Activity</h2>
        <p style="color: #666; margin-bottom: 15px;">Recent PE firm data center investments, acquisitions, and expansions</p>
        '''

        # Group by PE firm
        by_pe_firm = {}
        for ann in announcements:
            if ann.pe_firm not in by_pe_firm:
                by_pe_firm[ann.pe_firm] = []
            by_pe_firm[ann.pe_firm].append(ann)

        for pe_firm, anns in by_pe_firm.items():
            html += f'''
            <div style="background: #fdf4ff; padding: 15px; margin: 10px 0; border-radius: 8px; border-left: 4px solid #a855f7;">
                <h3 style="margin: 0 0 10px 0; color: #7c3aed;">{pe_firm}</h3>
                <ul style="margin: 0; padding-left: 20px;">
            '''
            for ann in anns[:3]:  # Limit to 3 per PE firm
                html += f'<li><a href="{ann.url}">{ann.title}</a>'
                if ann.content_summary:
                    summary_preview = ann.content_summary[:200]
                    if len(ann.content_summary) > 200:
                        summary_preview += "..."
                    html += f'<br><em style="font-size: 0.9em; color: #666;">{summary_preview}</em>'
                html += '</li>'
            html += '''
                </ul>
            </div>
            '''

        return html

    def _format_events_matrix(self, events_by_company: dict) -> str:
        """Format upcoming events as an HTML table, sorted by date (earliest first)."""
        html = '''
        <h2>Upcoming Events</h2>
        <table style="width: 100%; border-collapse: collapse; margin-top: 15px;">
            <thead>
                <tr style="background-color: #1a365d; color: white;">
                    <th style="padding: 12px; text-align: left; border: 1px solid #e2e8f0;">Company</th>
                    <th style="padding: 12px; text-align: left; border: 1px solid #e2e8f0;">Date</th>
                    <th style="padding: 12px; text-align: left; border: 1px solid #e2e8f0;">Description</th>
                </tr>
            </thead>
            <tbody>
        '''

        # Sort events by date (earliest first), with None dates at the end
        from datetime import date as date_type
        max_date = date_type(9999, 12, 31)  # Use far future date for sorting None values

        sorted_events = sorted(
            events_by_company.items(),
            key=lambda x: x[1].event_date if (x[1] and x[1].event_date) else max_date
        )

        row_colors = ['#ffffff', '#f7fafc']
        row_idx = 0

        for company_name, event in sorted_events:
            bg_color = row_colors[row_idx % 2]
            row_idx += 1

            if event and event.event_date:
                date_str = event.event_date.strftime("%b %d, %Y")
                description = event.description
            else:
                date_str = "TBD"
                description = event.description if event else "No upcoming events"

            html += f'''
                <tr style="background-color: {bg_color};">
                    <td style="padding: 10px; border: 1px solid #e2e8f0; font-weight: 600;">{company_name}</td>
                    <td style="padding: 10px; border: 1px solid #e2e8f0;">{date_str}</td>
                    <td style="padding: 10px; border: 1px solid #e2e8f0;">{description}</td>
                </tr>
            '''

        html += '''
            </tbody>
        </table>
        '''

        return html

    def _markdown_to_html(self, markdown_text: str) -> str:
        """Convert basic markdown to HTML."""
        import re

        html = markdown_text

        # Headers
        html = re.sub(r'^#### (.+)$', r'<h4>\1</h4>', html, flags=re.MULTILINE)
        html = re.sub(r'^### (.+)$', r'<h3>\1</h3>', html, flags=re.MULTILINE)
        html = re.sub(r'^## (.+)$', r'<h2>\1</h2>', html, flags=re.MULTILINE)
        html = re.sub(r'^# (.+)$', r'<h1>\1</h1>', html, flags=re.MULTILINE)

        # Bold
        html = re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', html)

        # Italic
        html = re.sub(r'\*(.+?)\*', r'<em>\1</em>', html)

        # Lists
        lines = html.split('\n')
        in_list = False
        result = []
        for line in lines:
            if line.strip().startswith('- '):
                if not in_list:
                    result.append('<ul>')
                    in_list = True
                result.append(f'<li>{line.strip()[2:]}</li>')
            else:
                if in_list:
                    result.append('</ul>')
                    in_list = False
                result.append(line)
        if in_list:
            result.append('</ul>')
        html = '\n'.join(result)

        # Paragraphs
        html = re.sub(r'\n\n', '</p><p>', html)
        html = f'<p>{html}</p>'

        return html

    def send_daily_digest(
        self,
        summary_text: str,
        companies: list[Company],
        articles_by_company: dict[str, list[NewsArticle]],
        snapshots_by_company: dict[str, Optional[FinancialSnapshot]],
        filings_by_company: dict[str, list] = None,
        transcripts_by_company: dict[str, list] = None,
        hyperscaler_announcements: list = None,
        events_by_company: dict = None,
        target_date: date = None,
        is_weekly: bool = False,
        week_start_date: date = None,
        pe_announcements: list = None
    ) -> bool:
        """Send the daily or weekly digest email.

        Args:
            summary_text: AI-generated summary.
            companies: List of companies.
            articles_by_company: Dict mapping company name to articles.
            snapshots_by_company: Dict mapping company name to financial snapshot.
            filings_by_company: Dict mapping company name to SEC filings.
            transcripts_by_company: Dict mapping company name to earnings transcripts.
            hyperscaler_announcements: List of hyperscaler announcements.
            events_by_company: Dict mapping company name to upcoming events.
            target_date: The date for this digest.
            is_weekly: If True, format as weekly summary.
            week_start_date: Start date of the week (for weekly summaries).
            pe_announcements: List of PE data center announcements.

        Returns:
            True if email was sent successfully.
        """
        if filings_by_company is None:
            filings_by_company = {}
        if transcripts_by_company is None:
            transcripts_by_company = {}
        if hyperscaler_announcements is None:
            hyperscaler_announcements = []
        if events_by_company is None:
            events_by_company = {}
        if pe_announcements is None:
            pe_announcements = []

        template = self._load_template()

        # Format summary
        summary_html = self._markdown_to_html(summary_text)

        # Format hyperscaler section
        hyperscaler_section = self._format_hyperscaler_section(hyperscaler_announcements)

        # Format PE section
        pe_section = self._format_pe_section(pe_announcements)

        # Format company details
        company_details = ""
        for company in companies:
            articles = articles_by_company.get(company.name, [])
            snapshot = snapshots_by_company.get(company.name)
            filings = filings_by_company.get(company.name, [])
            transcripts = transcripts_by_company.get(company.name, [])
            company_details += self._format_company_html(
                company, articles, snapshot, filings, transcripts
            )

        # Format events matrix
        events_matrix = ""
        if events_by_company:
            events_matrix = self._format_events_matrix(events_by_company)

        # Fill template based on daily vs weekly format
        if is_weekly:
            date_range_str = f"{week_start_date.strftime('%B %d')} - {target_date.strftime('%B %d, %Y')}"
            html_content = template.replace("{{DATE}}", f"Week of {date_range_str}")
            html_content = html_content.replace("{{SUMMARY}}", f"<h2>Week in Review</h2>{summary_html}")
            html_content = html_content.replace("Daily Company Briefing", "Weekly Company Summary")
            subject = f"Weekly Company Summary - {date_range_str}"
        else:
            html_content = template.replace("{{DATE}}", target_date.strftime("%B %d, %Y"))
            html_content = html_content.replace("{{SUMMARY}}", f"<h2>Executive Summary</h2>{summary_html}")
            subject = f"Daily Company Briefing - {target_date.strftime('%B %d, %Y')}"

        html_content = html_content.replace(
            "{{COMPANY_DETAILS}}",
            f"{hyperscaler_section}{pe_section}<h2>Company Details</h2>{company_details}{events_matrix}"
        )

        try:
            response = resend.Emails.send({
                "from": self.from_email,
                "to": [self.to_email],
                "subject": subject,
                "html": html_content
            })
            print(f"Email sent successfully. ID: {response['id']}")
            return True
        except Exception as e:
            print(f"Error sending email: {e}")
            return False
