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
        filings: list = None
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

    def _format_events_matrix(self, events_by_company: dict) -> str:
        """Format upcoming events as an HTML table."""
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

        row_colors = ['#ffffff', '#f7fafc']
        row_idx = 0

        for company_name, event in events_by_company.items():
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
        events_by_company: dict = None,
        target_date: date = None
    ) -> bool:
        """Send the daily digest email.

        Args:
            summary_text: AI-generated summary.
            companies: List of companies.
            articles_by_company: Dict mapping company name to articles.
            snapshots_by_company: Dict mapping company name to financial snapshot.
            filings_by_company: Dict mapping company name to SEC filings.
            events_by_company: Dict mapping company name to upcoming events.
            target_date: The date for this digest.

        Returns:
            True if email was sent successfully.
        """
        if filings_by_company is None:
            filings_by_company = {}
        if events_by_company is None:
            events_by_company = {}

        template = self._load_template()

        # Format summary
        summary_html = self._markdown_to_html(summary_text)

        # Format company details
        company_details = ""
        for company in companies:
            articles = articles_by_company.get(company.name, [])
            snapshot = snapshots_by_company.get(company.name)
            filings = filings_by_company.get(company.name, [])
            company_details += self._format_company_html(company, articles, snapshot, filings)

        # Format events matrix
        events_matrix = ""
        if events_by_company:
            events_matrix = self._format_events_matrix(events_by_company)

        # Fill template
        html_content = template.replace("{{DATE}}", target_date.strftime("%B %d, %Y"))
        html_content = html_content.replace("{{SUMMARY}}", f"<h2>Executive Summary</h2>{summary_html}")
        html_content = html_content.replace("{{COMPANY_DETAILS}}", f"<h2>Company Details</h2>{company_details}{events_matrix}")

        try:
            response = resend.Emails.send({
                "from": self.from_email,
                "to": [self.to_email],
                "subject": f"Daily Company Briefing - {target_date.strftime('%B %d, %Y')}",
                "html": html_content
            })
            print(f"Email sent successfully. ID: {response['id']}")
            return True
        except Exception as e:
            print(f"Error sending email: {e}")
            return False
