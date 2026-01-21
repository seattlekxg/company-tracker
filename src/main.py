#!/usr/bin/env python3
"""Main orchestration script for the Company Tracker."""

import sys
from datetime import datetime
from zoneinfo import ZoneInfo

from .config import COMPANIES, config

# Houston timezone (Central Time)
HOUSTON_TZ = ZoneInfo("America/Chicago")


def get_houston_date():
    """Get the current date in Houston timezone."""
    return datetime.now(HOUSTON_TZ).date()
from .email_sender import EmailSender
from .events_fetcher import EventsFetcher
from .finance_fetcher import FinanceFetcher
from .hyperscaler_fetcher import HyperscalerFetcher
from .news_fetcher import NewsFetcher
from .sec_fetcher import SECFetcher
from .storage import DailySummary, Storage
from .summarizer import Summarizer
from .transcript_fetcher import TranscriptFetcher


def run_tracker(dry_run: bool = False) -> bool:
    """Run the daily tracking workflow.

    Args:
        dry_run: If True, skip sending email and just print results.

    Returns:
        True if successful, False otherwise.
    """
    print("=" * 60)
    print("Company News & Financial Tracker")
    print(f"Date: {get_houston_date().strftime('%B %d, %Y')}")
    print("=" * 60)

    # Validate configuration
    missing = config.validate()
    if missing:
        print(f"\nError: Missing required configuration: {', '.join(missing)}")
        print("Please set these environment variables or add them to .env file")
        return False

    # Initialize components
    print("\n[1/10] Initializing...")
    storage = Storage()
    news_fetcher = NewsFetcher()
    finance_fetcher = FinanceFetcher()
    sec_fetcher = SECFetcher()
    events_fetcher = EventsFetcher()
    transcript_fetcher = TranscriptFetcher()
    hyperscaler_fetcher = HyperscalerFetcher()
    summarizer = Summarizer()

    # Sync companies to database
    print("\n[2/10] Syncing companies to database...")
    company_ids = storage.sync_companies(COMPANIES)
    print(f"  Tracking {len(company_ids)} companies")

    # Fetch news
    # Note: NewsAPI free tier only returns articles >24 hours old
    # Using 72 hours to ensure we capture recent news
    print("\n[3/10] Fetching news articles...")
    articles_by_company = news_fetcher.fetch_all_companies(
        COMPANIES,
        company_ids,
        hours_back=72,
        rate_limit_delay=1.0
    )

    # Save articles to database
    total_new = 0
    for company_name, articles in articles_by_company.items():
        for article in articles:
            if storage.save_article(article):
                total_new += 1
    print(f"  Saved {total_new} new articles to database")

    # Fetch financial data
    print("\n[4/10] Fetching financial data...")
    snapshots_by_company = finance_fetcher.fetch_all_companies(
        COMPANIES,
        company_ids
    )

    # Save financial snapshots
    for company_name, snapshot in snapshots_by_company.items():
        if snapshot:
            storage.save_financial_snapshot(snapshot)
    print("  Saved financial snapshots to database")

    # Fetch SEC filings
    print("\n[5/10] Fetching SEC filings...")
    filings_by_company = sec_fetcher.fetch_all_companies(
        COMPANIES,
        company_ids,
        days_back=7,  # Look back 7 days for SEC filings
        rate_limit_delay=0.5
    )

    # Analyze SEC filings and save to database
    total_filings = 0
    for company_name, filings in filings_by_company.items():
        for filing in filings:
            # Fetch filing content and analyze with AI
            print(f"  Analyzing {filing.form_type} for {company_name}...")
            content = sec_fetcher.fetch_filing_content(filing, max_chars=50000)
            if content:
                filing.content_summary = summarizer.analyze_sec_filing(filing, content)
            else:
                filing.content_summary = "Unable to fetch filing content"

            # Save to database
            if storage.save_sec_filing(filing):
                total_filings += 1

    print(f"  Processed {total_filings} new SEC filings")

    # Fetch earnings call transcripts
    print("\n[6/10] Fetching earnings call transcripts...")
    transcripts_by_company = transcript_fetcher.fetch_all_companies(
        COMPANIES,
        company_ids,
        quarters_back=2,
        rate_limit_delay=0.5
    )

    # Analyze transcripts and save to database
    total_transcripts = 0
    for company_name, transcripts in transcripts_by_company.items():
        for transcript in transcripts:
            if transcript.transcript_text:
                print(f"  Analyzing {transcript.quarter} transcript for {company_name}...")
                transcript.content_summary = summarizer.analyze_earnings_transcript(
                    transcript, transcript.transcript_text
                )
            else:
                transcript.content_summary = "No transcript content available"

            # Save to database
            if storage.save_earnings_transcript(transcript):
                total_transcripts += 1

    print(f"  Processed {total_transcripts} new earnings transcripts")

    # Fetch hyperscaler announcements
    print("\n[7/10] Fetching hyperscaler announcements...")
    hyperscaler_announcements = hyperscaler_fetcher.fetch_announcements(hours_back=72)

    # Analyze hyperscaler announcements and save to database
    total_announcements = 0
    for announcement in hyperscaler_announcements:
        print(f"  Analyzing {announcement.hyperscaler} announcement...")
        announcement.content_summary = summarizer.analyze_hyperscaler_announcement(
            announcement
        )

        # Save to database
        if storage.save_hyperscaler_announcement(announcement):
            total_announcements += 1

    print(f"  Processed {total_announcements} new hyperscaler announcements")

    # Fetch upcoming events
    print("\n[8/10] Fetching upcoming events...")
    events_by_company = events_fetcher.fetch_all_companies(COMPANIES)

    # Generate AI summary
    print("\n[9/10] Generating AI summary...")
    summary_text = summarizer.generate_summary(
        COMPANIES,
        articles_by_company,
        snapshots_by_company,
        filings_by_company,
        transcripts_by_company,
        hyperscaler_announcements,
        get_houston_date()
    )
    print("  Summary generated")

    # Save summary to database
    summary = DailySummary(
        id=None,
        date=get_houston_date(),
        summary_text=summary_text,
        email_sent=False
    )
    storage.save_daily_summary(summary)

    # Print summary preview
    print("\n" + "-" * 40)
    print("SUMMARY PREVIEW:")
    print("-" * 40)
    print(summary_text[:500] + "..." if len(summary_text) > 500 else summary_text)
    print("-" * 40)

    # Send email
    if dry_run:
        print("\n[10/10] Dry run mode - skipping email")
    else:
        print("\n[10/10] Sending email digest...")
        email_sender = EmailSender()
        if email_sender.send_daily_digest(
            summary_text,
            COMPANIES,
            articles_by_company,
            snapshots_by_company,
            filings_by_company,
            transcripts_by_company,
            hyperscaler_announcements,
            events_by_company,
            get_houston_date()
        ):
            storage.mark_summary_email_sent(get_houston_date())
            print("  Email sent successfully!")
        else:
            print("  Failed to send email")
            return False

    print("\n" + "=" * 60)
    print("Tracking complete!")
    print("=" * 60)

    return True


def main():
    """Main entry point."""
    # Check for dry-run flag
    dry_run = "--dry-run" in sys.argv or "-n" in sys.argv

    if dry_run:
        print("Running in dry-run mode (no email will be sent)\n")

    success = run_tracker(dry_run=dry_run)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
