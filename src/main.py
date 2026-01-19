#!/usr/bin/env python3
"""Main orchestration script for the Company Tracker."""

import sys
from datetime import date

from .config import COMPANIES, config
from .email_sender import EmailSender
from .finance_fetcher import FinanceFetcher
from .news_fetcher import NewsFetcher
from .storage import DailySummary, Storage
from .summarizer import Summarizer


def run_tracker(dry_run: bool = False) -> bool:
    """Run the daily tracking workflow.

    Args:
        dry_run: If True, skip sending email and just print results.

    Returns:
        True if successful, False otherwise.
    """
    print("=" * 60)
    print("Company News & Financial Tracker")
    print(f"Date: {date.today().strftime('%B %d, %Y')}")
    print("=" * 60)

    # Validate configuration
    missing = config.validate()
    if missing:
        print(f"\nError: Missing required configuration: {', '.join(missing)}")
        print("Please set these environment variables or add them to .env file")
        return False

    # Initialize components
    print("\n[1/6] Initializing...")
    storage = Storage()
    news_fetcher = NewsFetcher()
    finance_fetcher = FinanceFetcher()
    summarizer = Summarizer()

    # Sync companies to database
    print("\n[2/6] Syncing companies to database...")
    company_ids = storage.sync_companies(COMPANIES)
    print(f"  Tracking {len(company_ids)} companies")

    # Fetch news
    print("\n[3/6] Fetching news articles...")
    articles_by_company = news_fetcher.fetch_all_companies(
        COMPANIES,
        company_ids,
        hours_back=24,
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
    print("\n[4/6] Fetching financial data...")
    snapshots_by_company = finance_fetcher.fetch_all_companies(
        COMPANIES,
        company_ids
    )

    # Save financial snapshots
    for company_name, snapshot in snapshots_by_company.items():
        if snapshot:
            storage.save_financial_snapshot(snapshot)
    print("  Saved financial snapshots to database")

    # Generate AI summary
    print("\n[5/6] Generating AI summary...")
    summary_text = summarizer.generate_summary(
        COMPANIES,
        articles_by_company,
        snapshots_by_company,
        date.today()
    )
    print("  Summary generated")

    # Save summary to database
    summary = DailySummary(
        id=None,
        date=date.today(),
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
        print("\n[6/6] Dry run mode - skipping email")
    else:
        print("\n[6/6] Sending email digest...")
        email_sender = EmailSender()
        if email_sender.send_daily_digest(
            summary_text,
            COMPANIES,
            articles_by_company,
            snapshots_by_company,
            date.today()
        ):
            storage.mark_summary_email_sent(date.today())
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
