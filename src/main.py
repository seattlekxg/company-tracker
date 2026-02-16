#!/usr/bin/env python3
"""Main orchestration script for the Company Tracker."""

import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from .config import COMPANIES, config

# Houston timezone (Central Time)
HOUSTON_TZ = ZoneInfo("America/Chicago")


def get_houston_date():
    """Get the current date in Houston timezone."""
    return datetime.now(HOUSTON_TZ).date()


def is_friday():
    """Check if today is Friday (weekday 4)."""
    return get_houston_date().weekday() == 4


def get_week_start_date():
    """Get the start date for weekly summary (6 days ago)."""
    return get_houston_date() - timedelta(days=6)
from .email_sender import EmailSender
from .events_fetcher import EventsFetcher
from .finance_fetcher import FinanceFetcher
from .gnews_fetcher import GNewsFetcher
from .hyperscaler_fetcher import HyperscalerFetcher
from .news_fetcher import NewsFetcher
from .pe_datacenter_fetcher import PEDatacenterFetcher
from .rss_fetcher import RSSFetcher
from .sec_fetcher import SECFetcher
from .storage import DailySummary, PEDatacenterAnnouncement, Storage
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
    today = get_houston_date()
    is_weekly_edition = is_friday()
    if is_weekly_edition:
        week_start = get_week_start_date()
        print(f"WEEKLY EDITION: {week_start.strftime('%B %d')} - {today.strftime('%B %d, %Y')}")
    else:
        print(f"Date: {today.strftime('%B %d, %Y')}")
    print("=" * 60)

    # Validate configuration
    missing = config.validate()
    if missing:
        print(f"\nError: Missing required configuration: {', '.join(missing)}")
        print("Please set these environment variables or add them to .env file")
        return False

    # Initialize components
    print("\n[1/14] Initializing...")
    storage = Storage()
    news_fetcher = NewsFetcher()
    finance_fetcher = FinanceFetcher()
    sec_fetcher = SECFetcher()
    events_fetcher = EventsFetcher()
    transcript_fetcher = TranscriptFetcher()
    hyperscaler_fetcher = HyperscalerFetcher()
    pe_datacenter_fetcher = PEDatacenterFetcher()
    summarizer = Summarizer()
    rss_fetcher = RSSFetcher(translator=summarizer)  # Pass summarizer for translation

    # Initialize GNews fetcher if API key is configured
    gnews_fetcher = None
    if config.gnews_api_key:
        gnews_fetcher = GNewsFetcher(translator=summarizer)
        print("  GNews API configured for international news")

    # Sync companies to database
    print("\n[2/14] Syncing companies to database...")
    company_ids = storage.sync_companies(COMPANIES)
    print(f"  Tracking {len(company_ids)} companies")

    # Fetch news
    # Note: NewsAPI free tier only returns articles >24 hours old
    # Using 72 hours to ensure we capture recent news
    print("\n[3/14] Fetching news articles...")
    articles_by_company = news_fetcher.fetch_all_companies(
        COMPANIES,
        company_ids,
        hours_back=72,
        rate_limit_delay=1.0
    )

    # Filter articles for relevance using AI
    print("  Filtering articles for data center relevance...")
    total_fetched = sum(len(a) for a in articles_by_company.values())
    filtered_articles_by_company = {}
    for company_name, articles in articles_by_company.items():
        if articles:
            filtered = summarizer.filter_relevant_articles(company_name, articles)
            filtered_articles_by_company[company_name] = filtered
            if len(filtered) < len(articles):
                print(f"    {company_name}: {len(filtered)}/{len(articles)} articles relevant")
        else:
            filtered_articles_by_company[company_name] = []

    total_relevant = sum(len(a) for a in filtered_articles_by_company.values())
    print(f"  Filtered to {total_relevant}/{total_fetched} relevant articles")

    # Fetch news from RSS feeds (company press releases)
    print("\n[4/14] Fetching RSS feeds (press releases)...")
    rss_articles_by_company = rss_fetcher.fetch_all_companies(
        COMPANIES,
        company_ids,
        hours_back=72,
        rate_limit_delay=0.5
    )

    # Filter RSS articles for relevance
    total_rss_fetched = sum(len(a) for a in rss_articles_by_company.values())
    if total_rss_fetched > 0:
        print("  Filtering RSS articles for relevance...")
        for company_name, articles in rss_articles_by_company.items():
            if articles:
                filtered = summarizer.filter_relevant_articles(company_name, articles)
                rss_articles_by_company[company_name] = filtered
                if len(filtered) < len(articles):
                    print(f"    {company_name}: {len(filtered)}/{len(articles)} RSS articles relevant")

    total_rss_relevant = sum(len(a) for a in rss_articles_by_company.values())
    print(f"  Found {total_rss_relevant} relevant articles from RSS feeds")

    # Merge NewsAPI and RSS articles
    for company_name in filtered_articles_by_company:
        rss_articles = rss_articles_by_company.get(company_name, [])
        if rss_articles:
            filtered_articles_by_company[company_name].extend(rss_articles)

    # Fetch news from GNews API (international/multi-language)
    if gnews_fetcher:
        print("\n[5/14] Fetching GNews (international news)...")
        gnews_articles_by_company = gnews_fetcher.fetch_all_companies(
            COMPANIES,
            company_ids,
            hours_back=72,
            rate_limit_delay=1.0  # GNews has rate limits
        )

        # Filter GNews articles for relevance
        total_gnews_fetched = sum(len(a) for a in gnews_articles_by_company.values())
        if total_gnews_fetched > 0:
            print("  Filtering GNews articles for relevance...")
            for company_name, articles in gnews_articles_by_company.items():
                if articles:
                    filtered = summarizer.filter_relevant_articles(company_name, articles)
                    gnews_articles_by_company[company_name] = filtered
                    if len(filtered) < len(articles):
                        print(f"    {company_name}: {len(filtered)}/{len(articles)} GNews articles relevant")

        total_gnews_relevant = sum(len(a) for a in gnews_articles_by_company.values())
        print(f"  Found {total_gnews_relevant} relevant articles from GNews")

        # Merge GNews articles
        for company_name in filtered_articles_by_company:
            gnews_articles = gnews_articles_by_company.get(company_name, [])
            if gnews_articles:
                filtered_articles_by_company[company_name].extend(gnews_articles)
    else:
        print("\n[5/14] Skipping GNews (no API key configured)")

    # Save all filtered articles to database
    total_new = 0
    for company_name, articles in filtered_articles_by_company.items():
        for article in articles:
            if storage.save_article(article):
                total_new += 1
    print(f"  Saved {total_new} new articles to database")

    # Fetch financial data
    print("\n[6/14] Fetching financial data...")
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
    print("\n[7/14] Fetching SEC filings...")
    filings_by_company = sec_fetcher.fetch_all_companies(
        COMPANIES,
        company_ids,
        days_back=30,  # Look back 30 days for SEC filings (quarterly reports are infrequent)
        rate_limit_delay=0.2  # SEC allows 10 req/sec
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
    print("\n[8/14] Fetching earnings call transcripts...")
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
    print("\n[9/14] Fetching hyperscaler announcements...")
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

    # Fetch PE data center announcements
    print("\n[10/14] Fetching PE data center announcements...")
    pe_announcements = pe_datacenter_fetcher.fetch_announcements(hours_back=72)

    # Analyze PE announcements and save to database
    total_pe_announcements = 0
    for announcement in pe_announcements:
        print(f"  Analyzing {announcement.pe_firm} announcement...")
        # Convert to storage dataclass
        pe_storage_announcement = PEDatacenterAnnouncement(
            id=None,
            pe_firm=announcement.pe_firm,
            title=announcement.title,
            description=announcement.description,
            url=announcement.url,
            published_at=announcement.published_at,
            content_summary=summarizer.analyze_pe_announcement(announcement)
        )

        # Save to database
        if storage.save_pe_announcement(pe_storage_announcement):
            total_pe_announcements += 1

    print(f"  Processed {total_pe_announcements} new PE data center announcements")

    # Fetch upcoming events
    print("\n[11/14] Fetching upcoming events...")
    events_by_company = events_fetcher.fetch_all_companies(COMPANIES)

    # Query items for email
    # For weekly edition (Friday): get all items from the past 7 days
    # For daily edition: get only unsent items
    if is_weekly_edition:
        print("\n[12/14] Querying week's data for weekly summary...")
        week_start = get_week_start_date()
        email_articles_by_company = {}
        email_filings_by_company = {}
        email_transcripts_by_company = {}

        for company in COMPANIES:
            company_db = storage.get_company_by_name(company.name)
            if company_db:
                company_id = company_db["id"]
                email_articles_by_company[company.name] = storage.get_articles_in_range(
                    company_id, week_start, today
                )
                email_filings_by_company[company.name] = storage.get_sec_filings_in_range(
                    company_id, week_start, today
                )
                email_transcripts_by_company[company.name] = storage.get_transcripts_in_range(
                    company_id, week_start, today
                )

        email_hyperscaler = storage.get_hyperscaler_announcements_in_range(week_start, today)
        email_pe_announcements = storage.get_pe_announcements_in_range(week_start, today)

        # Count items for weekly summary
        total_articles = sum(len(a) for a in email_articles_by_company.values())
        total_filings = sum(len(f) for f in email_filings_by_company.values())
        total_transcripts = sum(len(t) for t in email_transcripts_by_company.values())
        total_hyperscaler = len(email_hyperscaler)
        total_pe = len(email_pe_announcements)
        print(f"  Found {total_articles} articles, {total_filings} filings, "
              f"{total_transcripts} transcripts, {total_hyperscaler} hyperscaler, "
              f"{total_pe} PE announcements this week")
    else:
        print("\n[12/14] Querying unsent items for email...")
        email_articles_by_company = {}
        email_filings_by_company = {}
        email_transcripts_by_company = {}

        for company in COMPANIES:
            company_db = storage.get_company_by_name(company.name)
            if company_db:
                company_id = company_db["id"]
                email_articles_by_company[company.name] = storage.get_unsent_articles(company_id)
                email_filings_by_company[company.name] = storage.get_unsent_sec_filings(company_id)
                email_transcripts_by_company[company.name] = storage.get_unsent_transcripts(company_id)

        email_hyperscaler = storage.get_unsent_hyperscaler_announcements()
        email_pe_announcements = storage.get_unsent_pe_announcements()

        # Count unsent items
        total_articles = sum(len(a) for a in email_articles_by_company.values())
        total_filings = sum(len(f) for f in email_filings_by_company.values())
        total_transcripts = sum(len(t) for t in email_transcripts_by_company.values())
        total_hyperscaler = len(email_hyperscaler)
        total_pe = len(email_pe_announcements)
        print(f"  Found {total_articles} unsent articles, {total_filings} unsent filings, "
              f"{total_transcripts} unsent transcripts, {total_hyperscaler} unsent hyperscaler, "
              f"{total_pe} unsent PE announcements")

    # Generate AI summary
    if is_weekly_edition:
        print("\n[13/14] Generating weekly AI summary...")
        summary_text = summarizer.generate_summary(
            COMPANIES,
            email_articles_by_company,
            snapshots_by_company,
            email_filings_by_company,
            email_transcripts_by_company,
            email_hyperscaler,
            today,
            is_weekly=True,
            week_start_date=get_week_start_date(),
            pe_announcements=email_pe_announcements
        )
        print("  Weekly summary generated")
    else:
        print("\n[13/14] Generating daily AI summary...")
        summary_text = summarizer.generate_summary(
            COMPANIES,
            email_articles_by_company,
            snapshots_by_company,
            email_filings_by_company,
            email_transcripts_by_company,
            email_hyperscaler,
            today,
            pe_announcements=email_pe_announcements
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
        print("\n[14/14] Dry run mode - skipping email")
    else:
        email_type = "weekly" if is_weekly_edition else "daily"
        print(f"\n[14/14] Sending {email_type} email digest...")
        email_sender = EmailSender()
        if email_sender.send_daily_digest(
            summary_text,
            COMPANIES,
            email_articles_by_company,
            snapshots_by_company,
            email_filings_by_company,
            email_transcripts_by_company,
            email_hyperscaler,
            events_by_company,
            today,
            is_weekly=is_weekly_edition,
            week_start_date=get_week_start_date() if is_weekly_edition else None,
            pe_announcements=email_pe_announcements
        ):
            storage.mark_summary_email_sent(today)

            # Mark all items as emailed (for deduplication in future daily emails)
            for company_name, articles in email_articles_by_company.items():
                article_ids = [a.id for a in articles if a.id]
                storage.mark_articles_emailed(article_ids)

            for company_name, filings in email_filings_by_company.items():
                filing_ids = [f.id for f in filings if f.id]
                storage.mark_sec_filings_emailed(filing_ids)

            for company_name, transcripts in email_transcripts_by_company.items():
                transcript_ids = [t.id for t in transcripts if t.id]
                storage.mark_transcripts_emailed(transcript_ids)

            announcement_ids = [a.id for a in email_hyperscaler if a.id]
            storage.mark_hyperscaler_announcements_emailed(announcement_ids)

            pe_announcement_ids = [a.id for a in email_pe_announcements if a.id]
            storage.mark_pe_announcements_emailed(pe_announcement_ids)

            print(f"  {email_type.capitalize()} email sent successfully!")
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
