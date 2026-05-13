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
from .ppt_generator import SeasonalPPTGenerator
from .rss_fetcher import RSSFetcher
from .sec_fetcher import SECFetcher
from .storage import DailySummary, PEDatacenterAnnouncement, Storage
from .summarizer import Summarizer
from .transcript_fetcher import TranscriptFetcher


def _get_public_company_names() -> list[str]:
    """Get names of all public companies (those with tickers)."""
    return [c.name for c in COMPANIES if c.ticker]


def _check_season_completeness(storage, today):
    """Check if any active earnings season should be marked complete."""
    public_companies = _get_public_company_names()

    # Check all seasons that aren't yet complete
    conn = storage._get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT quarter FROM earnings_seasons WHERE season_complete = FALSE"
    )
    active_quarters = [row["quarter"] for row in cursor.fetchall()]
    conn.close()

    for quarter in active_quarters:
        season = storage.get_or_create_earnings_season(quarter)

        if not season.companies_reported:
            continue

        # Check 1: All public companies have reported
        reported_set = set(season.companies_reported)
        public_set = set(public_companies)
        all_reported = public_set.issubset(reported_set)

        # Check 2: 10+ days since last report (handles companies whose
        # transcripts aren't available via the API)
        days_since_last = None
        if season.last_report_date:
            days_since_last = (today - season.last_report_date).days

        if all_reported:
            print(f"  Earnings season {quarter}: ALL public companies have reported")
            storage.mark_season_complete(quarter)
        elif days_since_last is not None and days_since_last >= 10:
            reported_count = len(reported_set & public_set)
            print(f"  Earnings season {quarter}: {reported_count}/{len(public_set)} "
                  f"companies reported, {days_since_last} days since last report - marking complete")
            storage.mark_season_complete(quarter)


def _generate_seasonal_ppt_summary(
    storage, summarizer, email_sender, season, today, dry_run
):
    """Generate and send the seasonal PPT summary for a complete earnings season."""
    quarter = season.quarter
    print(f"\n  === Generating {quarter} Earnings Season PPT Summary ===")

    # Gather all season data
    # 1. Get all transcripts for the quarter
    all_transcripts = storage.get_transcripts_for_quarter(quarter)
    transcripts_by_company = {}
    for t in all_transcripts:
        company_db = None
        # Look up company name from company_id
        for company in COMPANIES:
            db_entry = storage.get_company_by_name(company.name)
            if db_entry and db_entry["id"] == t.company_id:
                company_db = company
                break
        if company_db:
            if company_db.name not in transcripts_by_company:
                transcripts_by_company[company_db.name] = []
            transcripts_by_company[company_db.name].append(t)

    # 2. Get financial snapshots at season start and end
    snapshots_start = {}
    snapshots_end = {}
    market_data = []

    for company in COMPANIES:
        if not company.ticker:
            continue
        company_db = storage.get_company_by_name(company.name)
        if not company_db:
            continue

        start_snap = None
        end_snap = None
        if season.first_report_date:
            start_snap = storage.get_financial_snapshot_nearest(
                company_db["id"], season.first_report_date
            )
        if season.last_report_date:
            end_snap = storage.get_financial_snapshot_nearest(
                company_db["id"], season.last_report_date
            )

        snapshots_start[company.name] = start_snap
        snapshots_end[company.name] = end_snap

        # Build market data for the table
        start_price = start_snap.price if start_snap else None
        end_price = end_snap.price if end_snap else None
        change = None
        if start_price and end_price:
            change = ((end_price - start_price) / start_price) * 100

        market_data.append({
            "company": company.name,
            "ticker": company.ticker,
            "start_price": start_price,
            "end_price": end_price,
            "change_percent": change
        })

    # 3. Get news, SEC filings, hyperscaler, and PE data from the season date range
    articles_by_company = {}
    filings_by_company = {}

    if season.first_report_date and season.last_report_date:
        for company in COMPANIES:
            company_db = storage.get_company_by_name(company.name)
            if not company_db:
                continue
            articles_by_company[company.name] = storage.get_articles_in_range(
                company_db["id"], season.first_report_date, season.last_report_date
            )
            filings_by_company[company.name] = storage.get_sec_filings_in_range(
                company_db["id"], season.first_report_date, season.last_report_date
            )

        hyperscaler_announcements = storage.get_hyperscaler_announcements_in_range(
            season.first_report_date, season.last_report_date
        )
        pe_announcements = storage.get_pe_announcements_in_range(
            season.first_report_date, season.last_report_date
        )
    else:
        hyperscaler_announcements = []
        pe_announcements = []

    # 4. Generate AI seasonal summary
    print(f"  Generating AI seasonal summary for {quarter}...")
    seasonal_analysis = summarizer.generate_seasonal_summary(
        quarter=quarter,
        companies=COMPANIES,
        transcripts_by_company=transcripts_by_company,
        filings_by_company=filings_by_company,
        articles_by_company=articles_by_company,
        snapshots_start=snapshots_start,
        snapshots_end=snapshots_end,
        hyperscaler_announcements=hyperscaler_announcements,
        pe_announcements=pe_announcements
    )
    print("  AI seasonal summary generated")

    # 5. Build season_data for PPT generator
    season_data = {
        "first_report_date": season.first_report_date,
        "last_report_date": season.last_report_date,
        "executive_summary": seasonal_analysis.get("executive_summary", ""),
        "sector_themes": seasonal_analysis.get("sector_themes", ""),
        "company_highlights": seasonal_analysis.get("company_highlights", {}),
        "outlook": seasonal_analysis.get("outlook", ""),
        "market_data": market_data,
        "hyperscaler_summary": seasonal_analysis.get("hyperscaler_summary", ""),
        "pe_summary": seasonal_analysis.get("pe_summary", ""),
        "companies_reported": season.companies_reported,
    }

    # 6. Generate PPT
    import os
    output_dir = os.path.join(
        os.path.dirname(__file__), "..", "data"
    )
    safe_quarter = quarter.replace(" ", "_")
    pptx_filename = f"{safe_quarter}_earnings_summary.pptx"
    pptx_path = os.path.join(output_dir, pptx_filename)

    print(f"  Generating PowerPoint: {pptx_filename}...")
    ppt_gen = SeasonalPPTGenerator()
    ppt_gen.generate(quarter, season_data, pptx_path)
    print(f"  PowerPoint saved to {pptx_path}")

    # 7. Send email with attachment
    if dry_run:
        print(f"  Dry run - skipping seasonal summary email for {quarter}")
        print(f"  PPT file available at: {pptx_path}")
    else:
        print(f"  Sending {quarter} seasonal summary email...")
        if email_sender.send_seasonal_summary(
            quarter=quarter,
            pptx_path=pptx_path,
            companies_reported=season.companies_reported,
            first_date=season.first_report_date,
            last_date=season.last_report_date
        ):
            storage.mark_season_summary_sent(quarter)
            print(f"  {quarter} seasonal summary sent successfully!")
        else:
            print(f"  Failed to send {quarter} seasonal summary email")


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

    # Update earnings season tracking for any new transcripts
    print("  Updating earnings season tracking...")
    for company_name, transcripts in transcripts_by_company.items():
        for transcript in transcripts:
            if transcript.quarter and transcript.transcript_date:
                storage.update_earnings_season_report(
                    transcript.quarter,
                    company_name,
                    transcript.transcript_date
                )

    # Also check existing transcripts in DB for season tracking
    # This ensures season tracking catches up if transcripts were saved on previous runs
    for company in COMPANIES:
        if not company.ticker:
            continue
        company_db = storage.get_company_by_name(company.name)
        if not company_db:
            continue
        # Check recent transcripts (last 2 quarters worth)
        db_transcripts = storage.get_transcripts_in_range(
            company_db["id"],
            today - timedelta(days=180),
            today
        )
        for transcript in db_transcripts:
            if transcript.quarter and transcript.transcript_date:
                storage.update_earnings_season_report(
                    transcript.quarter,
                    company.name,
                    transcript.transcript_date
                )

    # Check if any earnings season is complete
    # A season is complete when all public companies have reported
    # OR when 10+ days have passed since the last report
    _check_season_completeness(storage, today)

    # Fetch hyperscaler announcements
    print("\n[9/14] Fetching hyperscaler announcements...")
    hyperscaler_announcements = hyperscaler_fetcher.fetch_announcements(hours_back=72)

    # Analyze hyperscaler announcements and save to database
    total_announcements = 0
    for announcement in hyperscaler_announcements:
        print(f"  Analyzing {announcement.hyperscaler} announcement...")
        analysis = summarizer.analyze_hyperscaler_announcement(announcement)
        announcement.content_summary = analysis["summary"]

        # Save to database
        if storage.save_hyperscaler_announcement(announcement):
            total_announcements += 1
            # Get the saved announcement's ID and store MW data
            # The announcement was just inserted, get its ID by URL lookup
            conn = storage._get_connection()
            cursor = conn.cursor()
            cursor.execute(
                "SELECT id FROM hyperscaler_announcements WHERE url = ?",
                (announcement.url,)
            )
            row = cursor.fetchone()
            conn.close()
            if row and (analysis["capacity_mw"] is not None or analysis["target_year"] is not None):
                storage.save_announcement_mw_data(
                    "hyperscaler_announcements",
                    row["id"],
                    analysis["capacity_mw"],
                    analysis["target_year"]
                )

    print(f"  Processed {total_announcements} new hyperscaler announcements")

    # Fetch PE data center announcements
    print("\n[10/14] Fetching PE data center announcements...")
    pe_announcements = pe_datacenter_fetcher.fetch_announcements(hours_back=72)

    # Analyze PE announcements and save to database
    total_pe_announcements = 0
    for announcement in pe_announcements:
        print(f"  Analyzing {announcement.pe_firm} announcement...")
        analysis = summarizer.analyze_pe_announcement(announcement)
        # Convert to storage dataclass
        pe_storage_announcement = PEDatacenterAnnouncement(
            id=None,
            pe_firm=announcement.pe_firm,
            title=announcement.title,
            description=announcement.description,
            url=announcement.url,
            published_at=announcement.published_at,
            content_summary=analysis["summary"]
        )

        # Save to database
        if storage.save_pe_announcement(pe_storage_announcement):
            total_pe_announcements += 1
            # Get the saved announcement's ID and store MW data
            conn = storage._get_connection()
            cursor = conn.cursor()
            cursor.execute(
                "SELECT id FROM pe_datacenter_announcements WHERE url = ?",
                (announcement.url,)
            )
            row = cursor.fetchone()
            conn.close()
            if row and (analysis["capacity_mw"] is not None or analysis["target_year"] is not None):
                storage.save_announcement_mw_data(
                    "pe_datacenter_announcements",
                    row["id"],
                    analysis["capacity_mw"],
                    analysis["target_year"]
                )

    print(f"  Processed {total_pe_announcements} new PE data center announcements")

    # Backfill MW capacity data for existing announcements missing it
    for table_name, label in [
        ("hyperscaler_announcements", "hyperscaler"),
        ("pe_datacenter_announcements", "PE"),
    ]:
        backfill_rows = storage.get_announcements_for_backfill(table_name)
        if backfill_rows:
            print(f"  Backfilling MW data for {len(backfill_rows)} {label} announcements...")
            for row in backfill_rows:
                extracted = summarizer.extract_mw_from_summary(row["content_summary"])
                storage.save_announcement_mw_data(
                    table_name,
                    row["id"],
                    extracted["capacity_mw"],
                    extracted["target_year"]
                )
            print(f"  {label} MW backfill complete")

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

    # Query MW capacity data for the email matrix
    mw_data = storage.get_mw_capacity_summary()

    # Send email
    email_sender = EmailSender()

    if dry_run:
        print("\n[14/14] Dry run mode - skipping email")
    else:
        email_type = "weekly" if is_weekly_edition else "daily"
        print(f"\n[14/14] Sending {email_type} email digest...")
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
            pe_announcements=email_pe_announcements,
            mw_data=mw_data
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

    # Check for pending seasonal earnings summary (on Fridays)
    if is_weekly_edition:
        pending_season = storage.get_pending_season_summary()
        if pending_season:
            print(f"\n  Earnings season {pending_season.quarter} is complete - "
                  f"{len(pending_season.companies_reported)} companies reported")
            _generate_seasonal_ppt_summary(
                storage, summarizer, email_sender,
                pending_season, today, dry_run
            )
        else:
            print("\n  No pending seasonal summaries to generate")

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
