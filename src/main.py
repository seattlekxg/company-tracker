#!/usr/bin/env python3
"""Main orchestration script for the Company Tracker."""

import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from anthropic.types.message_create_params import MessageCreateParamsNonStreaming
from anthropic.types.messages.batch_create_params import Request

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

    # Skip weekends (Saturday=5, Sunday=6)
    if today.weekday() in (5, 6):
        day_name = "Saturday" if today.weekday() == 5 else "Sunday"
        print(f"Date: {today.strftime('%B %d, %Y')} ({day_name})")
        print("Skipping — emails do not run on weekends.")
        print("=" * 60)
        return True

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

    # ========== Steps 3-5: Fetch all news ==========

    print("\n[3/14] Fetching news articles...")
    articles_by_company = news_fetcher.fetch_all_companies(
        COMPANIES,
        company_ids,
        hours_back=72,
        rate_limit_delay=1.0
    )

    print("\n[4/14] Fetching RSS feeds (press releases)...")
    rss_articles_by_company = rss_fetcher.fetch_all_companies(
        COMPANIES,
        company_ids,
        hours_back=72,
        rate_limit_delay=0.5
    )

    if gnews_fetcher:
        print("\n[5/14] Fetching GNews (international news)...")
        gnews_articles_by_company = gnews_fetcher.fetch_all_companies(
            COMPANIES,
            company_ids,
            hours_back=72,
            rate_limit_delay=1.0
        )
    else:
        print("\n[5/14] Skipping GNews (no API key configured)")
        gnews_articles_by_company = {}

    # ========== Step 5.5: BATCH 1 — Filter all articles ==========

    print("\n  [5.5] Filtering all articles via batch API...")

    # Collect all (company_name, articles) pairs that need filtering
    all_filter_pairs = []  # list of (company_name, articles, source_label)

    for company_name, articles in articles_by_company.items():
        if articles:
            all_filter_pairs.append((company_name, articles, "newsapi"))

    for company_name, articles in rss_articles_by_company.items():
        if articles:
            all_filter_pairs.append((company_name, articles, "rss"))

    for company_name, articles in gnews_articles_by_company.items():
        if articles:
            all_filter_pairs.append((company_name, articles, "gnews"))

    total_fetched = sum(len(pair[1]) for pair in all_filter_pairs)

    if all_filter_pairs:
        # Build batch requests
        filter_batch_requests = []
        filter_batch_meta = []  # parallel list to track metadata
        for idx, (company_name, articles, source) in enumerate(all_filter_pairs):
            params = summarizer._build_filter_params(company_name, articles)
            filter_batch_requests.append(
                Request(
                    custom_id=f"filter-{idx}",
                    params=MessageCreateParamsNonStreaming(**params),
                )
            )
            filter_batch_meta.append((company_name, articles, source))

        # Submit and poll
        try:
            batch_id = summarizer.submit_batch(filter_batch_requests)
            print(f"  Filter batch submitted: {batch_id} ({len(filter_batch_requests)} requests)")
            summarizer.poll_batch(batch_id)
            filter_results = summarizer.get_batch_results(batch_id)
        except Exception as e:
            print(f"  Warning: Filter batch failed ({e}), falling back to no filtering")
            filter_results = {}

        # Apply filtering results
        filtered_articles_by_company = {name: [] for name in articles_by_company}
        for idx, (company_name, articles, source) in enumerate(filter_batch_meta):
            custom_id = f"filter-{idx}"
            if custom_id in filter_results:
                filtered = summarizer._parse_filter_response(
                    filter_results[custom_id], articles
                )
            else:
                # Batch item failed — keep all articles as fallback
                filtered = articles

            if len(filtered) < len(articles):
                print(f"    {company_name} ({source}): {len(filtered)}/{len(articles)} articles relevant")

            if source == "newsapi":
                filtered_articles_by_company.setdefault(company_name, []).extend(filtered)
            elif source == "rss":
                filtered_articles_by_company.setdefault(company_name, []).extend(filtered)
            elif source == "gnews":
                filtered_articles_by_company.setdefault(company_name, []).extend(filtered)
    else:
        filtered_articles_by_company = {name: [] for name in articles_by_company}

    total_relevant = sum(len(a) for a in filtered_articles_by_company.values())
    print(f"  Filtered to {total_relevant}/{total_fetched} relevant articles")

    # ========== Step 5.6: Save filtered articles to DB ==========

    total_new = 0
    for company_name, articles in filtered_articles_by_company.items():
        for article in articles:
            if storage.save_article(article):
                total_new += 1
    print(f"  Saved {total_new} new articles to database")

    # ========== Step 6: Fetch financial data ==========

    print("\n[6/14] Fetching financial data...")
    snapshots_by_company = finance_fetcher.fetch_all_companies(
        COMPANIES,
        company_ids
    )

    for company_name, snapshot in snapshots_by_company.items():
        if snapshot:
            storage.save_financial_snapshot(snapshot)
    print("  Saved financial snapshots to database")

    # ========== Steps 7-10: Fetch & save WITHOUT summaries ==========

    # Step 7: SEC filings
    print("\n[7/14] Fetching SEC filings...")
    filings_by_company = sec_fetcher.fetch_all_companies(
        COMPANIES,
        company_ids,
        days_back=30,
        rate_limit_delay=0.2
    )

    # Fetch filing content and save to DB WITHOUT analysis
    filings_content = {}  # accession_number -> (filing, content)
    total_filings = 0
    for company_name, filings in filings_by_company.items():
        for filing in filings:
            content = sec_fetcher.fetch_filing_content(filing, max_chars=50000)
            if content:
                filing.content_summary = None  # Will be filled by batch
                filings_content[filing.accession_number] = (filing, content)
            else:
                filing.content_summary = "Unable to fetch filing content"

            if storage.save_sec_filing(filing):
                total_filings += 1

    print(f"  Saved {total_filings} new SEC filings (analysis pending)")

    # Step 8: Earnings transcripts
    print("\n[8/14] Fetching earnings call transcripts...")
    transcripts_by_company = transcript_fetcher.fetch_all_companies(
        COMPANIES,
        company_ids,
        quarters_back=2,
        rate_limit_delay=0.5
    )

    # Save transcripts to DB WITHOUT analysis
    transcripts_for_batch = []  # (company_name, transcript)
    total_transcripts = 0
    for company_name, transcripts in transcripts_by_company.items():
        for transcript in transcripts:
            if transcript.transcript_text:
                transcript.content_summary = None  # Will be filled by batch
                transcripts_for_batch.append((company_name, transcript))
            else:
                transcript.content_summary = "No transcript content available"

            if storage.save_earnings_transcript(transcript):
                total_transcripts += 1

    print(f"  Saved {total_transcripts} new earnings transcripts (analysis pending)")

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
    for company in COMPANIES:
        if not company.ticker:
            continue
        company_db = storage.get_company_by_name(company.name)
        if not company_db:
            continue
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

    _check_season_completeness(storage, today)

    # Step 9: Hyperscaler announcements
    print("\n[9/14] Fetching hyperscaler announcements...")
    hyperscaler_announcements = hyperscaler_fetcher.fetch_announcements(hours_back=72)

    # Save to DB WITHOUT analysis
    hyperscaler_for_batch = []  # (index, announcement)
    total_announcements = 0
    for i, announcement in enumerate(hyperscaler_announcements):
        announcement.content_summary = None  # Will be filled by batch
        if storage.save_hyperscaler_announcement(announcement):
            total_announcements += 1
            hyperscaler_for_batch.append((i, announcement))

    print(f"  Saved {total_announcements} new hyperscaler announcements (analysis pending)")

    # Step 10: PE announcements
    print("\n[10/14] Fetching PE data center announcements...")
    pe_announcements = pe_datacenter_fetcher.fetch_announcements(hours_back=72)

    pe_for_batch = []  # (index, pe_storage_announcement, original_announcement)
    total_pe_announcements = 0
    for i, announcement in enumerate(pe_announcements):
        pe_storage_announcement = PEDatacenterAnnouncement(
            id=None,
            pe_firm=announcement.pe_firm,
            title=announcement.title,
            description=announcement.description,
            url=announcement.url,
            published_at=announcement.published_at,
            content_summary=None  # Will be filled by batch
        )

        if storage.save_pe_announcement(pe_storage_announcement):
            total_pe_announcements += 1
            pe_for_batch.append((i, pe_storage_announcement, announcement))

    print(f"  Saved {total_pe_announcements} new PE data center announcements (analysis pending)")

    # ========== Step 10.5: Collect MW backfill items ==========

    backfill_items = []  # (table_name, row_id, content_summary)
    for table_name in ("hyperscaler_announcements", "pe_datacenter_announcements"):
        backfill_rows = storage.get_announcements_for_backfill(table_name)
        for row in backfill_rows:
            backfill_items.append((table_name, row["id"], row["content_summary"]))

    # ========== Step 10.6: BATCH 2 — All analysis ==========

    analysis_batch_requests = []
    analysis_batch_keys = []  # parallel list: (type, metadata)

    # SEC filing analysis
    for accession_number, (filing, content) in filings_content.items():
        if content and filing.content_summary is None:
            params = summarizer._build_sec_filing_params(filing, content)
            analysis_batch_requests.append(
                Request(
                    custom_id=f"sec-{accession_number}",
                    params=MessageCreateParamsNonStreaming(**params),
                )
            )
            analysis_batch_keys.append(("sec", accession_number))

    # Transcript analysis
    for company_name, transcript in transcripts_for_batch:
        if transcript.transcript_text and transcript.content_summary is None:
            params = summarizer._build_transcript_params(transcript, transcript.transcript_text)
            safe_quarter = transcript.quarter.replace(" ", "_")
            analysis_batch_requests.append(
                Request(
                    custom_id=f"transcript-{transcript.company_id}-{safe_quarter}",
                    params=MessageCreateParamsNonStreaming(**params),
                )
            )
            analysis_batch_keys.append(("transcript", (transcript.company_id, transcript.quarter)))

    # Hyperscaler analysis
    for idx, announcement in hyperscaler_for_batch:
        params = summarizer._build_hyperscaler_params(announcement)
        analysis_batch_requests.append(
            Request(
                custom_id=f"hs-{idx}",
                params=MessageCreateParamsNonStreaming(**params),
            )
        )
        analysis_batch_keys.append(("hs", (idx, announcement)))

    # PE analysis
    for idx, pe_storage_ann, original_ann in pe_for_batch:
        params = summarizer._build_pe_params(original_ann)
        analysis_batch_requests.append(
            Request(
                custom_id=f"pe-{idx}",
                params=MessageCreateParamsNonStreaming(**params),
            )
        )
        analysis_batch_keys.append(("pe", (idx, pe_storage_ann, original_ann)))

    # MW backfill extraction
    for table_name, row_id, content_summary in backfill_items:
        params = summarizer._build_mw_extraction_params(content_summary)
        analysis_batch_requests.append(
            Request(
                custom_id=f"mw-{table_name}-{row_id}",
                params=MessageCreateParamsNonStreaming(**params),
            )
        )
        analysis_batch_keys.append(("mw", (table_name, row_id)))

    if analysis_batch_requests:
        print(f"\n  [10.6] Submitting analysis batch ({len(analysis_batch_requests)} requests)...")
        try:
            batch_id = summarizer.submit_batch(analysis_batch_requests)
            print(f"  Analysis batch submitted: {batch_id}")
            summarizer.poll_batch(batch_id)
            analysis_results = summarizer.get_batch_results(batch_id)
        except Exception as e:
            print(f"  Warning: Analysis batch failed ({e}), items will have no summaries")
            analysis_results = {}

        # ========== Step 10.7: Distribute batch results ==========

        print("  [10.7] Distributing batch results...")

        for custom_id, response_text in analysis_results.items():
            if custom_id.startswith("sec-"):
                accession_number = custom_id[4:]
                summary = summarizer._parse_sec_filing_response(response_text)
                storage.update_sec_filing_summary(accession_number, summary)

            elif custom_id.startswith("transcript-"):
                # Parse "transcript-{company_id}-{quarter}"
                parts = custom_id[len("transcript-"):].split("-", 1)
                if len(parts) == 2:
                    company_id = int(parts[0])
                    quarter = parts[1].replace("_", " ")
                    summary = summarizer._parse_transcript_response(response_text)
                    storage.update_transcript_summary(company_id, quarter, summary)

            elif custom_id.startswith("hs-"):
                idx = int(custom_id[3:])
                analysis = summarizer._parse_hyperscaler_response(response_text)
                # Find the matching announcement
                for batch_idx, announcement in hyperscaler_for_batch:
                    if batch_idx == idx:
                        storage.update_hyperscaler_summary(announcement.url, analysis["summary"])
                        # Store MW data
                        ann_id = storage.get_id_by_url("hyperscaler_announcements", announcement.url)
                        if ann_id and (analysis["capacity_mw"] is not None or analysis["target_year"] is not None):
                            storage.save_announcement_mw_data(
                                "hyperscaler_announcements",
                                ann_id,
                                analysis["capacity_mw"],
                                analysis["target_year"]
                            )
                        break

            elif custom_id.startswith("pe-"):
                idx = int(custom_id[3:])
                analysis = summarizer._parse_pe_response(response_text)
                for batch_idx, pe_storage_ann, original_ann in pe_for_batch:
                    if batch_idx == idx:
                        storage.update_pe_summary(pe_storage_ann.url, analysis["summary"])
                        ann_id = storage.get_id_by_url("pe_datacenter_announcements", pe_storage_ann.url)
                        if ann_id and (analysis["capacity_mw"] is not None or analysis["target_year"] is not None):
                            storage.save_announcement_mw_data(
                                "pe_datacenter_announcements",
                                ann_id,
                                analysis["capacity_mw"],
                                analysis["target_year"]
                            )
                        break

            elif custom_id.startswith("mw-"):
                # Parse "mw-{table_name}-{row_id}"
                rest = custom_id[3:]
                # table_name contains underscores, so split from the right
                last_dash = rest.rfind("-")
                if last_dash != -1:
                    table_name = rest[:last_dash]
                    row_id = int(rest[last_dash + 1:])
                    extracted = summarizer._parse_mw_extraction_response(response_text)
                    # Use 0 as sentinel when both are unknown, so backfill
                    # won't retry this item (query filters capacity_mw > 0)
                    capacity = extracted["capacity_mw"] if extracted["capacity_mw"] is not None else 0
                    storage.save_announcement_mw_data(
                        table_name,
                        row_id,
                        capacity,
                        extracted["target_year"]
                    )

        print("  Batch results distributed to database")
    else:
        print("\n  No analysis items to batch")

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

    # Generate AI summary (direct call, cached)
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
