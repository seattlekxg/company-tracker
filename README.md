# Company News & Financial Tracker

A Python-based system that monitors specified companies for news and financial updates, stores findings in SQLite, and sends AI-summarized daily email digests.

## Features

- Fetches news articles from NewsAPI for tracked companies
- Retrieves real-time stock data from Yahoo Finance
- Generates AI-powered daily summaries using Claude
- Sends professional HTML email digests via SendGrid
- Stores all data in SQLite for historical analysis
- Automated daily runs via GitHub Actions

## Architecture

```
GitHub Actions (Cron) --> Python Scripts --> SendGrid (Email)
                              |
          +-------------------+-------------------+
          |                   |                   |
     NewsAPI            Yahoo Finance         Claude API
          |                   |                   |
          +-------------------+-------------------+
                              |
                          SQLite DB
```

## Quick Start

### 1. Clone and Install

```bash
git clone https://github.com/yourusername/company-tracker.git
cd company-tracker
pip install -r requirements.txt
```

### 2. Configure Environment

```bash
cp .env.example .env
# Edit .env with your API keys
```

### 3. Run Locally

```bash
# Full run with email
python -m src.main

# Dry run (no email)
python -m src.main --dry-run
```

## Configuration

### Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `NEWSAPI_KEY` | Yes | NewsAPI key from [newsapi.org](https://newsapi.org/) |
| `ANTHROPIC_API_KEY` | Yes | Claude API key from [console.anthropic.com](https://console.anthropic.com/) |
| `SENDGRID_API_KEY` | Yes | SendGrid API key from [sendgrid.com](https://sendgrid.com/) |
| `EMAIL_TO` | Yes | Recipient email address |
| `EMAIL_FROM` | Yes | Sender email (must be verified in SendGrid) |
| `DB_PATH` | No | Custom database path (default: `data/tracker.db`) |

### Companies to Track

The default configuration tracks data center infrastructure companies:

- **Eaton** (ETN) - UPS, power distribution
- **Schneider Electric** (SBGSY) - UPS, cooling, EcoStruxure
- **Cummins** (CMI) - Backup generators, power generation
- **Caterpillar** (CAT) - Generators, power systems

Edit `src/config.py` to modify the list of tracked companies:

```python
COMPANIES = [
    Company(
        name="Eaton",
        ticker="ETN",
        keywords=["Eaton data center", "Eaton UPS", "Eaton power distribution"]
    ),
    # Add more companies...
]
```

## GitHub Actions Setup

### 1. Fork the Repository

Fork this repo to your GitHub account.

### 2. Add Repository Secrets

Go to Settings > Secrets and variables > Actions > New repository secret:

- `NEWSAPI_KEY`
- `ANTHROPIC_API_KEY`
- `SENDGRID_API_KEY`
- `EMAIL_TO`
- `EMAIL_FROM`

### 3. Enable GitHub Actions

The workflow runs automatically at 8 AM UTC daily. You can also trigger it manually:

1. Go to Actions tab
2. Select "Daily Company Tracker"
3. Click "Run workflow"

## Database Schema

The SQLite database (`data/tracker.db`) contains:

- **companies**: Tracked company information
- **news_articles**: All fetched articles (deduplicated by URL)
- **financial_snapshots**: Daily stock data
- **daily_summaries**: AI-generated summaries

## Project Structure

```
company-tracker/
├── .github/workflows/
│   └── daily-tracker.yml    # GitHub Actions workflow
├── src/
│   ├── __init__.py
│   ├── config.py            # Configuration and company list
│   ├── news_fetcher.py      # NewsAPI integration
│   ├── finance_fetcher.py   # Yahoo Finance integration
│   ├── storage.py           # SQLite database operations
│   ├── summarizer.py        # Claude API summarization
│   ├── email_sender.py      # SendGrid email sending
│   └── main.py              # Main orchestration script
├── data/
│   └── tracker.db           # SQLite database (auto-created)
├── templates/
│   └── email_template.html  # Email template
├── requirements.txt
├── .env.example
└── README.md
```

## API Free Tier Limits

| Service | Free Tier | Typical Usage |
|---------|-----------|---------------|
| NewsAPI | 100 requests/day | ~5-20 requests/run |
| Claude API | Pay per use | ~$0.01-0.05/summary |
| SendGrid | 100 emails/day | 1 email/run |
| GitHub Actions | 2000 min/month | ~2-5 min/run |

## Estimated Monthly Cost

- NewsAPI: Free
- Claude API: ~$1-3 (depends on summary length)
- SendGrid: Free
- GitHub Actions: Free

**Total: ~$1-5/month**

## Troubleshooting

### No news articles found

- Check your NewsAPI key is valid
- Verify company keywords return results on NewsAPI website
- NewsAPI free tier only searches last 30 days

### Financial data missing

- Ensure ticker symbols are correct
- Some international stocks may not be available via yfinance
- Markets may be closed (weekends/holidays)

### Email not sent

- Verify SendGrid API key
- Ensure sender email is verified in SendGrid
- Check SendGrid dashboard for delivery status

## License

MIT
