# Keepa Scraper for Railway

Automated Keepa API scraper that stores ASINs and Amazon URLs in PostgreSQL.

## Environment Variables

Required environment variables for Railway:

1. **DATABASE_URL** - PostgreSQL connection string (auto-provisioned by Railway)
2. **KEEPA_API_KEY** - Your Keepa API key
3. **SHARD_FIELD** - Field to shard by (default: "avg90_SALES")
4. **SHARD_SIZE** - Shard size (default: 10000)
5. **DB_SCHEMA** - Database schema (default: "keepa_scrape")
6. **DB_TABLE** - Database table (default: "Downloaded_asin")
7. Category URLs (ELECTRONICS, CELL_PHONES_AND_ACCESSORIES, etc.)

## Setup on Railway

1. Push this repository to GitHub
2. Connect Railway to your GitHub repository
3. Railway will automatically provision a PostgreSQL database
4. Set environment variables in Railway dashboard
5. Deploy!

## Running Locally

1. Create `.env` file with your configuration
2. Install dependencies: `pip install -r requirements.txt`
3. Run: `python keepa_downloader.py`