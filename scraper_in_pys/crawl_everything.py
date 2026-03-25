#!/usr/bin/env python3
"""
Unified scraper runner - runs all scrapers sequentially.
For parallel execution, use scraper_manager.py or the dashboard.
"""
import logging
from scraper_in_pys.stock_price import StockPriceScraper
from scraper_in_pys.monthly_revenue import MonthlyRevenueScraper
from scraper_in_pys.quarter_report import QuarterlyReportScraper
from scraper_in_pys.news_scraper import NewsScraper
from scraper_in_pys.ptt_scraper import PTTScraper

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
)
logger = logging.getLogger(__name__)


def main():
    logger.info("=== Starting full data collection ===")

    # 1. Stock prices
    logger.info("--- Stock Prices ---")
    try:
        StockPriceScraper().update_data()
    except Exception as e:
        logger.error(f"Stock price scraper failed: {e}")

    # 2. Monthly revenue
    logger.info("--- Monthly Revenue ---")
    try:
        MonthlyRevenueScraper().update_monthly_revenue()
    except Exception as e:
        logger.error(f"Monthly revenue scraper failed: {e}")

    # 3. Quarterly reports
    logger.info("--- Quarterly Reports ---")
    try:
        QuarterlyReportScraper().update_financial_statements()
    except Exception as e:
        logger.error(f"Quarterly report scraper failed: {e}")

    # 4. News
    logger.info("--- News ---")
    try:
        NewsScraper().update_news(stock_ids=[2330, 2317, 2454, 2308, 3008])
    except Exception as e:
        logger.error(f"News scraper failed: {e}")

    # 5. PTT
    logger.info("--- PTT ---")
    try:
        PTTScraper().update_ptt(pages=5)
    except Exception as e:
        logger.error(f"PTT scraper failed: {e}")

    logger.info("=== Full data collection complete ===")


if __name__ == '__main__':
    main()
