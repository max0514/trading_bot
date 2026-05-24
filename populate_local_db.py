"""Populate local MongoDB with a tech-stock subset for backtesting tests.

Pure FinMind based — no MOPS scraping (their URLs have shifted).
Run: `python3 populate_local_db.py [price|rev|fin|all]`
"""
import os
import sys
import time
import logging
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(__file__))
load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("populate")

from scraper_in_pys.mongo import Mongo
from FinMind.data import DataLoader

TECH_STOCKS = sorted({
    "2330", "2454", "2303", "2379", "3008", "6488", "5347", "3034", "3035", "6669",
    "2376", "2382", "2308", "2317", "2353", "2474", "2356", "3005", "3231", "3702",
    "2412", "2884", "2891", "3037", "2345", "2337", "2344", "2451", "2458", "2401",
    "3017", "3045", "2360", "6147", "8086", "3443", "6239", "8016", "2436",
    "2492", "3023", "3054", "3060", "3680", "4938", "4958", "6271", "6281",
})

START_DATE = "2022-01-01"


def _dl():
    dl = DataLoader()
    dl.login_by_token(api_token=os.getenv("FINMIND_API_KEY"))
    return dl


def populate_prices():
    log.info(f"=== Prices: {len(TECH_STOCKS)} stocks from {START_DATE} ===")
    repo = Mongo(collection="stock_price")
    dl = _dl()
    for i, sid in enumerate(TECH_STOCKS, 1):
        try:
            df = dl.taiwan_stock_daily(stock_id=sid, start_date=START_DATE)
            if df.empty:
                continue
            df.rename(columns={"date": "Timestamp"}, inplace=True)
            repo.upsert_documents(df.to_dict("records"), key_fields=["stock_id", "Timestamp"])
            log.info(f"[{i}/{len(TECH_STOCKS)}] {sid}: {len(df)} rows")
        except Exception as e:
            log.error(f"{sid}: {e}")
            time.sleep(2)


def populate_monthly_revenue():
    log.info(f"=== Monthly revenue (FinMind): {len(TECH_STOCKS)} stocks ===")
    repo = Mongo(collection="month_revenue")
    dl = _dl()
    for i, sid in enumerate(TECH_STOCKS, 1):
        try:
            df = dl.taiwan_stock_month_revenue(stock_id=sid, start_date=START_DATE)
            if df.empty:
                continue
            df = df.rename(columns={"date": "Timestamp"})
            repo.upsert_documents(df.to_dict("records"), key_fields=["stock_id", "Timestamp"])
            log.info(f"[{i}/{len(TECH_STOCKS)}] {sid}: {len(df)} rows")
        except Exception as e:
            log.error(f"{sid}: {e}")
            time.sleep(2)


def populate_financial_statement():
    log.info(f"=== Financial statements (FinMind): {len(TECH_STOCKS)} stocks ===")
    repo = Mongo(collection="financial_statement")
    dl = _dl()
    for i, sid in enumerate(TECH_STOCKS, 1):
        try:
            df = dl.taiwan_stock_financial_statement(stock_id=sid, start_date=START_DATE)
            if df.empty:
                continue
            df = df.rename(columns={"date": "Timestamp"})
            repo.upsert_documents(df.to_dict("records"), key_fields=["stock_id", "Timestamp", "type"])
            log.info(f"[{i}/{len(TECH_STOCKS)}] {sid}: {len(df)} rows")
        except Exception as e:
            log.error(f"{sid}: {e}")
            time.sleep(2)


if __name__ == "__main__":
    target = sys.argv[1] if len(sys.argv) > 1 else "all"
    if target in ("all", "price"):
        populate_prices()
    if target in ("all", "rev"):
        populate_monthly_revenue()
    if target in ("all", "fin"):
        populate_financial_statement()
    log.info("Done.")
