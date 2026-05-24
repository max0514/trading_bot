"""Monthly revenue scraper, FinMind-backed.

Pulls Taiwan-listed companies' monthly revenue from FinMind's
`taiwan_stock_month_revenue` endpoint and upserts into the `month_revenue`
collection in MongoDB.

Replaces the legacy MOPS-HTML scraper whose URLs (`mops.twse.com.tw/nas/t21/`)
have been retired by TWSE. The class+method interface is unchanged so
ScraperManager and the dashboard work without modification.
"""
from __future__ import annotations
import logging
import os
import time

from dotenv import load_dotenv
from FinMind.data import DataLoader

from scraper_in_pys.mongo import Mongo

load_dotenv()
logger = logging.getLogger(__name__)


class MonthlyRevenueScraper:
    """Fetch monthly revenue per stock via FinMind."""

    def __init__(self, stock_id_list=None, start_date: str = "2013-01-01"):
        self.repo = Mongo(collection="month_revenue")
        self.stock_id_list = stock_id_list or self.repo.get_stock_id_list()
        self.start_date = start_date
        self._dl = None
        self._status = {
            "total": len(self.stock_id_list),
            "done": 0,
            "errors": 0,
            "running": False,
        }

    @property
    def status(self):
        return dict(self._status)

    def _loader(self) -> DataLoader:
        if self._dl is None:
            dl = DataLoader()
            token = os.getenv("FINMIND_API_KEY")
            if token:
                dl.login_by_token(api_token=token)
            self._dl = dl
        return self._dl

    def update_monthly_revenue(self, progress_callback=None):
        """Incrementally fetch monthly revenue for every tracked stock."""
        if not self.repo.connected:
            logger.error("MongoDB unavailable — skipping monthly revenue update.")
            return

        self._status.update(running=True, done=0, errors=0,
                            total=len(self.stock_id_list))
        dl = self._loader()

        for i, sid in enumerate(self.stock_id_list, 1):
            sid = str(sid)
            try:
                latest = self.repo.get_latest_data_date(stock_id=sid)
                start = self.start_date if latest is None else _next_month(latest)
                df = dl.taiwan_stock_month_revenue(stock_id=sid, start_date=start)
                if not df.empty:
                    df = df.rename(columns={"date": "Timestamp"})
                    df["stock_id"] = df["stock_id"].astype(str)
                    self.repo.upsert_documents(
                        df.to_dict("records"),
                        key_fields=["stock_id", "Timestamp"],
                    )
                    logger.info(f"[{i}/{len(self.stock_id_list)}] {sid}: +{len(df)} rows")
                self._status["done"] += 1
            except Exception as e:
                self._status["errors"] += 1
                logger.error(f"{sid} failed: {e}")
                time.sleep(2)
            if progress_callback:
                progress_callback(self._status)

        self._status["running"] = False
        logger.info(
            f"Monthly revenue update done: {self._status['done']}/{self._status['total']}"
            f" ({self._status['errors']} errors)"
        )


def _next_month(date_str: str) -> str:
    """Given 'YYYY-MM-DD', return the 1st-of-next-month as 'YYYY-MM-DD'."""
    import pandas as pd
    return (pd.to_datetime(date_str) + pd.offsets.MonthBegin(1)).strftime("%Y-%m-%d")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    MonthlyRevenueScraper().update_monthly_revenue()
