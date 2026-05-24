"""Quarterly financial statements scraper, FinMind-backed.

Replaces the legacy MOPS-HTML scraper whose URLs (`mops.twse.com.tw/server-java/t164sb01`)
have been retired. Pulls income statement, balance sheet, and cash flow via FinMind:
    - taiwan_stock_financial_statement   → income statement
    - taiwan_stock_balance_sheet         → balance sheet
    - taiwan_stock_cash_flows_statement  → cash flows

NOTE on R&D: FinMind's free tier exposes aggregate line items (`Revenue`,
`OperatingExpenses`, `GrossProfit`, ...) but does not break out R&D as a
separate field. Strategies that need a true R&D ratio should use
`OperatingExpenses / Revenue` as a proxy or upgrade to a paid data source.

Stores into MongoDB collections:
    - financial_statement   (income statement long format)
    - balance_sheet         (long format)
    - cash_flow             (long format)
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


class QuarterlyReportScraper:
    """Fetch quarterly statements (income / balance / cash flow) per stock via FinMind."""

    def __init__(self, stock_id_list=None, start_date: str = "2013-01-01"):
        self.income_repo = Mongo(collection="financial_statement")
        self.balance_repo = Mongo(collection="balance_sheet")
        self.cashflow_repo = Mongo(collection="cash_flow")
        self.stock_id_list = stock_id_list or self.income_repo.get_stock_id_list()
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

    def _push(self, repo: Mongo, df, sid: str):
        if df is None or df.empty:
            return 0
        df = df.rename(columns={"date": "Timestamp"})
        df["stock_id"] = df["stock_id"].astype(str)
        repo.upsert_documents(
            df.to_dict("records"),
            key_fields=["stock_id", "Timestamp", "type"],
        )
        return len(df)

    def update_financial_statements(self, progress_callback=None):
        if not self.income_repo.connected:
            logger.error("MongoDB unavailable — skipping quarterly update.")
            return

        self._status.update(running=True, done=0, errors=0,
                            total=len(self.stock_id_list))
        dl = self._loader()

        for i, sid in enumerate(self.stock_id_list, 1):
            sid = str(sid)
            try:
                n_inc = self._push(self.income_repo,
                                   dl.taiwan_stock_financial_statement(
                                       stock_id=sid, start_date=self.start_date), sid)
                n_bs = self._push(self.balance_repo,
                                  dl.taiwan_stock_balance_sheet(
                                      stock_id=sid, start_date=self.start_date), sid)
                n_cf = self._push(self.cashflow_repo,
                                  dl.taiwan_stock_cash_flows_statement(
                                      stock_id=sid, start_date=self.start_date), sid)
                logger.info(f"[{i}/{len(self.stock_id_list)}] {sid}: inc={n_inc} bs={n_bs} cf={n_cf}")
                self._status["done"] += 1
            except Exception as e:
                self._status["errors"] += 1
                logger.error(f"{sid} failed: {e}")
                time.sleep(2)
            if progress_callback:
                progress_callback(self._status)

        self._status["running"] = False
        logger.info(
            f"Quarterly update done: {self._status['done']}/{self._status['total']}"
            f" ({self._status['errors']} errors)"
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    QuarterlyReportScraper().update_financial_statements()
