import logging
import pandas as pd
import datetime as dt
from scraper_in_pys.mongo import Mongo
from FinMind.data import DataLoader
from dotenv import load_dotenv
import os
import time

load_dotenv()
logger = logging.getLogger(__name__)


class StockPriceScraper:
    """Fetches daily stock prices from FinMind and stores them in MongoDB."""

    def __init__(self, stock_id_list=None):
        self.repo = Mongo(db='trading_bot', collection='stock_price')
        self.stock_id_list = stock_id_list or self.repo.get_stock_id_list()
        self.dl = DataLoader()
        self.dl.login_by_token(api_token=os.getenv('FINMIND_API_KEY'))
        self.dl.login(
            user_id=os.getenv('FINMIND_USER_ID'),
            password=os.getenv('FINMIND_PASSWORD'),
        )
        self._status = {"total": len(self.stock_id_list), "done": 0, "errors": 0, "running": False}

    @property
    def status(self):
        return dict(self._status)

    def update_data(self, progress_callback=None):
        """Incrementally update stock prices for all tracked stocks."""
        self._status["running"] = True
        self._status["done"] = 0
        self._status["errors"] = 0

        today = dt.datetime.now().strftime('%Y-%m-%d')
        weekday = dt.datetime.now().weekday()

        if weekday >= 5:
            logger.info("Weekend — skipping stock price update.")
            self._status["running"] = False
            return

        for i, stock_id in enumerate(self.stock_id_list):
            stock_id = str(stock_id)
            try:
                latest = self.repo.get_latest_data_date(stock_id=stock_id)

                if latest:
                    start = (pd.to_datetime(latest) + pd.DateOffset(days=1)).strftime('%Y-%m-%d')
                    if start >= today:
                        logger.debug(f"{stock_id} is up-to-date")
                        self._status["done"] += 1
                        continue
                else:
                    start = '2013-01-01'

                df = self.dl.taiwan_stock_daily(stock_id=stock_id, start_date=start)
                if df.empty:
                    self._status["done"] += 1
                    continue

                df.rename(columns={'date': 'Timestamp'}, inplace=True)
                records = df.to_dict(orient='records')
                self.repo.upsert_documents(records, key_fields=['stock_id', 'Timestamp'])
                self._status["done"] += 1
                logger.info(f"[{i+1}/{len(self.stock_id_list)}] {stock_id}: +{len(records)} rows")

            except Exception as e:
                self._status["errors"] += 1
                logger.error(f"Error updating {stock_id}: {e}")

            if progress_callback:
                progress_callback(self._status)

        self._status["running"] = False
        logger.info(f"Stock price update complete. {self._status['done']} done, {self._status['errors']} errors.")


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    scraper = StockPriceScraper()
    scraper.update_data()
