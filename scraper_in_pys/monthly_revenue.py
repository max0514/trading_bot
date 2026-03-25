import logging
import random
import pandas as pd
import requests
import datetime as dt
import numpy as np
from scraper_in_pys.mongo import Mongo
from io import StringIO

logger = logging.getLogger(__name__)

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Version/17.2 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0',
]


def _random_header():
    return {
        'Accept': '*/*',
        'Connection': 'keep-alive',
        'User-Agent': random.choice(USER_AGENTS),
    }


class MonthlyRevenueScraper:
    """Scrapes monthly revenue data from TWSE MOPS and stores in MongoDB."""

    def __init__(self):
        self.mongo = Mongo(db='trading_bot', collection='month_revenue')
        self._status = {"total": 0, "done": 0, "errors": 0, "running": False}

    @property
    def status(self):
        return dict(self._status)

    def _generate_urls(self, latest):
        if latest is None:
            latest = pd.Timestamp(2013, 1, 1)
        else:
            latest = pd.Timestamp(latest)

        date_range = pd.date_range(latest, dt.datetime.now(), freq='1ME')
        # Skip first month (already in DB)
        date_range = date_range[1:]

        urls = []
        timestamps = []
        for date in date_range:
            year = date.year
            month = date.month
            timestamps.append(date.strftime('%Y-%m-%d'))
            url = f'https://mops.twse.com.tw/nas/t21/sii/t21sc03_{year - 1911}_{month}_0.html'
            urls.append(url)

        return urls, timestamps

    def _crawl_monthly_report(self, url, timestamp):
        r = requests.get(url, headers=_random_header(), timeout=30)
        r.encoding = 'big5'

        if '查無資料' in r.text:
            return pd.DataFrame()

        dfs = pd.read_html(StringIO(r.text))
        return self._process_html(dfs, timestamp)

    def _process_html(self, dfs, timestamp):
        df = pd.concat([df for df in dfs if 5 < df.shape[1] <= 11])

        if '備註' in df.columns.get_level_values(-1):
            df = df.drop(columns=['備註'], errors='ignore')

        df.columns = df.columns.get_level_values(1)
        df = df.drop(columns=['當月累計營收', '公司名稱'], errors='ignore')
        df = df.replace('不適用', np.nan)
        df.insert(1, 'Timestamp', timestamp)

        df.columns = [
            'stock_id', 'Timestamp', '當月營收', '上月營收', '去年當月營收',
            '上月比較增減(%)', '去年同月增減(%)', '去年累計營收', '前期比較增減(%)'
        ]
        numeric_cols = ['當月營收', '上月營收', '去年當月營收', '上月比較增減(%)',
                        '去年同月增減(%)', '去年累計營收', '前期比較增減(%)']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        df = df[df['stock_id'] != '合計']
        return df

    def update_monthly_revenue(self, progress_callback=None):
        """Incrementally update monthly revenue for all stocks."""
        self._status["running"] = True
        self._status["done"] = 0
        self._status["errors"] = 0

        latest = self.mongo.get_latest_data_date()
        logger.info(f"Latest revenue date in DB: {latest}")

        urls, timestamps = self._generate_urls(latest)
        self._status["total"] = len(urls)

        for url, ts in zip(urls, timestamps):
            try:
                df = self._crawl_monthly_report(url, ts)
                if df.empty:
                    logger.warning(f"No data for {ts}")
                    self._status["done"] += 1
                    continue

                records = df.to_dict('records')
                self.mongo.upsert_documents(records, key_fields=['stock_id', 'Timestamp'])
                self._status["done"] += 1
                logger.info(f"Revenue {ts}: {len(records)} records saved")

            except Exception as e:
                self._status["errors"] += 1
                logger.error(f"Error scraping revenue for {ts}: {e}")

            if progress_callback:
                progress_callback(self._status)

        self._status["running"] = False
        logger.info(f"Monthly revenue update complete. {self._status['done']} done, {self._status['errors']} errors.")


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    scraper = MonthlyRevenueScraper()
    scraper.update_monthly_revenue()
