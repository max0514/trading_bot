import logging
import datetime
import random
import time
import requests
import pandas as pd
from io import StringIO
from scraper_in_pys.mongo import Mongo

pd.options.mode.chained_assignment = None
logger = logging.getLogger(__name__)


class QuarterlyReportScraper:
    """Scrapes quarterly financial statements (balance sheet, income, cash flow) from TWSE MOPS."""

    HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                       'AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36'
    }

    def __init__(self, start_year=2013, start_season=1, end_year=None):
        self.start_year = max(start_year, 2013)
        self.start_season = start_season
        self.end_year = end_year or datetime.datetime.now().year
        self.year_now = datetime.datetime.now().year

        self.balance_sheet_repo = Mongo(db='trading_bot', collection='balance_sheet')
        self.income_sheet_repo = Mongo(db='trading_bot', collection='income_sheet')
        self.cash_flow_repo = Mongo(db='trading_bot', collection='cash_flow')

        self._status = {"total": 0, "done": 0, "errors": 0, "running": False}

    @property
    def status(self):
        return dict(self._status)

    @staticmethod
    def _adjust_weekend(y, m, d):
        day = datetime.datetime(y, m, d)
        if day.weekday() == 5:
            day += datetime.timedelta(days=2)
        elif day.weekday() == 6:
            day += datetime.timedelta(days=1)
        return day

    def _is_report_available(self, year, season):
        """Check if the financial report for a given year/season should be available by now."""
        now = datetime.datetime.now()
        deadlines = {
            4: self._adjust_weekend(year + 1, 3, 31),  # Q4 due by next year March 31
            1: self._adjust_weekend(year, 5, 15),
            2: self._adjust_weekend(year, 8, 14),
            3: self._adjust_weekend(year, 11, 14),
        }
        return now >= deadlines.get(season, now)

    def _fetch_tables(self, stock_id, year, season):
        """Fetch financial statement tables from MOPS. Try GET first, then POST."""
        url = (
            f'https://mops.twse.com.tw/server-java/t164sb01?'
            f't203sb01Form=t203sb01Form&step=1&CO_ID={stock_id}'
            f'&SYEAR={year}&SSEASON={season}&REPORT_ID=C'
        )
        try:
            resp = requests.get(url, headers=self.HEADERS, timeout=30)
            resp.encoding = 'big5'
            tables = pd.read_html(StringIO(resp.text))
            return tables, 'C'
        except Exception:
            pass

        # Fallback: POST with report_id=A
        payload = {
            'step': 1, 'CO_ID': str(stock_id),
            'SYEAR': str(year), 'SSEASON': str(season), 'REPORT_ID': 'C'
        }
        url_post = 'https://mops.twse.com.tw/server-java/t164sb01'
        resp = requests.post(url_post, data=payload, headers=self.HEADERS, timeout=30)
        resp.encoding = 'big5'
        tables = pd.read_html(StringIO(resp.text))
        return tables, 'A'

    def _extract_statements(self, tables, year):
        """Extract balance_sheet, income, cash_flow DataFrames from parsed tables."""
        offset = 0 if year >= 2019 else 1
        return tables[offset], tables[offset + 1], tables[offset + 2]

    def _process_df(self, df, stock_id, year, season):
        """Convert a financial statement DataFrame into a dict record."""
        if year >= 2019:
            index = df.iloc[:, 1]
            values = df.iloc[:, 2]
        else:
            index = df.iloc[:, 0]
            values = df.iloc[:, 1]

        data = pd.Series(values.values, index=index.values)
        data.dropna(inplace=True)
        header = pd.Series([str(stock_id), f'{year}Q{season}'], index=['stock_id', 'Timestamp'])
        return pd.concat([header, data]).to_dict()

    def scrape_stock(self, stock_id):
        """Scrape all available quarterly reports for a single stock, storing to MongoDB."""
        results = {'balance_sheet': [], 'income_sheet': [], 'cash_flow': []}

        for year in range(self.start_year, self.end_year + 1):
            seasons = range(self.start_season if year == self.start_year else 1, 5)
            for season in seasons:
                if not self._is_report_available(year, season):
                    return results

                retries = 0
                while retries < 3:
                    try:
                        tables, report_type = self._fetch_tables(stock_id, year, season)
                        bs_df, is_df, cf_df = self._extract_statements(tables, year)

                        bs = self._process_df(bs_df, stock_id, year, season)
                        inc = self._process_df(is_df, stock_id, year, season)
                        cf = self._process_df(cf_df, stock_id, year, season)

                        results['balance_sheet'].append(bs)
                        results['income_sheet'].append(inc)
                        results['cash_flow'].append(cf)

                        rtype = '合併' if report_type == 'C' else '個別'
                        logger.info(f"{stock_id} {year}Q{season} ({rtype}) done")
                        break

                    except Exception as e:
                        retries += 1
                        logger.warning(f"{stock_id} {year}Q{season} retry {retries}: {e}")
                        time.sleep(random.uniform(5, 16))

                time.sleep(random.uniform(0.5, 1.5))

        return results

    def update_financial_statements(self, progress_callback=None):
        """Update financial statements for all tracked stocks."""
        self._status["running"] = True
        self._status["done"] = 0
        self._status["errors"] = 0

        stock_id_list = self.balance_sheet_repo.get_stock_id_list()
        self._status["total"] = len(stock_id_list)

        for stock_id in stock_id_list:
            try:
                # Determine where to resume from
                latest = self.balance_sheet_repo.get_latest_data_date(stock_id=str(stock_id))
                if latest:
                    current_season = int(latest[-1])
                    current_year = int(latest[:4])
                    if current_season == 4:
                        start_year, start_season = current_year + 1, 1
                    else:
                        start_year, start_season = current_year, current_season + 1
                else:
                    start_year, start_season = 2013, 1

                scraper = QuarterlyReportScraper(
                    start_year=start_year, start_season=start_season
                )
                results = scraper.scrape_stock(stock_id)

                # Store results
                if results['balance_sheet']:
                    self.balance_sheet_repo.upsert_documents(
                        results['balance_sheet'], key_fields=['stock_id', 'Timestamp']
                    )
                    self.income_sheet_repo.upsert_documents(
                        results['income_sheet'], key_fields=['stock_id', 'Timestamp']
                    )
                    self.cash_flow_repo.upsert_documents(
                        results['cash_flow'], key_fields=['stock_id', 'Timestamp']
                    )

                self._status["done"] += 1
                logger.info(f"{stock_id}: {len(results['balance_sheet'])} quarters saved")

            except Exception as e:
                self._status["errors"] += 1
                logger.error(f"Error on {stock_id}: {e}")

            if progress_callback:
                progress_callback(self._status)

        self._status["running"] = False
        logger.info(f"Quarterly update complete. {self._status['done']} done, {self._status['errors']} errors.")


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    scraper = QuarterlyReportScraper()
    scraper.update_financial_statements()
