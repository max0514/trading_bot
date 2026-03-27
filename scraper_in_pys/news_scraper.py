import logging
import random
import time
import re
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from scraper_in_pys.db_factory import get_db

logger = logging.getLogger(__name__)

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0',
]


def _headers():
    return {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
        'User-Agent': random.choice(USER_AGENTS),
    }


class NewsScraper:
    """Scrapes financial news from multiple Taiwan sources and stores in MongoDB."""

    def __init__(self):
        self.mongo = get_db(db='trading_bot', collection='news')
        self._status = {"total": 0, "done": 0, "errors": 0, "running": False}

    @property
    def status(self):
        return dict(self._status)

    def scrape_cnyes(self, stock_id=None, pages=3):
        """Scrape news from CNYES (鉅亨網)."""
        articles = []
        for page in range(1, pages + 1):
            try:
                if stock_id:
                    url = f'https://news.cnyes.com/news/cat/tw_stock_{stock_id}?page={page}'
                else:
                    url = f'https://news.cnyes.com/news/cat/tw_stock?page={page}'

                resp = requests.get(url, headers=_headers(), timeout=15)
                soup = BeautifulSoup(resp.text, 'html.parser')

                for item in soup.select('a[href*="/news/id/"]'):
                    title = item.get_text(strip=True)
                    link = item.get('href', '')
                    if not link.startswith('http'):
                        link = 'https://news.cnyes.com' + link

                    if title and len(title) > 5:
                        articles.append({
                            'source': 'cnyes',
                            'title': title,
                            'url': link,
                            'stock_id': str(stock_id) if stock_id else None,
                            'scraped_at': datetime.now().isoformat(),
                            'Timestamp': datetime.now().strftime('%Y-%m-%d'),
                        })

                time.sleep(random.uniform(1, 3))
            except Exception as e:
                logger.error(f"CNYES page {page} error: {e}")

        return articles

    def scrape_yahoo_tw_news(self, stock_id=None, pages=3):
        """Scrape news from Yahoo Finance Taiwan."""
        articles = []
        for page in range(pages):
            try:
                if stock_id:
                    url = f'https://tw.stock.yahoo.com/quote/{stock_id}.TW/news'
                else:
                    url = 'https://tw.stock.yahoo.com/news'

                resp = requests.get(url, headers=_headers(), timeout=15)
                soup = BeautifulSoup(resp.text, 'html.parser')

                for item in soup.select('h3 a, [data-test="mega-item"] a, li a[href*="news"]'):
                    title = item.get_text(strip=True)
                    link = item.get('href', '')
                    if not link.startswith('http'):
                        link = 'https://tw.stock.yahoo.com' + link

                    if title and len(title) > 5:
                        articles.append({
                            'source': 'yahoo_tw',
                            'title': title,
                            'url': link,
                            'stock_id': str(stock_id) if stock_id else None,
                            'scraped_at': datetime.now().isoformat(),
                            'Timestamp': datetime.now().strftime('%Y-%m-%d'),
                        })

                time.sleep(random.uniform(1, 3))
            except Exception as e:
                logger.error(f"Yahoo TW page {page} error: {e}")

        return articles

    def scrape_twse_announcements(self):
        """Scrape official TWSE market announcements (重大訊息)."""
        articles = []
        try:
            url = 'https://mops.twse.com.tw/mops/web/ajax_t05sr01_1'
            payload = {
                'encodeURIComponent': 1,
                'step': 1,
                'firstin': 1,
                'off': 1,
                'keyword4': '',
                'code1': '',
                'TYPEK2': '',
                'checkbtn': '',
                'queryName': 'co_id',
                'inpuType': 'co_id',
                'TYPEK': 'all',
                'isnew': 'true',
            }
            resp = requests.post(url, data=payload, headers=_headers(), timeout=15)
            resp.encoding = 'utf-8'
            soup = BeautifulSoup(resp.text, 'html.parser')

            for row in soup.select('table tr')[1:]:
                cols = row.find_all('td')
                if len(cols) >= 4:
                    articles.append({
                        'source': 'twse_announcement',
                        'stock_id': cols[0].get_text(strip=True),
                        'company': cols[1].get_text(strip=True),
                        'title': cols[2].get_text(strip=True),
                        'date': cols[3].get_text(strip=True) if len(cols) > 3 else '',
                        'scraped_at': datetime.now().isoformat(),
                        'Timestamp': datetime.now().strftime('%Y-%m-%d'),
                    })

        except Exception as e:
            logger.error(f"TWSE announcement error: {e}")

        return articles

    def scrape_google_news(self, query='台股', num_results=20):
        """Scrape Google News search results for Taiwan stock market."""
        articles = []
        try:
            url = f'https://news.google.com/rss/search?q={query}&hl=zh-TW&gl=TW&ceid=TW:zh-Hant'
            resp = requests.get(url, headers=_headers(), timeout=15)
            soup = BeautifulSoup(resp.content, 'xml')

            for item in soup.find_all('item')[:num_results]:
                title = item.title.get_text(strip=True) if item.title else ''
                link = item.link.get_text(strip=True) if item.link else ''
                pub_date = item.pubDate.get_text(strip=True) if item.pubDate else ''
                source_tag = item.source
                source_name = source_tag.get_text(strip=True) if source_tag else 'google_news'

                articles.append({
                    'source': f'google_news:{source_name}',
                    'title': title,
                    'url': link,
                    'published_at': pub_date,
                    'scraped_at': datetime.now().isoformat(),
                    'Timestamp': datetime.now().strftime('%Y-%m-%d'),
                })

        except Exception as e:
            logger.error(f"Google News error: {e}")

        return articles

    def update_news(self, stock_ids=None, progress_callback=None):
        """Scrape news from all sources and store in MongoDB."""
        self._status = {"total": 4, "done": 0, "errors": 0, "running": True}
        all_articles = []

        # 1. Google News general market
        try:
            articles = self.scrape_google_news('台股')
            articles += self.scrape_google_news('台灣股市')
            all_articles.extend(articles)
            self._status["done"] += 1
            logger.info(f"Google News: {len(articles)} articles")
        except Exception as e:
            self._status["errors"] += 1
            logger.error(f"Google News scrape failed: {e}")

        # 2. CNYES general
        try:
            articles = self.scrape_cnyes()
            all_articles.extend(articles)
            self._status["done"] += 1
            logger.info(f"CNYES: {len(articles)} articles")
        except Exception as e:
            self._status["errors"] += 1
            logger.error(f"CNYES scrape failed: {e}")

        # 3. Yahoo TW general
        try:
            articles = self.scrape_yahoo_tw_news()
            all_articles.extend(articles)
            self._status["done"] += 1
            logger.info(f"Yahoo TW: {len(articles)} articles")
        except Exception as e:
            self._status["errors"] += 1
            logger.error(f"Yahoo TW scrape failed: {e}")

        # 4. TWSE Announcements
        try:
            articles = self.scrape_twse_announcements()
            all_articles.extend(articles)
            self._status["done"] += 1
            logger.info(f"TWSE announcements: {len(articles)} articles")
        except Exception as e:
            self._status["errors"] += 1
            logger.error(f"TWSE announcements failed: {e}")

        # 5. Stock-specific news for given IDs
        if stock_ids:
            self._status["total"] += len(stock_ids)
            for sid in stock_ids:
                try:
                    articles = self.scrape_cnyes(stock_id=sid, pages=2)
                    articles += self.scrape_yahoo_tw_news(stock_id=sid, pages=1)
                    all_articles.extend(articles)
                    self._status["done"] += 1
                except Exception as e:
                    self._status["errors"] += 1
                    logger.error(f"Stock {sid} news error: {e}")
                time.sleep(random.uniform(1, 2))

        # Deduplicate by title
        seen = set()
        unique = []
        for a in all_articles:
            if a['title'] not in seen:
                seen.add(a['title'])
                unique.append(a)

        if unique:
            self.mongo.upsert_documents(unique, key_fields=['source', 'title'])

        self._status["running"] = False
        logger.info(f"News update complete: {len(unique)} unique articles stored.")

        if progress_callback:
            progress_callback(self._status)

        return unique


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    scraper = NewsScraper()
    scraper.update_news(stock_ids=[2330, 2317, 2454])
