import logging
import random
import re
import time
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from scraper_in_pys.mongo import Mongo

logger = logging.getLogger(__name__)

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0',
]

PTT_BASE = 'https://www.ptt.cc'


class PTTScraper:
    """Scrapes PTT Stock board (批踢踢股票版) for market sentiment and discussion."""

    def __init__(self):
        self.mongo = Mongo(db='trading_bot', collection='ptt_posts')
        self.session = requests.Session()
        # Accept over-18 cookie for PTT
        self.session.cookies.set('over18', '1')
        self._status = {"total": 0, "done": 0, "errors": 0, "running": False}

    @property
    def status(self):
        return dict(self._status)

    def _headers(self):
        return {
            'User-Agent': random.choice(USER_AGENTS),
            'Accept-Language': 'zh-TW,zh;q=0.9,en;q=0.8',
        }

    def scrape_board(self, board='Stock', pages=5):
        """Scrape recent posts from a PTT board."""
        posts = []
        url = f'{PTT_BASE}/bbs/{board}/index.html'

        for page_num in range(pages):
            try:
                resp = self.session.get(url, headers=self._headers(), timeout=15)
                soup = BeautifulSoup(resp.text, 'html.parser')

                # Get entries
                for entry in soup.select('.r-ent'):
                    title_tag = entry.select_one('.title a')
                    if not title_tag:
                        continue

                    title = title_tag.get_text(strip=True)
                    link = PTT_BASE + title_tag['href']
                    author = entry.select_one('.meta .author')
                    author = author.get_text(strip=True) if author else ''
                    date = entry.select_one('.meta .date')
                    date = date.get_text(strip=True) if date else ''
                    push = entry.select_one('.nrec span')
                    push_count = push.get_text(strip=True) if push else '0'

                    # Detect stock_ids mentioned in title
                    mentioned_stocks = re.findall(r'\b(\d{4})\b', title)

                    # Categorize post type
                    post_type = 'general'
                    if title.startswith('[標的]'):
                        post_type = 'target'
                    elif title.startswith('[請益]'):
                        post_type = 'question'
                    elif title.startswith('[心得]'):
                        post_type = 'review'
                    elif title.startswith('[新聞]'):
                        post_type = 'news'
                    elif title.startswith('[閒聊]'):
                        post_type = 'chat'

                    posts.append({
                        'source': f'ptt_{board.lower()}',
                        'board': board,
                        'title': title,
                        'url': link,
                        'author': author,
                        'date': date,
                        'push_count': push_count,
                        'post_type': post_type,
                        'mentioned_stocks': mentioned_stocks,
                        'scraped_at': datetime.now().isoformat(),
                        'Timestamp': datetime.now().strftime('%Y-%m-%d'),
                    })

                # Find previous page link
                prev_link = None
                for btn in soup.select('.btn-group-paging a'):
                    if '上頁' in btn.get_text():
                        prev_link = PTT_BASE + btn['href']
                        break

                if prev_link:
                    url = prev_link
                else:
                    break

                time.sleep(random.uniform(0.5, 1.5))

            except Exception as e:
                logger.error(f"PTT {board} page {page_num} error: {e}")

        return posts

    def scrape_post_content(self, url):
        """Scrape the full content of a single PTT post."""
        try:
            resp = self.session.get(url, headers=self._headers(), timeout=15)
            soup = BeautifulSoup(resp.text, 'html.parser')

            # Main content
            main_content = soup.select_one('#main-content')
            if not main_content:
                return None

            # Remove metadata spans
            for meta in main_content.select('.article-metaline, .article-metaline-right'):
                meta.decompose()

            # Get text before push section
            content_text = main_content.get_text()
            # Cut at the push section
            push_start = content_text.find('※ 發信站')
            if push_start > 0:
                content_text = content_text[:push_start]

            # Count pushes
            pushes = soup.select('.push')
            push_up = sum(1 for p in pushes if p.select_one('.push-tag') and '推' in p.select_one('.push-tag').text)
            push_down = sum(1 for p in pushes if p.select_one('.push-tag') and '噓' in p.select_one('.push-tag').text)

            return {
                'content': content_text.strip()[:2000],  # Limit content length
                'push_up': push_up,
                'push_down': push_down,
                'total_comments': len(pushes),
            }

        except Exception as e:
            logger.error(f"Error scraping PTT post {url}: {e}")
            return None

    def update_ptt(self, boards=None, pages=5, fetch_content=False, progress_callback=None):
        """Scrape PTT boards and store posts in MongoDB."""
        boards = boards or ['Stock', 'Stock_D']  # Stock_D = 股票 Day trading
        self._status = {"total": len(boards), "done": 0, "errors": 0, "running": True}

        all_posts = []
        for board in boards:
            try:
                posts = self.scrape_board(board=board, pages=pages)

                if fetch_content:
                    for post in posts:
                        content = self.scrape_post_content(post['url'])
                        if content:
                            post.update(content)
                        time.sleep(random.uniform(0.3, 0.8))

                all_posts.extend(posts)
                self._status["done"] += 1
                logger.info(f"PTT {board}: {len(posts)} posts scraped")

            except Exception as e:
                self._status["errors"] += 1
                logger.error(f"PTT {board} scrape failed: {e}")

            if progress_callback:
                progress_callback(self._status)

        if all_posts:
            self.mongo.upsert_documents(all_posts, key_fields=['source', 'title', 'author'])

        self._status["running"] = False
        logger.info(f"PTT update complete: {len(all_posts)} posts stored.")
        return all_posts


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    scraper = PTTScraper()
    scraper.update_ptt(pages=3, fetch_content=True)
