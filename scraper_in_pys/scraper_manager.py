import logging
import threading
from datetime import datetime

logger = logging.getLogger(__name__)

# Scraper class names mapped to their module paths for lazy import
SCRAPER_REGISTRY = {
    'stock_price': ('scraper_in_pys.stock_price', 'StockPriceScraper', 'update_data'),
    'monthly_revenue': ('scraper_in_pys.monthly_revenue', 'MonthlyRevenueScraper', 'update_monthly_revenue'),
    'quarterly_report': ('scraper_in_pys.quarter_report', 'QuarterlyReportScraper', 'update_financial_statements'),
    'news': ('scraper_in_pys.news_scraper', 'NewsScraper', 'update_news'),
    'ptt': ('scraper_in_pys.ptt_scraper', 'PTTScraper', 'update_ptt'),
}

# Default kwargs for each scraper's update method
SCRAPER_KWARGS = {
    'news': {'stock_ids': [2330, 2317, 2454, 2308, 3008]},
    'ptt': {'pages': 5, 'fetch_content': False},
}


class _DummyStatus:
    """Placeholder status when scraper hasn't been initialized yet."""
    @property
    def status(self):
        return {"total": 0, "done": 0, "errors": 0, "running": False}


class ScraperManager:
    """Orchestrates all scrapers with status tracking for the dashboard.

    Scrapers are initialized lazily on first run to avoid requiring
    a database connection at dashboard startup.
    """

    def __init__(self):
        self._scrapers = {}  # Lazily populated
        self._threads = {}
        self._log = []
        self._dummy = _DummyStatus()

    def _get_scraper(self, name):
        """Lazily create and cache a scraper instance."""
        if name not in self._scrapers:
            if name not in SCRAPER_REGISTRY:
                raise ValueError(f"Unknown scraper: {name}")
            module_path, class_name, _ = SCRAPER_REGISTRY[name]
            import importlib
            mod = importlib.import_module(module_path)
            cls = getattr(mod, class_name)
            self._scrapers[name] = cls()
        return self._scrapers[name]

    def get_status(self):
        """Return status of all scrapers."""
        result = {}
        for name in SCRAPER_REGISTRY:
            if name in self._scrapers:
                s = self._scrapers[name].status
            else:
                s = self._dummy.status
            s['name'] = name
            result[name] = s
        return result

    def get_log(self, limit=50):
        return self._log[-limit:]

    def _add_log(self, scraper_name, message, level='INFO'):
        entry = {
            'timestamp': datetime.now().isoformat(),
            'scraper': scraper_name,
            'message': message,
            'level': level,
        }
        self._log.append(entry)
        if len(self._log) > 500:
            self._log = self._log[-300:]

    def is_running(self, scraper_name):
        t = self._threads.get(scraper_name)
        return t is not None and t.is_alive()

    def run_scraper(self, scraper_name):
        """Run a scraper in a background thread (lazy init)."""
        if self.is_running(scraper_name):
            logger.warning(f"{scraper_name} is already running")
            return False

        def _run():
            self._add_log(scraper_name, 'Started')
            try:
                scraper = self._get_scraper(scraper_name)
                _, _, method_name = SCRAPER_REGISTRY[scraper_name]
                method = getattr(scraper, method_name)
                kwargs = SCRAPER_KWARGS.get(scraper_name, {})
                method(**kwargs)
                self._add_log(scraper_name, 'Completed successfully')
            except Exception as e:
                self._add_log(scraper_name, f'Failed: {e}', level='ERROR')
                logger.error(f"{scraper_name} failed: {e}")

        t = threading.Thread(target=_run, daemon=True)
        self._threads[scraper_name] = t
        t.start()
        return True

    def run_all(self):
        for name in SCRAPER_REGISTRY:
            self.run_scraper(name)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    manager = ScraperManager()
    manager.run_all()
