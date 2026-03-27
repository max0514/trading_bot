"""
SQLite-based local database — drop-in replacement for Mongo class.

Usage:
    Set DB_BACKEND=local in .env (or leave MONGODB_USER empty) to use this.
    Data is stored in data/trading_bot.db by default.
"""

import logging
import sqlite3
import json
import os
import pandas as pd
from pathlib import Path

logger = logging.getLogger(__name__)

# Default DB path: <project_root>/data/trading_bot.db
_DEFAULT_DB_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data')
_DB_PATH = os.environ.get('LOCAL_DB_PATH', os.path.join(_DEFAULT_DB_DIR, 'trading_bot.db'))

# Singleton connection
_conn = None


def _get_conn():
    global _conn
    if _conn is None:
        os.makedirs(os.path.dirname(_DB_PATH), exist_ok=True)
        _conn = sqlite3.connect(_DB_PATH, check_same_thread=False)
        _conn.execute("PRAGMA journal_mode=WAL")
        _conn.execute("PRAGMA synchronous=NORMAL")
    return _conn


def _table_name(db, collection):
    """Sanitize db + collection into a valid SQLite table name."""
    return f"{db}__{collection}".replace('-', '_')


class LocalDB:
    """SQLite-backed store with the same interface as the Mongo class."""

    def __init__(self, db, collection):
        self.conn = _get_conn()
        self.table = _table_name(db, collection)
        self._ensure_table()

    def _ensure_table(self):
        self.conn.execute(f"""
            CREATE TABLE IF NOT EXISTS "{self.table}" (
                _id INTEGER PRIMARY KEY AUTOINCREMENT,
                data TEXT NOT NULL
            )
        """)
        # Index on extracted fields for fast lookups
        for col in ('stock_id', 'Timestamp'):
            try:
                self.conn.execute(f"""
                    CREATE INDEX IF NOT EXISTS "idx_{self.table}_{col}"
                    ON "{self.table}" (json_extract(data, '$.{col}'))
                """)
            except Exception:
                pass
        self.conn.commit()

    # ── Write methods ──

    def send_document(self, records):
        try:
            self.conn.execute(
                f'INSERT INTO "{self.table}" (data) VALUES (?)',
                (json.dumps(records, default=str),),
            )
            self.conn.commit()
        except Exception as e:
            logger.error(f"insert failed: {e}")

    def send_documents_bulk(self, records_list):
        if not records_list:
            return
        try:
            self.conn.executemany(
                f'INSERT INTO "{self.table}" (data) VALUES (?)',
                [(json.dumps(r, default=str),) for r in records_list],
            )
            self.conn.commit()
        except Exception as e:
            logger.error(f"bulk insert failed: {e}")

    def upsert_documents(self, records_list, key_fields):
        if not records_list:
            return
        inserted = 0
        updated = 0
        for rec in records_list:
            # Build WHERE clause from key_fields
            conditions = []
            params = []
            for k in key_fields:
                if k in rec:
                    conditions.append(f"json_extract(data, '$.{k}') = ?")
                    params.append(str(rec[k]) if not isinstance(rec[k], (int, float)) else rec[k])

            if not conditions:
                self.send_document(rec)
                inserted += 1
                continue

            where = " AND ".join(conditions)
            row = self.conn.execute(
                f'SELECT _id FROM "{self.table}" WHERE {where} LIMIT 1', params
            ).fetchone()

            if row:
                self.conn.execute(
                    f'UPDATE "{self.table}" SET data = ? WHERE _id = ?',
                    (json.dumps(rec, default=str), row[0]),
                )
                updated += 1
            else:
                self.conn.execute(
                    f'INSERT INTO "{self.table}" (data) VALUES (?)',
                    (json.dumps(rec, default=str),),
                )
                inserted += 1

        self.conn.commit()
        logger.info(f"Upsert: {inserted} inserted, {updated} modified")

    # ── Read methods ──

    def _rows_to_df(self, rows):
        if not rows:
            return pd.DataFrame()
        records = [json.loads(r[0]) for r in rows]
        return pd.DataFrame(records)

    def get_oldest_data_date(self):
        row = self.conn.execute(
            f"""SELECT data FROM "{self.table}"
                ORDER BY json_extract(data, '$.Timestamp') ASC LIMIT 1"""
        ).fetchone()
        if row:
            doc = json.loads(row[0])
            return doc.get('Timestamp')
        return None

    def get_latest_data_date(self, stock_id=None):
        try:
            if stock_id:
                row = self.conn.execute(
                    f"""SELECT data FROM "{self.table}"
                        WHERE json_extract(data, '$.stock_id') = ?
                        ORDER BY json_extract(data, '$.Timestamp') DESC LIMIT 1""",
                    (str(stock_id),),
                ).fetchone()
            else:
                row = self.conn.execute(
                    f"""SELECT data FROM "{self.table}"
                        ORDER BY json_extract(data, '$.Timestamp') DESC LIMIT 1"""
                ).fetchone()
            if row:
                doc = json.loads(row[0])
                return doc.get('Timestamp')
        except Exception:
            pass
        return None

    def get_data_by_stock_id(self, stock_id):
        rows = self.conn.execute(
            f"""SELECT data FROM "{self.table}"
                WHERE json_extract(data, '$.stock_id') = ?""",
            (str(stock_id),),
        ).fetchall()
        return self._rows_to_df(rows)

    def get_data_by_date(self, date_string='2012-02-29'):
        rows = self.conn.execute(
            f"""SELECT data FROM "{self.table}"
                WHERE json_extract(data, '$.Timestamp') = ?""",
            (str(date_string),),
        ).fetchall()
        return self._rows_to_df(rows)

    def get_all_data(self):
        rows = self.conn.execute(f'SELECT data FROM "{self.table}"').fetchall()
        return self._rows_to_df(rows)

    def get_recent_data(self, limit=100, sort_field='Timestamp'):
        rows = self.conn.execute(
            f"""SELECT data FROM "{self.table}"
                ORDER BY json_extract(data, '$.{sort_field}') DESC
                LIMIT ?""",
            (limit,),
        ).fetchall()
        return self._rows_to_df(rows)

    def get_stock_id_list(self):
        # Reuse the same hardcoded list from Mongo class
        from scraper_in_pys.mongo import Mongo as _Mongo
        return _Mongo.get_stock_id_list(self)

    def count_documents(self, query=None):
        if query:
            conditions = []
            params = []
            for k, v in query.items():
                conditions.append(f"json_extract(data, '$.{k}') = ?")
                params.append(str(v) if not isinstance(v, (int, float)) else v)
            where = " AND ".join(conditions) if conditions else "1=1"
            row = self.conn.execute(
                f'SELECT COUNT(*) FROM "{self.table}" WHERE {where}', params
            ).fetchone()
        else:
            row = self.conn.execute(f'SELECT COUNT(*) FROM "{self.table}"').fetchone()
        return row[0] if row else 0
