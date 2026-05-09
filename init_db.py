from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_DB_PATH = BASE_DIR / "data" / "stocks.db"
DEFAULT_NOTICE_DIR = BASE_DIR / "data" / "notices"


SCHEMA_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS st_stocks (
        code TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        category TEXT NOT NULL,
        source TEXT NOT NULL DEFAULT '',
        total_market_cap REAL,
        float_market_cap REAL,
        shareholder_equity_total REAL,
        balance_sheet_report_date TEXT,
        shareholder_count INTEGER,
        shareholder_date TEXT,
        industry TEXT,
        concepts TEXT,
        fetched_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS sync_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        synced_at TEXT NOT NULL,
        source TEXT NOT NULL DEFAULT '',
        total_count INTEGER NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS tags (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS stock_tags (
        security TEXT NOT NULL,
        tag_id INTEGER NOT NULL,
        created_at TEXT NOT NULL,
        PRIMARY KEY (security, tag_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS keywords (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS announcements (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        security TEXT NOT NULL,
        stock_name TEXT,
        notice_title TEXT NOT NULL,
        notice_type TEXT,
        notice_date TEXT NOT NULL,
        detail_url TEXT NOT NULL,
        infocode TEXT NOT NULL UNIQUE,
        pdf_url TEXT,
        local_pdf_path TEXT,
        file_size INTEGER,
        download_status TEXT NOT NULL DEFAULT 'pending',
        ocr_text TEXT,
        ocr_status TEXT NOT NULL DEFAULT 'pending',
        ocr_source TEXT,
        ocr_error TEXT,
        ocr_updated_at TEXT,
        fetched_at TEXT NOT NULL,
        downloaded_at TEXT
    )
    """,
    """
    CREATE VIRTUAL TABLE IF NOT EXISTS announcements_fts USING fts5(
        infocode UNINDEXED,
        security UNINDEXED,
        stock_name,
        notice_title,
        notice_type,
        ocr_text,
        tokenize = 'trigram'
    )
    """,
    """
    CREATE TRIGGER IF NOT EXISTS announcements_ai AFTER INSERT ON announcements BEGIN
        INSERT INTO announcements_fts (
            rowid, infocode, security, stock_name, notice_title, notice_type, ocr_text
        ) VALUES (
            new.id, new.infocode, new.security, new.stock_name, new.notice_title, new.notice_type,
            COALESCE(new.ocr_text, '')
        );
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS announcements_ad AFTER DELETE ON announcements BEGIN
        DELETE FROM announcements_fts WHERE rowid = old.id;
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS announcements_au AFTER UPDATE ON announcements BEGIN
        DELETE FROM announcements_fts WHERE rowid = old.id;
        INSERT INTO announcements_fts (
            rowid, infocode, security, stock_name, notice_title, notice_type, ocr_text
        ) VALUES (
            new.id, new.infocode, new.security, new.stock_name, new.notice_title, new.notice_type,
            COALESCE(new.ocr_text, '')
        );
    END
    """,
    """
    CREATE TABLE IF NOT EXISTS announcement_chunks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        announcement_id INTEGER NOT NULL,
        security TEXT NOT NULL,
        stock_name TEXT,
        notice_title TEXT NOT NULL,
        notice_type TEXT,
        notice_date TEXT NOT NULL,
        chunk_index INTEGER NOT NULL,
        chunk_text TEXT NOT NULL,
        chunk_hash TEXT NOT NULL,
        char_count INTEGER NOT NULL DEFAULT 0,
        parse_source TEXT NOT NULL DEFAULT 'ocr',
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        UNIQUE(announcement_id, chunk_index)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_announcement_chunks_security_date
    ON announcement_chunks (security, notice_date, announcement_id, chunk_index)
    """,
    """
    CREATE TABLE IF NOT EXISTS announcement_parse_tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        announcement_id INTEGER NOT NULL,
        security TEXT NOT NULL,
        task_type TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending',
        error_message TEXT,
        started_at TEXT,
        finished_at TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS idx_announcement_parse_tasks_unique
    ON announcement_parse_tasks (announcement_id, task_type)
    """,
    """
    CREATE TABLE IF NOT EXISTS announcement_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        announcement_id INTEGER NOT NULL,
        security TEXT NOT NULL,
        stock_name TEXT,
        event_type TEXT NOT NULL,
        risk_level TEXT NOT NULL DEFAULT 'medium',
        event_date TEXT,
        subject TEXT,
        summary TEXT NOT NULL,
        evidence_text TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_announcement_events_security_type_date
    ON announcement_events (security, event_type, event_date)
    """,
    """
    CREATE TABLE IF NOT EXISTS qa_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        question TEXT NOT NULL,
        question_type TEXT NOT NULL DEFAULT '',
        retrieved_chunks TEXT NOT NULL DEFAULT '[]',
        retrieved_announcements TEXT NOT NULL DEFAULT '[]',
        answer TEXT NOT NULL DEFAULT '',
        created_at TEXT NOT NULL
    )
    """,
]


def initialize_database(db_path: Path, notice_dir: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    notice_dir.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        for statement in SCHEMA_STATEMENTS:
            conn.execute(statement)
        conn.commit()


def main() -> None:
    parser = argparse.ArgumentParser(description="Initialize the local SQLite schema.")
    parser.add_argument(
        "--db",
        dest="db_path",
        default=str(DEFAULT_DB_PATH),
        help="SQLite database path, default: data/stocks.db",
    )
    parser.add_argument(
        "--notice-dir",
        dest="notice_dir",
        default=str(DEFAULT_NOTICE_DIR),
        help="Notice PDF directory, default: data/notices",
    )
    args = parser.parse_args()

    db_path = Path(args.db_path).resolve()
    notice_dir = Path(args.notice_dir).resolve()
    initialize_database(db_path, notice_dir)
    print(f"Initialized database schema at {db_path}")
    print(f"Ensured notice directory exists at {notice_dir}")


if __name__ == "__main__":
    main()
