from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
import io
import json
import os
import re
import shutil
import sqlite3
from contextlib import closing
from contextlib import contextmanager
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import akshare as ak
import fitz
import pytesseract
import requests
from akshare.stock.cons import (
    zh_sina_a_stock_count_url,
    zh_sina_a_stock_payload,
    zh_sina_a_stock_url,
)
from akshare.utils import request as ak_request
from PIL import Image


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "stocks.db"
NOTICE_DIR = DATA_DIR / "notices"
HOST = "127.0.0.1"
PORT = 8000
DETAIL_WORKERS = 6
OCR_TEXT_MIN_LENGTH = 80
OCR_IMAGE_SCALE = 2.0
OCR_LANG = "chi_sim+eng"
PROXY_ENV_KEYS = (
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
)
DEFAULT_REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/136.0.0.0 Safari/537.36"
    ),
    "Accept": "application/pdf,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}
TESSERACT_PATH = shutil.which("tesseract")

if TESSERACT_PATH:
    pytesseract.pytesseract.tesseract_cmd = TESSERACT_PATH


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


@contextmanager
def without_proxy_env():
    saved: dict[str, str] = {}
    removed: list[str] = []
    for key in PROXY_ENV_KEYS:
        if key in os.environ:
            saved[key] = os.environ[key]
            removed.append(key)
            del os.environ[key]
    try:
        yield
    finally:
        for key in removed:
            os.environ[key] = saved[key]


@contextmanager
def akshare_direct_session():
    original_session_cls = ak_request.requests.Session
    original_requests_session_cls = requests.Session
    original_requests_sessions_session_cls = requests.sessions.Session

    class DirectSession(requests.Session):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.trust_env = False

    ak_request.requests.Session = DirectSession
    requests.Session = DirectSession
    requests.sessions.Session = DirectSession
    try:
        yield
    finally:
        ak_request.requests.Session = original_session_cls
        requests.Session = original_requests_session_cls
        requests.sessions.Session = original_requests_sessions_session_cls


def ensure_database() -> None:
    DATA_DIR.mkdir(exist_ok=True)
    NOTICE_DIR.mkdir(exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
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
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sync_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                synced_at TEXT NOT NULL,
                source TEXT NOT NULL DEFAULT '',
                total_count INTEGER NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tags (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS stock_tags (
                security TEXT NOT NULL,
                tag_id INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (security, tag_id)
            )
            """
        )
        conn.execute(
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
            """
        )
        conn.execute(
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
            """
        )
        conn.execute(
            """
            CREATE TRIGGER IF NOT EXISTS announcements_ai AFTER INSERT ON announcements BEGIN
                INSERT INTO announcements_fts (
                    rowid, infocode, security, stock_name, notice_title, notice_type, ocr_text
                ) VALUES (
                    new.id, new.infocode, new.security, new.stock_name, new.notice_title, new.notice_type, COALESCE(new.ocr_text, '')
                );
            END
            """
        )
        conn.execute(
            """
            CREATE TRIGGER IF NOT EXISTS announcements_ad AFTER DELETE ON announcements BEGIN
                DELETE FROM announcements_fts WHERE rowid = old.id;
            END
            """
        )
        conn.execute(
            """
            CREATE TRIGGER IF NOT EXISTS announcements_au AFTER UPDATE ON announcements BEGIN
                DELETE FROM announcements_fts WHERE rowid = old.id;
                INSERT INTO announcements_fts (
                    rowid, infocode, security, stock_name, notice_title, notice_type, ocr_text
                ) VALUES (
                    new.id, new.infocode, new.security, new.stock_name, new.notice_title, new.notice_type, COALESCE(new.ocr_text, '')
                );
            END
            """
        )
        stock_columns = {
            row[1] for row in conn.execute("PRAGMA table_info(st_stocks)").fetchall()
        }
        st_stock_migrations = {
            "source": "ALTER TABLE st_stocks ADD COLUMN source TEXT NOT NULL DEFAULT ''",
            "total_market_cap": "ALTER TABLE st_stocks ADD COLUMN total_market_cap REAL",
            "float_market_cap": "ALTER TABLE st_stocks ADD COLUMN float_market_cap REAL",
            "shareholder_equity_total": "ALTER TABLE st_stocks ADD COLUMN shareholder_equity_total REAL",
            "balance_sheet_report_date": "ALTER TABLE st_stocks ADD COLUMN balance_sheet_report_date TEXT",
            "shareholder_count": "ALTER TABLE st_stocks ADD COLUMN shareholder_count INTEGER",
            "shareholder_date": "ALTER TABLE st_stocks ADD COLUMN shareholder_date TEXT",
            "industry": "ALTER TABLE st_stocks ADD COLUMN industry TEXT",
            "concepts": "ALTER TABLE st_stocks ADD COLUMN concepts TEXT",
        }
        for column, sql in st_stock_migrations.items():
            if column not in stock_columns:
                conn.execute(sql)
        run_columns = {
            row[1] for row in conn.execute("PRAGMA table_info(sync_runs)").fetchall()
        }
        if "source" not in run_columns:
            conn.execute(
                "ALTER TABLE sync_runs ADD COLUMN source TEXT NOT NULL DEFAULT ''"
            )
        announcement_columns = {
            row[1] for row in conn.execute("PRAGMA table_info(announcements)").fetchall()
        }
        announcement_migrations = {
            "ocr_text": "ALTER TABLE announcements ADD COLUMN ocr_text TEXT",
            "ocr_status": "ALTER TABLE announcements ADD COLUMN ocr_status TEXT NOT NULL DEFAULT 'pending'",
            "ocr_source": "ALTER TABLE announcements ADD COLUMN ocr_source TEXT",
            "ocr_error": "ALTER TABLE announcements ADD COLUMN ocr_error TEXT",
            "ocr_updated_at": "ALTER TABLE announcements ADD COLUMN ocr_updated_at TEXT",
        }
        for column, sql in announcement_migrations.items():
            if column not in announcement_columns:
                conn.execute(sql)
        announcement_count = conn.execute("SELECT COUNT(*) FROM announcements").fetchone()[0]
        fts_count = conn.execute("SELECT COUNT(*) FROM announcements_fts").fetchone()[0]
        if announcement_count != fts_count:
            conn.execute("INSERT INTO announcements_fts(announcements_fts) VALUES ('rebuild')")
        conn.commit()


def clean_tag_name(name: str) -> str:
    name = re.sub(r"\s+", " ", (name or "").strip())
    if not name:
        raise ValueError("标签名称不能为空")
    if len(name) > 30:
        raise ValueError("标签名称不能超过 30 个字符")
    return name


def load_tags() -> list[dict[str, object]]:
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT
                t.id,
                t.name,
                t.created_at,
                t.updated_at,
                COUNT(st.security) AS stock_count
            FROM tags t
            LEFT JOIN stock_tags st
            ON st.tag_id = t.id
            GROUP BY t.id, t.name, t.created_at, t.updated_at
            ORDER BY t.name COLLATE NOCASE ASC, t.id ASC
            """
        ).fetchall()
    return [dict(row) for row in rows]


def create_tag(name: str) -> dict[str, object]:
    tag_name = clean_tag_name(name)
    now = utc_now_iso()
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.execute(
            """
            INSERT INTO tags (name, created_at, updated_at)
            VALUES (?, ?, ?)
            """,
            (tag_name, now, now),
        )
        conn.commit()
        tag_id = cursor.lastrowid
    return {"id": tag_id, "name": tag_name, "created_at": now, "updated_at": now, "stock_count": 0}


def update_tag(tag_id: int, name: str) -> dict[str, object]:
    tag_name = clean_tag_name(name)
    now = utc_now_iso()
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute(
            """
            UPDATE tags
            SET name = ?, updated_at = ?
            WHERE id = ?
            """,
            (tag_name, now, tag_id),
        )
        if cursor.rowcount == 0:
            raise ValueError("标签不存在")
        conn.commit()
        row = conn.execute(
            """
            SELECT t.id, t.name, t.created_at, t.updated_at, COUNT(st.security) AS stock_count
            FROM tags t
            LEFT JOIN stock_tags st
            ON st.tag_id = t.id
            WHERE t.id = ?
            GROUP BY t.id, t.name, t.created_at, t.updated_at
            """,
            (tag_id,),
        ).fetchone()
    return dict(row) if row else {"id": tag_id, "name": tag_name, "updated_at": now}


def delete_tag(tag_id: int) -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("DELETE FROM stock_tags WHERE tag_id = ?", (tag_id,))
        cursor = conn.execute("DELETE FROM tags WHERE id = ?", (tag_id,))
        if cursor.rowcount == 0:
            raise ValueError("标签不存在")
        conn.commit()


def replace_stock_tags(security: str, tag_ids: list[int]) -> dict[str, object]:
    unique_ids = sorted({int(tag_id) for tag_id in tag_ids})
    now = utc_now_iso()
    with sqlite3.connect(DB_PATH) as conn:
        exists = conn.execute("SELECT 1 FROM st_stocks WHERE code = ?", (security,)).fetchone()
        if not exists:
            raise ValueError("股票不存在")
        if unique_ids:
            placeholders = ",".join("?" for _ in unique_ids)
            existing_ids = {
                row[0]
                for row in conn.execute(
                    f"SELECT id FROM tags WHERE id IN ({placeholders})",
                    unique_ids,
                ).fetchall()
            }
            missing = [tag_id for tag_id in unique_ids if tag_id not in existing_ids]
            if missing:
                raise ValueError("存在无效标签")
        conn.execute("DELETE FROM stock_tags WHERE security = ?", (security,))
        if unique_ids:
            conn.executemany(
                """
                INSERT INTO stock_tags (security, tag_id, created_at)
                VALUES (?, ?, ?)
                """,
                [(security, tag_id, now) for tag_id in unique_ids],
            )
        conn.commit()
    return {"security": security, "tag_ids": unique_ids}


def normalize_category(name: str) -> str:
    stripped = name.strip().upper()
    if stripped.startswith("*ST"):
        return "*ST"
    return "ST"


def run_akshare_call(func, *args, **kwargs):
    with without_proxy_env(), akshare_direct_session():
        return func(*args, **kwargs)


def normalize_market_prefix(code: str) -> str:
    code = normalize_code(code)
    if code.startswith(("6", "5", "9")):
        return "sh"
    if code.startswith(("4", "8")):
        return "bj"
    return "sz"


def normalize_market_suffix(code: str) -> str:
    code = normalize_code(code)
    if code.startswith(("6", "5", "9")):
        return "SH"
    if code.startswith(("4", "8")):
        return "BJ"
    return "SZ"


def normalize_code(raw_code: str) -> str:
    code = str(raw_code or "").strip().lower()
    for prefix in ("sh", "sz", "bj"):
        if code.startswith(prefix):
            code = code[len(prefix) :]
            break
    return code


def to_float(value) -> float | None:
    if value in (None, "", "-"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def to_int(value) -> int | None:
    number = to_float(value)
    if number is None:
        return None
    return int(number)


def utc_today() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def two_years_ago_today() -> str:
    today = datetime.now(timezone.utc).date()
    return today.replace(year=today.year - 2).isoformat()


def build_stock_rows(df, source: str) -> list[dict[str, str]]:
    fetched_at = utc_now_iso()
    stocks: list[dict[str, str]] = []
    for row in df[["代码", "名称"]].itertuples(index=False):
        code = normalize_code(row.代码)
        name = str(row.名称).strip()
        if not code or not name or "ST" not in name.upper():
            continue
        stocks.append(
            {
                "code": code,
                "name": name,
                "category": normalize_category(name),
                "source": source,
                "total_market_cap": None,
                "float_market_cap": None,
                "shareholder_equity_total": None,
                "balance_sheet_report_date": None,
                "shareholder_count": None,
                "shareholder_date": None,
                "industry": None,
                "concepts": None,
                "fetched_at": fetched_at,
            }
        )
    return stocks


def fetch_sina_st_stock_rows() -> list[dict[str, object]]:
    with without_proxy_env():
        total_text = requests.get(zh_sina_a_stock_count_url, timeout=15).text
    total_count = int("".join(filter(str.isdigit, total_text)))
    page_count = (total_count + 79) // 80
    stocks: list[dict[str, object]] = []
    fetched_at = utc_now_iso()

    for page in range(1, page_count + 1):
        params = zh_sina_a_stock_payload.copy()
        params.update({"page": str(page)})
        with without_proxy_env():
            response = requests.get(zh_sina_a_stock_url, params=params, timeout=20)
            response.raise_for_status()
        rows = json.loads(response.text)
        for row in rows:
            code = normalize_code(row.get("code"))
            name = str(row.get("name") or "").strip()
            if not code or not name or "ST" not in name.upper():
                continue
            stocks.append(
                {
                    "code": code,
                    "name": name,
                    "category": normalize_category(name),
                    "source": "sina_a_spot",
                    "total_market_cap": to_float(row.get("mktcap")),
                    "float_market_cap": to_float(row.get("nmc")),
                    "shareholder_equity_total": None,
                    "balance_sheet_report_date": None,
                    "shareholder_count": None,
                    "shareholder_date": None,
                    "industry": None,
                    "concepts": None,
                    "fetched_at": fetched_at,
                }
            )
    return stocks


def fetch_st_stocks() -> tuple[list[dict[str, str]], str]:
    errors: list[str] = []

    try:
        with without_proxy_env(), akshare_direct_session():
            df = ak.stock_zh_a_st_em()
        return build_stock_rows(df, "eastmoney_st_board"), "eastmoney_st_board"
    except requests.exceptions.ProxyError as exc:
        errors.append(f"东方财富 ST 板块代理异常: {exc}")
    except requests.exceptions.RequestException as exc:
        errors.append(f"东方财富 ST 板块请求失败: {exc}")
    except Exception as exc:
        errors.append(f"东方财富 ST 板块处理失败: {exc}")

    try:
        stocks = fetch_sina_st_stock_rows()
        if stocks:
            return stocks, "sina_a_spot"
        errors.append("新浪 A 股实时行情返回成功，但未筛出任何 ST/*ST 记录")
    except requests.exceptions.ProxyError as exc:
        errors.append(f"新浪 A 股实时行情代理异常: {exc}")
    except requests.exceptions.RequestException as exc:
        errors.append(f"新浪 A 股实时行情请求失败: {exc}")
    except Exception as exc:
        errors.append(f"新浪 A 股实时行情处理失败: {exc}")

    raise RuntimeError("；".join(errors))


def fetch_shareholder_map() -> dict[str, dict[str, object]]:
    try:
        df = run_akshare_call(ak.stock_zh_a_gdhs, "最新")
    except Exception:
        return {}

    holder_map: dict[str, dict[str, object]] = {}
    for row in df.to_dict("records"):
        code = normalize_code(row.get("代码", ""))
        if not code:
            continue
        holder_map[code] = {
            "shareholder_count": to_int(row.get("股东户数-本次")),
            "shareholder_date": str(row.get("公告日期") or row.get("股东户数统计截止日-本次") or "").strip() or None,
        }
    return holder_map


def recent_quarter_dates(limit: int = 8) -> list[str]:
    now = datetime.now()
    quarter_ends = [(12, 31), (9, 30), (6, 30), (3, 31)]
    dates: list[str] = []
    year = now.year
    while len(dates) < limit:
        for month, day in quarter_ends:
            quarter_date = datetime(year, month, day)
            if quarter_date <= now:
                dates.append(quarter_date.strftime("%Y%m%d"))
                if len(dates) >= limit:
                    break
        year -= 1
    return dates


def fetch_latest_balance_sheet_map(codes: set[str]) -> dict[str, dict[str, object]]:
    balance_map: dict[str, dict[str, object]] = {}
    if not codes:
        return balance_map

    latest_report_date: str | None = None
    latest_df = None
    for report_date in recent_quarter_dates(limit=4):
        try:
            df = run_akshare_call(ak.stock_zcfz_em, report_date)
        except Exception:
            continue
        if not df.empty:
            latest_report_date = report_date
            latest_df = df
            break

    if latest_df is None or latest_report_date is None:
        return balance_map

    for row in latest_df.to_dict("records"):
        code = normalize_code(row.get("股票代码", ""))
        if code not in codes:
            continue
        balance_map[code] = {
            "shareholder_equity_total": to_float(row.get("股东权益合计")),
            "balance_sheet_report_date": latest_report_date,
        }

    return balance_map


def fetch_eastmoney_datacenter(report_name: str, params: dict[str, str]) -> list[dict[str, object]]:
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    with without_proxy_env():
        response = requests.get(
            url,
            params={
                "reportName": report_name,
                "source": "WEB",
                "client": "WEB",
                **params,
            },
            timeout=20,
        )
        response.raise_for_status()
    payload = response.json()
    result = payload.get("result") or {}
    return result.get("data") or []


def extract_infocode(detail_url: str) -> str | None:
    match = re.search(r"/([A-Z0-9]+)\.html", detail_url)
    return match.group(1) if match else None


def fetch_notice_pdf_url(infocode: str) -> str | None:
    url = "https://np-cnotice-wap.eastmoney.com/api/content/ann/rich"
    with without_proxy_env():
        session = requests.Session()
        session.trust_env = False
        session.headers.update(DEFAULT_REQUEST_HEADERS)
        response = session.get(
            url,
            params={
                "art_code": infocode,
                "client_source": "wap",
                "page_index": "1",
                "is_rich": "1",
                "show_act_num": "1",
                "show_abstract": "1",
                "business": "WapGonggao",
            },
            timeout=20,
        )
        response.raise_for_status()
    payload = response.json()
    data = payload.get("data") or {}
    attach_url = str(data.get("attach_url_web") or "").strip()
    return attach_url or None


def safe_filename(text: str) -> str:
    cleaned = re.sub(r'[\\/:*?"<>|]+', "_", text)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned[:120] if cleaned else "notice"


def normalize_notice_text(text: str) -> str:
    text = text.replace("\x00", " ").replace("\r", "\n")
    text = re.sub(r"[ \t\f\v]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def build_notice_fts_query(keyword: str) -> str | None:
    tokens = re.findall(r"[\u4e00-\u9fffA-Za-z0-9]+", keyword)
    parts: list[str] = []
    for token in tokens:
        if len(token) < 3:
            continue
        if re.fullmatch(r"[A-Za-z0-9]+", token):
            parts.append(f'"{token}"*')
        else:
            parts.append(f'"{token}"')
    return " AND ".join(parts) if parts else None


def extract_notice_text_from_pdf(file_path: Path) -> tuple[str, str]:
    if not file_path.exists():
        raise FileNotFoundError(str(file_path))

    page_texts: list[str] = []
    used_sources: set[str] = set()
    with fitz.open(file_path) as doc:
        for page in doc:
            text = normalize_notice_text(page.get_text("text"))
            compact = re.sub(r"\s+", "", text)
            if len(compact) >= OCR_TEXT_MIN_LENGTH:
                page_texts.append(text)
                used_sources.add("pdf_text")
                continue

            if not TESSERACT_PATH:
                page_texts.append(text)
                used_sources.add("pdf_text")
                continue
            pixmap = page.get_pixmap(matrix=fitz.Matrix(OCR_IMAGE_SCALE, OCR_IMAGE_SCALE), alpha=False)
            image = Image.open(io.BytesIO(pixmap.tobytes("png")))
            ocr_text = normalize_notice_text(
                pytesseract.image_to_string(image, lang=OCR_LANG, config="--psm 6")
            )
            final_text = ocr_text or text
            page_texts.append(final_text)
            used_sources.add("ocr" if ocr_text else "pdf_text")

    merged_text = normalize_notice_text("\n\n".join(part for part in page_texts if part))
    if not merged_text:
        raise ValueError(f"no text extracted from {file_path}")
    if used_sources == {"pdf_text"}:
        source = "pdf_text"
    elif used_sources == {"ocr"}:
        source = "ocr"
    else:
        source = "mixed"
    return merged_text, source


def is_valid_pdf_bytes(content: bytes) -> bool:
    return content.startswith(b"%PDF-")


def is_valid_pdf_file(file_path: Path) -> bool:
    if not file_path.exists() or file_path.stat().st_size < 5:
        return False
    with file_path.open("rb") as file_obj:
        return is_valid_pdf_bytes(file_obj.read(5))


def extract_bot_challenge_cookies(text: str) -> dict[str, str] | None:
    if "__tst_status" not in text or "EO_Bot_Ssid" not in text:
        return None
    cookie_seed_match = re.search(r'WTKkN:(\d+).*?bOYDu:(\d+).*?wyeCN:(\d+)', text)
    bot_ssid_match = re.search(r"\(t,(\d+)\);continue;case\"4\":var t=\"\"", text)
    if not cookie_seed_match or not bot_ssid_match:
        return None
    seed_values = [int(value) for value in cookie_seed_match.groups()]
    return {
        "__tst_status": f"{sum(seed_values)}#",
        "EO_Bot_Ssid": bot_ssid_match.group(1),
    }


def fetch_pdf_content(pdf_url: str) -> bytes:
    with without_proxy_env():
        session = requests.Session()
        session.trust_env = False
        session.headers.update(DEFAULT_REQUEST_HEADERS)
        for _ in range(3):
            response = session.get(pdf_url, timeout=60)
            response.raise_for_status()
            if is_valid_pdf_bytes(response.content):
                return response.content
            challenge = extract_bot_challenge_cookies(response.text)
            if not challenge:
                break
            for name, value in challenge.items():
                session.cookies.set(name, value, domain="pdf.dfcfw.com", path="/")
    raise ValueError(f"failed to download valid PDF from {pdf_url}")


def invalidate_broken_notice_files(security: str | None = None) -> int:
    where_sql = "download_status = 'downloaded'"
    params: list[object] = []
    if security:
        where_sql += " AND security = ?"
        params.append(security)

    broken_ids: list[int] = []
    broken_paths: list[Path] = []
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            f"""
            SELECT id, local_pdf_path
            FROM announcements
            WHERE {where_sql}
            """,
            params,
        ).fetchall()
        for row in rows:
            local_path = str(row["local_pdf_path"] or "").strip()
            file_path = Path(local_path) if local_path else None
            if not file_path or not is_valid_pdf_file(file_path):
                broken_ids.append(row["id"])
                if file_path and file_path.exists():
                    broken_paths.append(file_path)
        if broken_ids:
            placeholders = ",".join("?" for _ in broken_ids)
            conn.execute(
                f"""
                UPDATE announcements
                SET local_pdf_path = NULL,
                    file_size = NULL,
                    download_status = 'failed',
                    downloaded_at = NULL
                WHERE id IN ({placeholders})
                """,
                broken_ids,
            )
            conn.commit()

    for file_path in broken_paths:
        try:
            file_path.unlink()
        except OSError:
            pass
    return len(broken_ids)


def download_notice_pdf(security: str, notice_date: str, infocode: str, pdf_url: str, notice_title: str) -> tuple[str, int]:
    stock_dir = NOTICE_DIR / security
    stock_dir.mkdir(parents=True, exist_ok=True)
    filename = f"{notice_date}_{infocode}_{safe_filename(notice_title)}.pdf"
    file_path = stock_dir / filename
    pdf_content = fetch_pdf_content(pdf_url)
    file_path.write_bytes(pdf_content)
    return str(file_path), file_path.stat().st_size


def fetch_and_download_notices(security: str, begin_date: str, end_date: str) -> dict[str, object]:
    invalidated = invalidate_broken_notice_files(security)
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        existing_rows = conn.execute(
            """
            SELECT infocode, pdf_url, local_pdf_path, file_size, download_status, downloaded_at
            FROM announcements
            WHERE security = ?
            """,
            (security,),
        ).fetchall()
    existing_map = {str(row["infocode"]): dict(row) for row in existing_rows}
    df = run_akshare_call(
        ak.stock_individual_notice_report,
        security=security,
        begin_date=begin_date,
        end_date=end_date,
    )
    if df.empty:
        return {"security": security, "saved": 0, "downloaded": 0}

    fetched_at = utc_now_iso()
    notices: list[dict[str, object]] = []
    for row in df.to_dict("records"):
        notice_date = str(row.get("公告日期") or "").strip()
        infocode = extract_infocode(str(row.get("网址") or ""))
        if not notice_date or not infocode:
            continue
        notices.append(
            {
                "security": security,
                "stock_name": str(row.get("名称") or "").strip(),
                "notice_title": str(row.get("公告标题") or "").strip(),
                "notice_type": str(row.get("公告类型") or "").strip(),
                "notice_date": notice_date,
                "detail_url": str(row.get("网址") or "").strip(),
                "infocode": infocode,
                "pdf_url": None,
                "local_pdf_path": None,
                "file_size": None,
                "download_status": "pending",
                "fetched_at": fetched_at,
                "downloaded_at": None,
            }
        )

    downloaded = 0
    for notice in notices:
        try:
            existing = existing_map.get(str(notice["infocode"]))
            existing_path = Path(str(existing.get("local_pdf_path") or "")) if existing else None
            if (
                existing
                and existing.get("download_status") == "downloaded"
                and existing_path
                and is_valid_pdf_file(existing_path)
            ):
                notice["pdf_url"] = existing.get("pdf_url")
                notice["local_pdf_path"] = str(existing_path)
                notice["file_size"] = existing.get("file_size")
                notice["download_status"] = "downloaded"
                notice["downloaded_at"] = existing.get("downloaded_at") or utc_now_iso()
                continue
            pdf_url = fetch_notice_pdf_url(notice["infocode"])
            notice["pdf_url"] = pdf_url
            if pdf_url:
                local_path, file_size = download_notice_pdf(
                    security=security,
                    notice_date=notice["notice_date"],
                    infocode=notice["infocode"],
                    pdf_url=pdf_url,
                    notice_title=notice["notice_title"],
                )
                notice["local_pdf_path"] = local_path
                notice["file_size"] = file_size
                notice["download_status"] = "downloaded"
                notice["downloaded_at"] = utc_now_iso()
                downloaded += 1
            else:
                notice["download_status"] = "no_pdf"
        except Exception:
            notice["download_status"] = "failed"

    with sqlite3.connect(DB_PATH) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.executemany(
                """
                INSERT INTO announcements (
                    security, stock_name, notice_title, notice_type, notice_date,
                    detail_url, infocode, pdf_url, local_pdf_path, file_size,
                    download_status, fetched_at, downloaded_at
                )
                VALUES (
                    :security, :stock_name, :notice_title, :notice_type, :notice_date,
                    :detail_url, :infocode, :pdf_url, :local_pdf_path, :file_size,
                    :download_status, :fetched_at, :downloaded_at
                )
                ON CONFLICT(infocode) DO UPDATE SET
                    stock_name=excluded.stock_name,
                    notice_title=excluded.notice_title,
                    notice_type=excluded.notice_type,
                    notice_date=excluded.notice_date,
                    detail_url=excluded.detail_url,
                    pdf_url=excluded.pdf_url,
                    local_pdf_path=excluded.local_pdf_path,
                    file_size=excluded.file_size,
                    download_status=excluded.download_status,
                    fetched_at=excluded.fetched_at,
                    downloaded_at=excluded.downloaded_at
                """,
                notices,
            )
        conn.commit()

    return {
        "security": security,
        "saved": len(notices),
        "downloaded": downloaded,
        "invalidated": invalidated,
    }


def sync_notice_ocr(
    security: str | None = None,
    force: bool = False,
    limit: int | None = None,
) -> dict[str, object]:
    where_clauses = ["download_status = 'downloaded'", "local_pdf_path IS NOT NULL", "local_pdf_path <> ''"]
    params: list[object] = []
    if security:
        where_clauses.append("security = ?")
        params.append(security)
    if not force:
        where_clauses.append("COALESCE(ocr_status, 'pending') <> 'done'")
    where_sql = " AND ".join(where_clauses)
    limit_sql = ""
    if limit and limit > 0:
        limit_sql = f" LIMIT {int(limit)}"

    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            f"""
            SELECT id, infocode, local_pdf_path
            FROM announcements
            WHERE {where_sql}
            ORDER BY notice_date DESC, id DESC
            {limit_sql}
            """,
            params,
        ).fetchall()

    processed = 0
    success = 0
    failed = 0
    for row in rows:
        processed += 1
        file_path = Path(str(row["local_pdf_path"]))
        try:
            text, source = extract_notice_text_from_pdf(file_path)
            with sqlite3.connect(DB_PATH) as conn:
                conn.execute(
                    """
                    UPDATE announcements
                    SET ocr_text = ?,
                        ocr_status = 'done',
                        ocr_source = ?,
                        ocr_error = NULL,
                        ocr_updated_at = ?
                    WHERE id = ?
                    """,
                    (text, source, utc_now_iso(), row["id"]),
                )
                conn.commit()
            success += 1
        except Exception as exc:
            with sqlite3.connect(DB_PATH) as conn:
                conn.execute(
                    """
                    UPDATE announcements
                    SET ocr_status = 'failed',
                        ocr_source = NULL,
                        ocr_error = ?,
                        ocr_updated_at = ?
                    WHERE id = ?
                    """,
                    (f"{exc.__class__.__name__}: {exc}"[:500], utc_now_iso(), row["id"]),
                )
                conn.commit()
            failed += 1

    return {
        "security": security or "ALL",
        "processed": processed,
        "success": success,
        "failed": failed,
        "force": force,
        "limit": limit,
    }


def batch_sync_notices_and_ocr(securities: list[str]) -> dict[str, object]:
    normalized = []
    seen: set[str] = set()
    for security in securities:
        code = normalize_code(str(security))
        if code and code not in seen:
            normalized.append(code)
            seen.add(code)
    if not normalized:
        raise ValueError("至少选择一只股票")

    items: list[dict[str, object]] = []
    total_saved = 0
    total_downloaded = 0
    total_ocr_processed = 0
    total_ocr_success = 0
    total_ocr_failed = 0
    for security in normalized:
        notice_result = fetch_and_download_notices(
            security=security,
            begin_date=two_years_ago_today(),
            end_date=utc_today(),
        )
        ocr_result = sync_notice_ocr(security=security, force=False)
        total_saved += int(notice_result.get("saved", 0) or 0)
        total_downloaded += int(notice_result.get("downloaded", 0) or 0)
        total_ocr_processed += int(ocr_result.get("processed", 0) or 0)
        total_ocr_success += int(ocr_result.get("success", 0) or 0)
        total_ocr_failed += int(ocr_result.get("failed", 0) or 0)
        items.append(
            {
                "security": security,
                "notice": notice_result,
                "ocr": ocr_result,
            }
        )

    return {
        "count": len(normalized),
        "securities": normalized,
        "saved": total_saved,
        "downloaded": total_downloaded,
        "ocr_processed": total_ocr_processed,
        "ocr_success": total_ocr_success,
        "ocr_failed": total_ocr_failed,
        "items": items,
    }


def load_announcements(
    security: str | None = None,
    keyword: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    page: int = 1,
    page_size: int = 10,
) -> dict[str, object]:
    where_clauses = ["1 = 1"]
    params: list[object] = []
    if security:
        where_clauses.append("a.security = ?")
        params.append(security)
    if keyword:
        like_value = f"%{keyword}%"
        fts_query = build_notice_fts_query(keyword)
        keyword_clauses = [
            "a.notice_title LIKE ?",
            "a.notice_type LIKE ?",
            "a.stock_name LIKE ?",
            "COALESCE(a.ocr_text, '') LIKE ?",
        ]
        params.extend([like_value, like_value, like_value, like_value])
        if fts_query:
            keyword_clauses.append(
                "a.id IN (SELECT rowid FROM announcements_fts WHERE announcements_fts MATCH ?)"
            )
            params.append(fts_query)
        where_clauses.append(f"({' OR '.join(keyword_clauses)})")
    if date_from:
        where_clauses.append("a.notice_date >= ?")
        params.append(date_from)
    if date_to:
        where_clauses.append("a.notice_date <= ?")
        params.append(date_to)
    where_sql = " AND ".join(where_clauses)

    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        total = conn.execute(
            f"SELECT COUNT(*) FROM announcements a WHERE {where_sql}",
            params,
        ).fetchone()[0]
        page = max(page, 1)
        page_size = max(page_size, 1)
        offset = (page - 1) * page_size
        rows = conn.execute(
            f"""
            SELECT a.security, a.stock_name, a.notice_title, a.notice_type, a.notice_date,
                   a.detail_url, a.infocode, a.pdf_url, a.local_pdf_path, a.file_size,
                   a.download_status, a.ocr_status, a.ocr_source, a.ocr_updated_at,
                   a.fetched_at, a.downloaded_at, a.ocr_error,
                   substr(COALESCE(a.ocr_text, ''), 1, 240) AS ocr_text_preview
            FROM announcements a
            WHERE {where_sql}
            ORDER BY a.notice_date DESC, a.id DESC
            LIMIT ? OFFSET ?
            """,
            [*params, page_size, offset],
        ).fetchall()
    return {
        "items": [dict(row) for row in rows],
        "pagination": {
            "page": page,
            "page_size": page_size,
            "total": total,
            "total_pages": (total + page_size - 1) // page_size,
        },
    }


def get_announcement_summary(security: str | None = None) -> dict[str, object]:
    where_sql = ""
    params: tuple[object, ...] = ()
    if security:
        where_sql = "WHERE security = ?"
        params = (security,)
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            f"""
            SELECT
                COUNT(*) AS total_count,
                SUM(CASE WHEN download_status = 'downloaded' THEN 1 ELSE 0 END) AS downloaded_count,
                SUM(CASE WHEN COALESCE(ocr_status, 'pending') = 'done' THEN 1 ELSE 0 END) AS ocr_done_count,
                SUM(CASE WHEN COALESCE(ocr_status, 'pending') = 'failed' THEN 1 ELSE 0 END) AS ocr_failed_count,
                MAX(fetched_at) AS last_fetched_at
            FROM announcements
            {where_sql}
            """,
            params,
        ).fetchone()
    return {
        "security": security or "ALL",
        "total_count": row["total_count"] if row else 0,
        "downloaded_count": row["downloaded_count"] if row and row["downloaded_count"] is not None else 0,
        "ocr_done_count": row["ocr_done_count"] if row and row["ocr_done_count"] is not None else 0,
        "ocr_failed_count": row["ocr_failed_count"] if row and row["ocr_failed_count"] is not None else 0,
        "last_fetched_at": row["last_fetched_at"] if row else None,
    }


def fetch_stock_detail(code: str) -> dict[str, object]:
    details: dict[str, object] = {
        "industry": None,
        "concepts": None,
    }
    secucode = f"{normalize_code(code)}.{normalize_market_suffix(code)}"
    try:
        industry_rows = fetch_eastmoney_datacenter(
            "RPT_F10_BASIC_ORGINFO",
            {
                "columns": "ALL",
                "quoteColumns": "",
                "filter": f'(SECUCODE="{secucode}")',
                "pageNumber": "1",
                "pageSize": "1",
                "sortTypes": "",
                "sortColumns": "",
            },
        )
        if industry_rows:
            industry_row = industry_rows[0]
            details["industry"] = (
                str(industry_row.get("BOARD_NAME_LEVEL") or industry_row.get("EM2016") or "").strip()
                or None
            )

        concept_rows = fetch_eastmoney_datacenter(
            "RPT_F10_CORETHEME_BOARDTYPE",
            {
                "columns": "SECUCODE,SECURITY_CODE,SECURITY_NAME_ABBR,NEW_BOARD_CODE,BOARD_NAME,SELECTED_BOARD_REASON,IS_PRECISE,BOARD_RANK,BOARD_YIELD,DERIVE_BOARD_CODE",
                "quoteColumns": "f3~05~NEW_BOARD_CODE~BOARD_YIELD",
                "filter": f'(SECUCODE="{secucode}")(IS_PRECISE="1")',
                "pageNumber": "1",
                "pageSize": "200",
                "sortTypes": "1",
                "sortColumns": "BOARD_RANK",
            },
        )
        concepts = []
        for row in concept_rows:
            name = str(row.get("BOARD_NAME") or "").strip()
            if name and name not in concepts:
                concepts.append(name)
        details["concepts"] = "，".join(concepts) if concepts else None
    except Exception:
        pass

    return details


def enrich_stocks(stocks: list[dict[str, object]]) -> list[dict[str, object]]:
    if not stocks:
        return stocks

    holder_map = fetch_shareholder_map()
    balance_map = fetch_latest_balance_sheet_map({stock["code"] for stock in stocks})
    for stock in stocks:
        holder_info = holder_map.get(stock["code"], {})
        stock["shareholder_count"] = holder_info.get("shareholder_count")
        stock["shareholder_date"] = holder_info.get("shareholder_date")
        balance_info = balance_map.get(stock["code"], {})
        stock["shareholder_equity_total"] = balance_info.get("shareholder_equity_total")
        stock["balance_sheet_report_date"] = balance_info.get("balance_sheet_report_date")

    with ThreadPoolExecutor(max_workers=DETAIL_WORKERS) as executor:
        future_map = {
            executor.submit(fetch_stock_detail, stock["code"]): stock for stock in stocks
        }
        for future in as_completed(future_map):
            stock = future_map[future]
            try:
                details = future.result()
            except Exception:
                continue
            stock.update(details)

    return stocks


def save_stocks(stocks: list[dict[str, str]]) -> None:
    synced_at = utc_now_iso()
    source = stocks[0]["source"] if stocks else ""
    stock_codes = [stock["code"] for stock in stocks]
    with sqlite3.connect(DB_PATH) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute("DELETE FROM st_stocks")
            cursor.executemany(
                """
                INSERT INTO st_stocks (
                    code, name, category, source, total_market_cap, float_market_cap,
                    shareholder_equity_total, balance_sheet_report_date,
                    shareholder_count, shareholder_date, industry, concepts, fetched_at
                )
                VALUES (
                    :code, :name, :category, :source, :total_market_cap, :float_market_cap,
                    :shareholder_equity_total, :balance_sheet_report_date,
                    :shareholder_count, :shareholder_date, :industry, :concepts, :fetched_at
                )
                """,
                stocks,
            )
            cursor.execute(
                """
                INSERT INTO sync_runs (synced_at, source, total_count)
                VALUES (?, ?, ?)
                """,
                (synced_at, source, len(stocks)),
            )
            if stock_codes:
                placeholders = ",".join("?" for _ in stock_codes)
                cursor.execute(
                    f"DELETE FROM stock_tags WHERE security NOT IN ({placeholders})",
                    stock_codes,
                )
            else:
                cursor.execute("DELETE FROM stock_tags")
        conn.commit()


def sync_stocks() -> dict[str, object]:
    stocks, source = fetch_st_stocks()
    enrich_stocks(stocks)
    save_stocks(stocks)
    return {
        "synced_at": utc_now_iso(),
        "source": source,
        "total_count": len(stocks),
    }


def load_stocks(category: str | None = None, tag_id: int | None = None) -> list[dict[str, str]]:
    query = """
        SELECT
            s.code,
            s.name,
            s.category,
            s.source,
            s.total_market_cap,
            s.float_market_cap,
            s.shareholder_equity_total,
            s.balance_sheet_report_date,
            s.shareholder_count,
            s.shareholder_date,
            s.industry,
            s.concepts,
            s.fetched_at,
            COALESCE(a.notice_count, 0) AS notice_count,
            COALESCE(tags.tag_names, '') AS tag_names,
            COALESCE(tags.tag_ids, '') AS tag_ids
        FROM st_stocks s
        LEFT JOIN (
            SELECT security, COUNT(*) AS notice_count
            FROM announcements
            GROUP BY security
        ) a
        ON a.security = s.code
        LEFT JOIN (
            SELECT
                st.security,
                GROUP_CONCAT(t.name, ' | ') AS tag_names,
                GROUP_CONCAT(CAST(t.id AS TEXT), ',') AS tag_ids
            FROM stock_tags st
            JOIN tags t
            ON t.id = st.tag_id
            GROUP BY st.security
        ) tags
        ON tags.security = s.code
    """
    where_clauses: list[str] = []
    params: list[object] = []
    if category in {"ST", "*ST"}:
        where_clauses.append("s.category = ?")
        params.append(category)
    if tag_id is not None:
        where_clauses.append(
            "EXISTS (SELECT 1 FROM stock_tags stf WHERE stf.security = s.code AND stf.tag_id = ?)"
        )
        params.append(tag_id)
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)
    query += " ORDER BY category DESC, code ASC"

    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(query, params).fetchall()

    result: list[dict[str, object]] = []
    for row in rows:
        item = dict(row)
        tag_names = [name for name in str(item.pop("tag_names", "")).split(" | ") if name]
        tag_ids = [
            int(part)
            for part in str(item.pop("tag_ids", "")).split(",")
            if part.strip()
        ]
        item["tags"] = tag_names
        item["tag_ids"] = tag_ids
        result.append(item)
    return result


def get_sync_summary() -> dict[str, object]:
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        latest = conn.execute(
            """
            SELECT synced_at, source, total_count
            FROM sync_runs
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
        counts = conn.execute(
            """
            SELECT category, COUNT(*) AS count
            FROM st_stocks
            GROUP BY category
            """
        ).fetchall()
        tag_count = conn.execute("SELECT COUNT(*) FROM tags").fetchone()[0]

    by_category = {row["category"]: row["count"] for row in counts}
    return {
        "synced_at": latest["synced_at"] if latest else None,
        "source": latest["source"] if latest else None,
        "total_count": latest["total_count"] if latest else 0,
        "st_count": by_category.get("ST", 0),
        "star_st_count": by_category.get("*ST", 0),
        "tag_count": tag_count,
    }


INDEX_HTML = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>ST / *ST 股票清单</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f5f7fb;
      --panel: #ffffff;
      --line: #d9e0ea;
      --text: #16202a;
      --muted: #64748b;
      --accent: #0f766e;
      --accent-soft: #ccfbf1;
      --warn: #b91c1c;
      --warn-soft: #fee2e2;
    }

    * { box-sizing: border-box; }

    body {
      margin: 0;
      font-family: "Segoe UI", "PingFang SC", "Microsoft YaHei", sans-serif;
      background: var(--bg);
      color: var(--text);
    }

    main {
      width: 100%;
      max-width: none;
      margin: 0 auto;
      padding: 24px 24px 40px;
    }

    h1 {
      margin: 0 0 10px;
      font-size: 32px;
      line-height: 1.2;
    }

    p {
      margin: 0;
      color: var(--muted);
    }

    .toolbar,
    .stats,
    .table-wrap,
    .modal-card {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
    }

    .toolbar {
      margin-top: 24px;
      padding: 16px;
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
      align-items: center;
      justify-content: space-between;
    }

    .tag-panel-header,
    .tag-panel-body,
    .stock-tag-editor {
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
      align-items: center;
    }

    .tag-list {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
    }

    .tag-chip {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      min-height: 30px;
      padding: 4px 10px;
      border: 1px solid var(--line);
      border-radius: 999px;
      background: #f8fafc;
      font-size: 13px;
    }

    .tag-chip strong {
      font-weight: 600;
    }

    .tag-chip button,
    .stock-tag-button,
    .tag-cell button {
      height: 30px;
      padding: 0 10px;
      font-size: 12px;
    }

    .select-cell {
      width: 52px;
      text-align: center;
    }

    .tag-cell {
      display: flex;
      flex-wrap: wrap;
      gap: 6px;
      align-items: center;
    }

    .tag-pill {
      display: inline-flex;
      align-items: center;
      height: 24px;
      padding: 0 8px;
      border-radius: 999px;
      background: #e2e8f0;
      color: #334155;
      font-size: 12px;
    }

    .checkbox-list {
      display: grid;
      gap: 10px;
      width: 100%;
    }

    .checkbox-item {
      display: flex;
      align-items: center;
      gap: 8px;
      font-size: 14px;
    }

    .toolbar-left,
    .toolbar-right {
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
      align-items: center;
    }

    button,
    select,
    input {
      height: 40px;
      border-radius: 8px;
      border: 1px solid var(--line);
      background: #fff;
      padding: 0 14px;
      font: inherit;
      color: inherit;
    }

    button {
      background: var(--accent);
      border-color: var(--accent);
      color: #fff;
      cursor: pointer;
    }

    button:disabled {
      opacity: 0.6;
      cursor: wait;
    }

    .stats {
      margin-top: 16px;
      padding: 16px;
      display: grid;
      gap: 12px;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
    }

    .stat {
      padding: 12px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #fbfdff;
    }

    .stat strong {
      display: block;
      font-size: 24px;
      line-height: 1.2;
      margin-top: 8px;
    }

    .table-wrap {
      margin-top: 16px;
      overflow: auto;
    }

    .table-footer {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 12px;
      padding: 12px 16px;
      border-top: 1px solid var(--line);
      color: var(--muted);
      font-size: 14px;
    }

    .pager {
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
      align-items: center;
    }

    .notice-count-btn {
      min-width: 72px;
      background: #fff;
      color: var(--accent);
      border-color: var(--line);
    }

    .notice-count-btn:hover {
      border-color: var(--accent);
    }

    .notice-summary {
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
      color: var(--muted);
      font-size: 14px;
    }

    .modal {
      position: fixed;
      inset: 0;
      background: rgba(15, 23, 42, 0.28);
      display: none;
      align-items: center;
      justify-content: center;
      padding: 24px;
      z-index: 20;
    }

    .modal.open {
      display: flex;
    }

    .modal-card {
      width: min(1400px, 100%);
      max-height: min(90vh, 920px);
      display: flex;
      flex-direction: column;
      overflow: hidden;
    }

    .modal-header,
    .modal-toolbar,
    .modal-footer {
      padding: 14px 16px;
      border-bottom: 1px solid var(--line);
    }

    .modal-header,
    .modal-footer {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 12px;
    }

    .modal-toolbar {
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
      align-items: center;
    }

    .modal-body {
      padding: 16px;
      overflow: auto;
      background: #f8fafc;
    }

    .notice-list {
      display: grid;
      gap: 10px;
    }

    .notice-item {
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 12px 14px;
      background: #fbfdff;
    }

    .notice-item h3 {
      margin: 0 0 8px;
      font-size: 15px;
      line-height: 1.4;
    }

    .notice-meta,
    .notice-links {
      display: flex;
      flex-wrap: wrap;
      gap: 10px 16px;
      font-size: 13px;
      color: var(--muted);
    }

    .notice-links a {
      color: var(--accent);
      text-decoration: none;
    }

    .notice-links a:hover {
      text-decoration: underline;
    }

    .notice-excerpt {
      margin-top: 10px;
      padding: 10px 12px;
      border-radius: 6px;
      background: #f1f5f9;
      color: var(--text);
      font-size: 13px;
      line-height: 1.6;
      white-space: pre-wrap;
      word-break: break-word;
    }

    table {
      width: 100%;
      min-width: 1800px;
      border-collapse: collapse;
    }

    th,
    td {
      text-align: left;
      padding: 12px 14px;
      border-bottom: 1px solid var(--line);
      font-size: 14px;
    }

    th {
      background: #f8fafc;
      color: var(--muted);
      font-weight: 600;
      cursor: pointer;
      user-select: none;
    }

    th.sortable.active {
      color: var(--text);
    }

    .search-input {
      min-width: 260px;
      width: min(360px, 100%);
    }

    tr:last-child td {
      border-bottom: none;
    }

    .badge {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-width: 52px;
      height: 28px;
      padding: 0 10px;
      border-radius: 999px;
      font-weight: 600;
    }

    .badge.st {
      background: var(--accent-soft);
      color: var(--accent);
    }

    .badge.star {
      background: var(--warn-soft);
      color: var(--warn);
    }

    .status {
      min-height: 20px;
      color: var(--muted);
      font-size: 14px;
    }

    .status.error {
      color: var(--warn);
    }

    @media (max-width: 680px) {
      h1 { font-size: 26px; }
      th, td { padding: 10px 12px; }
      main { padding: 16px 12px 28px; }
    }
  </style>
</head>
<body>
  <main>
    <h1>ST / *ST 股票清单</h1>
    <p>先同步 ST / *ST 清单，再补充总市值、流通市值、股东权益合计、最新股东人数、行业和所属概念，全部落到本地 SQLite 后展示。</p>

    <section class="toolbar">
      <div class="toolbar-left">
        <button id="refreshButton" type="button">刷新本地数据库</button>
        <button id="batchNoticeOcrButton" type="button">批量下载近两年公告并 OCR (0)</button>
        <button id="openAllNoticesButton" type="button">全部公告检索</button>
        <button id="ocrAllNoticesButton" type="button">OCR 全部公告</button>
        <button id="openTagManagerButton" type="button">标签设置</button>
        <select id="categoryFilter" aria-label="分类筛选">
          <option value="">全部</option>
          <option value="ST">仅 ST</option>
          <option value="*ST">仅 *ST</option>
        </select>
        <select id="tagFilter" aria-label="标签筛选">
          <option value="">全部标签</option>
        </select>
        <input id="searchInput" class="search-input" type="text" placeholder="搜索代码、名称、行业、概念" aria-label="搜索">
      </div>
      <div class="toolbar-right">
        <div id="status" class="status"></div>
      </div>
    </section>

    <section class="stats" id="stats"></section>

    <section class="table-wrap">
      <table>
        <thead>
          <tr>
            <th class="select-cell"><input id="selectPageCheckbox" type="checkbox" aria-label="选择当前页"></th>
            <th class="sortable" data-key="code" data-type="string">代码</th>
            <th class="sortable" data-key="name" data-type="string">名称</th>
            <th class="sortable" data-key="category" data-type="string">分类</th>
            <th class="sortable" data-key="source" data-type="string">来源</th>
            <th class="sortable" data-key="total_market_cap" data-type="number">总市值(亿)</th>
            <th class="sortable" data-key="float_market_cap" data-type="number">流通市值(亿)</th>
            <th class="sortable" data-key="shareholder_equity_total" data-type="number">股东权益合计(亿)</th>
            <th class="sortable" data-key="balance_sheet_report_date" data-type="string">最新财报期</th>
            <th class="sortable" data-key="shareholder_count" data-type="number">最新股东人数(户)</th>
            <th class="sortable" data-key="shareholder_date" data-type="string">股东人数日期</th>
            <th class="sortable" data-key="notice_count" data-type="number">公告数量(条)</th>
            <th data-key="tags" data-type="string">标签</th>
            <th class="sortable" data-key="industry" data-type="string">行业</th>
            <th class="sortable" data-key="concepts" data-type="string">所属概念</th>
            <th class="sortable" data-key="fetched_at" data-type="string">抓取时间</th>
          </tr>
        </thead>
        <tbody id="stockTableBody"></tbody>
      </table>
      <div class="table-footer">
        <div id="tableSummary">-</div>
        <div class="pager">
          <button id="prevPageButton" type="button">上一页</button>
          <span id="pageInfo">-</span>
          <button id="nextPageButton" type="button">下一页</button>
        </div>
      </div>
    </section>
  </main>

  <div id="noticeModal" class="modal" aria-hidden="true">
    <div class="modal-card">
      <div class="modal-header">
        <div>
          <h2 id="noticeModalTitle" style="margin: 0; font-size: 22px;">公告列表</h2>
          <div id="noticeSummary" class="notice-summary" style="margin-top: 6px;"></div>
        </div>
        <button id="closeNoticeModalButton" type="button">关闭</button>
      </div>
      <div class="modal-toolbar">
        <input id="noticeSearchInput" class="search-input" type="text" placeholder="搜索公告标题、类型、正文" aria-label="公告搜索">
        <input id="noticeDateFromInput" type="date" aria-label="公告开始日期">
        <input id="noticeDateToInput" type="date" aria-label="公告结束日期">
        <button id="ocrCurrentNoticesButton" type="button">OCR 当前范围</button>
        <button id="clearNoticeFiltersButton" type="button">清空筛选</button>
      </div>
      <div class="modal-body">
        <div id="noticeList" class="notice-list"></div>
      </div>
      <div class="modal-footer">
        <div id="noticePageInfo">-</div>
        <div class="pager">
          <button id="noticePrevPageButton" type="button">上一页</button>
          <button id="noticeNextPageButton" type="button">下一页</button>
        </div>
      </div>
    </div>
  </div>

  <div id="stockTagModal" class="modal" aria-hidden="true">
    <div class="modal-card" style="width:min(560px,100%);max-height:min(80vh,760px);">
      <div class="modal-header">
        <div>
          <h2 id="stockTagModalTitle" style="margin: 0; font-size: 22px;">编辑股票标签</h2>
          <div id="stockTagModalSummary" class="notice-summary" style="margin-top: 6px;"></div>
        </div>
        <button id="closeStockTagModalButton" type="button">关闭</button>
      </div>
      <div class="modal-body">
        <div id="stockTagCheckboxList" class="checkbox-list"></div>
      </div>
      <div class="modal-footer">
        <div id="stockTagModalStatus" class="status"></div>
        <div class="pager">
          <button id="saveStockTagsButton" type="button">保存标签</button>
        </div>
      </div>
    </div>
  </div>

  <div id="tagManagerModal" class="modal" aria-hidden="true">
    <div class="modal-card" style="width:min(760px,100%);max-height:min(82vh,780px);">
      <div class="modal-header">
        <div>
          <h2 style="margin: 0; font-size: 22px;">标签管理</h2>
          <div class="notice-summary" style="margin-top: 6px;">
            <span>添加、修改、删除标签</span>
          </div>
        </div>
        <button id="closeTagManagerButton" type="button">关闭</button>
      </div>
      <div class="modal-toolbar">
        <input id="tagNameInput" type="text" placeholder="输入新标签名称" aria-label="新标签名称">
        <button id="createTagButton" type="button">添加标签</button>
      </div>
      <div class="modal-body">
        <div id="tagList" class="tag-list"></div>
      </div>
    </div>
  </div>

  <script>
    const statusEl = document.getElementById("status");
    const statsEl = document.getElementById("stats");
    const bodyEl = document.getElementById("stockTableBody");
    const filterEl = document.getElementById("categoryFilter");
    const searchEl = document.getElementById("searchInput");
    const refreshButton = document.getElementById("refreshButton");
    const batchNoticeOcrButton = document.getElementById("batchNoticeOcrButton");
    const openAllNoticesButton = document.getElementById("openAllNoticesButton");
    const ocrAllNoticesButton = document.getElementById("ocrAllNoticesButton");
    const openTagManagerButton = document.getElementById("openTagManagerButton");
    const tagFilterEl = document.getElementById("tagFilter");
    const tagNameInput = document.getElementById("tagNameInput");
    const createTagButton = document.getElementById("createTagButton");
    const tagListEl = document.getElementById("tagList");
    const prevPageButton = document.getElementById("prevPageButton");
    const nextPageButton = document.getElementById("nextPageButton");
    const pageInfoEl = document.getElementById("pageInfo");
    const tableSummaryEl = document.getElementById("tableSummary");
    const noticeModalEl = document.getElementById("noticeModal");
    const noticeModalTitleEl = document.getElementById("noticeModalTitle");
    const closeNoticeModalButton = document.getElementById("closeNoticeModalButton");
    const noticeSummaryEl = document.getElementById("noticeSummary");
    const noticeListEl = document.getElementById("noticeList");
    const noticeSearchInput = document.getElementById("noticeSearchInput");
    const noticeDateFromInput = document.getElementById("noticeDateFromInput");
    const noticeDateToInput = document.getElementById("noticeDateToInput");
    const ocrCurrentNoticesButton = document.getElementById("ocrCurrentNoticesButton");
    const clearNoticeFiltersButton = document.getElementById("clearNoticeFiltersButton");
    const noticePrevPageButton = document.getElementById("noticePrevPageButton");
    const noticeNextPageButton = document.getElementById("noticeNextPageButton");
    const noticePageInfoEl = document.getElementById("noticePageInfo");
    const tagManagerModalEl = document.getElementById("tagManagerModal");
    const closeTagManagerButton = document.getElementById("closeTagManagerButton");
    const stockTagModalEl = document.getElementById("stockTagModal");
    const stockTagModalTitleEl = document.getElementById("stockTagModalTitle");
    const stockTagModalSummaryEl = document.getElementById("stockTagModalSummary");
    const stockTagCheckboxListEl = document.getElementById("stockTagCheckboxList");
    const stockTagModalStatusEl = document.getElementById("stockTagModalStatus");
    const closeStockTagModalButton = document.getElementById("closeStockTagModalButton");
    const saveStockTagsButton = document.getElementById("saveStockTagsButton");
    const selectPageCheckbox = document.getElementById("selectPageCheckbox");
    const sortableHeaders = Array.from(document.querySelectorAll("th.sortable"));
    let allRows = [];
    let allTags = [];
    const selectedStockCodes = new Set();
    let sortState = { key: "category", direction: "desc", type: "string" };
    let filteredRows = [];
    let currentPage = 1;
    const pageSize = 20;
    const stockTagState = {
      security: null,
      stockName: "",
      tagIds: [],
    };
    const noticeState = {
      security: null,
      stockName: "",
      isGlobal: false,
      page: 1,
      pageSize: 10,
      keyword: "",
      dateFrom: "",
      dateTo: "",
      totalPages: 1,
    };

    function setStatus(message, isError = false) {
      statusEl.textContent = message;
      statusEl.className = isError ? "status error" : "status";
    }

    function formatDate(value) {
      if (!value) return "未同步";
      const date = new Date(value);
      if (Number.isNaN(date.getTime())) return value;
      return date.toLocaleString("zh-CN", { hour12: false });
    }

    function formatMarketCap(value) {
      if (value === null || value === undefined || value === "") return "-";
      const number = Number(value);
      if (!Number.isFinite(number)) return value;
      return `${(number / 10000).toFixed(2)} 亿`;
    }

    function formatEquity(value) {
      if (value === null || value === undefined || value === "") return "-";
      const number = Number(value);
      if (!Number.isFinite(number)) return value;
      return `${(number / 100000000).toFixed(2)} 亿`;
    }

    function formatCount(value) {
      if (value === null || value === undefined || value === "") return "-";
      const number = Number(value);
      if (!Number.isFinite(number)) return value;
      return number.toLocaleString("zh-CN");
    }

    function formatFileSize(value) {
      if (value === null || value === undefined || value === "") return "-";
      const number = Number(value);
      if (!Number.isFinite(number)) return value;
      return `${(number / 1024 / 1024).toFixed(2)} MB`;
    }

    function escapeHtml(value) {
      return (value ?? "").toString()
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#39;");
    }

    function compareValues(left, right, type) {
      if (type === "number") {
        const a = left === null || left === undefined || left === "" ? Number.NEGATIVE_INFINITY : Number(left);
        const b = right === null || right === undefined || right === "" ? Number.NEGATIVE_INFINITY : Number(right);
        return a - b;
      }
      const a = (left ?? "").toString();
      const b = (right ?? "").toString();
      return a.localeCompare(b, "zh-CN");
    }

    function updateHeaderState() {
      sortableHeaders.forEach((header) => {
        const isActive = header.dataset.key === sortState.key;
        header.classList.toggle("active", isActive);
        const arrow = isActive ? (sortState.direction === "asc" ? " ▲" : " ▼") : "";
        header.textContent = `${header.dataset.label || header.textContent.replace(/[ ▲▼]+$/, "")}${arrow}`;
      });
    }

    function renderTagFilterOptions() {
      const current = tagFilterEl.value;
      tagFilterEl.innerHTML = [
        `<option value="">全部标签</option>`,
        ...allTags.map((tag) => `<option value="${tag.id}">${escapeHtml(tag.name)}</option>`),
      ].join("");
      tagFilterEl.value = allTags.some((tag) => String(tag.id) === current) ? current : "";
    }

    function updateBatchSelectionUi() {
      batchNoticeOcrButton.textContent = `批量下载近两年公告并 OCR (${selectedStockCodes.size})`;
      batchNoticeOcrButton.disabled = selectedStockCodes.size === 0;
    }

    function getCurrentPageRows() {
      const total = filteredRows.length;
      const totalPages = Math.max(1, Math.ceil(total / pageSize));
      const safePage = Math.min(currentPage, totalPages);
      const start = (safePage - 1) * pageSize;
      const end = start + pageSize;
      return filteredRows.slice(start, end);
    }

    function updateSelectPageCheckbox() {
      const rows = getCurrentPageRows();
      if (!rows.length) {
        selectPageCheckbox.checked = false;
        selectPageCheckbox.indeterminate = false;
        selectPageCheckbox.disabled = true;
        return;
      }
      selectPageCheckbox.disabled = false;
      const selectedCount = rows.filter((row) => selectedStockCodes.has(row.code)).length;
      selectPageCheckbox.checked = selectedCount === rows.length;
      selectPageCheckbox.indeterminate = selectedCount > 0 && selectedCount < rows.length;
    }

    function renderTagManager() {
      if (!allTags.length) {
        tagListEl.innerHTML = `<span class="tag-chip">还没有标签</span>`;
        return;
      }
      tagListEl.innerHTML = allTags.map((tag) => `
        <span class="tag-chip">
          <strong>${escapeHtml(tag.name)}</strong>
          <span>${tag.stock_count || 0} 只</span>
          <button type="button" data-tag-edit="${tag.id}">改名</button>
          <button type="button" data-tag-delete="${tag.id}">删除</button>
        </span>
      `).join("");

      tagListEl.querySelectorAll("[data-tag-edit]").forEach((button) => {
        button.addEventListener("click", async () => {
          const tag = allTags.find((item) => String(item.id) === button.dataset.tagEdit);
          if (!tag) return;
          const nextName = window.prompt("修改标签名称", tag.name);
          if (nextName === null) return;
          await saveTag(tag.id, nextName);
        });
      });

      tagListEl.querySelectorAll("[data-tag-delete]").forEach((button) => {
        button.addEventListener("click", async () => {
          const tag = allTags.find((item) => String(item.id) === button.dataset.tagDelete);
          if (!tag) return;
          if (!window.confirm(`确认删除标签“${tag.name}”吗？`)) return;
          await deleteTagById(tag.id);
        });
      });
    }

    function openTagManagerModal() {
      tagManagerModalEl.classList.add("open");
      tagManagerModalEl.setAttribute("aria-hidden", "false");
      tagNameInput.focus();
    }

    function closeTagManagerModal() {
      tagManagerModalEl.classList.remove("open");
      tagManagerModalEl.setAttribute("aria-hidden", "true");
    }

    function openStockTagModal(row) {
      stockTagState.security = row.code;
      stockTagState.stockName = row.name;
      stockTagState.tagIds = Array.isArray(row.tag_ids) ? [...row.tag_ids] : [];
      stockTagModalTitleEl.textContent = `${row.name} (${row.code}) 标签`;
      stockTagModalSummaryEl.textContent = `已选 ${stockTagState.tagIds.length} 个标签`;
      stockTagModalStatusEl.textContent = "";
      renderStockTagCheckboxes();
      stockTagModalEl.classList.add("open");
      stockTagModalEl.setAttribute("aria-hidden", "false");
    }

    function closeStockTagModal() {
      stockTagModalEl.classList.remove("open");
      stockTagModalEl.setAttribute("aria-hidden", "true");
    }

    function renderStockTagCheckboxes() {
      if (!allTags.length) {
        stockTagCheckboxListEl.innerHTML = `<div class="notice-item">还没有标签，请先在上面添加标签。</div>`;
        return;
      }
      stockTagCheckboxListEl.innerHTML = allTags.map((tag) => `
        <label class="checkbox-item">
          <input type="checkbox" value="${tag.id}" ${stockTagState.tagIds.includes(tag.id) ? "checked" : ""}>
          <span>${escapeHtml(tag.name)} (${tag.stock_count || 0} 只)</span>
        </label>
      `).join("");
    }

    function applyFiltersAndSort() {
      const keyword = searchEl.value.trim().toLowerCase();
      const selectedTagId = tagFilterEl.value;
      filteredRows = allRows.filter((row) => {
        if (selectedTagId && !((row.tag_ids || []).map(String).includes(selectedTagId))) {
          return false;
        }
        if (!keyword) return true;
        return [
          row.code,
          row.name,
          (row.tags || []).join(" "),
          row.industry,
          row.concepts,
          row.source,
          row.shareholder_date,
          row.balance_sheet_report_date,
        ]
          .filter(Boolean)
          .some((value) => value.toString().toLowerCase().includes(keyword));
      });

      filteredRows.sort((a, b) => {
        const result = compareValues(a[sortState.key], b[sortState.key], sortState.type);
        return sortState.direction === "asc" ? result : -result;
      });

      currentPage = 1;
      renderStockPage();
      updateHeaderState();
    }

    function renderStats(summary) {
      const cards = [
        ["总数", summary.total_count ?? 0],
        ["ST", summary.st_count ?? 0],
        ["*ST", summary.star_st_count ?? 0],
        ["标签", summary.tag_count ?? 0],
        ["最近同步", formatDate(summary.synced_at)],
        ["数据源", summary.source || "未同步"],
      ];

      statsEl.innerHTML = cards.map(([label, value]) => `
        <div class="stat">
          <span>${label}</span>
          <strong>${value}</strong>
        </div>
      `).join("");
    }

    function renderRows(rows) {
      if (!rows.length) {
        bodyEl.innerHTML = `
          <tr>
            <td colspan="16">本地数据库里还没有数据，先点一下“刷新本地数据库”。</td>
          </tr>
        `;
        updateSelectPageCheckbox();
        return;
      }

      bodyEl.innerHTML = rows.map((row) => {
        const badgeClass = row.category === "*ST" ? "badge star" : "badge st";
        return `
          <tr>
            <td class="select-cell"><input type="checkbox" class="stock-select-checkbox" data-stock-select="${row.code}" ${selectedStockCodes.has(row.code) ? "checked" : ""} aria-label="选择 ${row.code}"></td>
            <td>${row.code}</td>
            <td>${row.name}</td>
            <td><span class="${badgeClass}">${row.category}</span></td>
            <td>${row.source || "-"}</td>
            <td>${formatMarketCap(row.total_market_cap)}</td>
            <td>${formatMarketCap(row.float_market_cap)}</td>
            <td>${formatEquity(row.shareholder_equity_total)}</td>
            <td>${row.balance_sheet_report_date || "-"}</td>
            <td>${formatCount(row.shareholder_count)}</td>
            <td>${row.shareholder_date || "-"}</td>
            <td><button type="button" class="notice-count-btn" data-security="${row.code}" data-name="${row.name}">${formatCount(row.notice_count)}</button></td>
            <td>
              <div class="tag-cell">
                ${(row.tags || []).map((tag) => `<span class="tag-pill">${escapeHtml(tag)}</span>`).join("") || `<span class="tag-pill">无标签</span>`}
                <button type="button" class="stock-tag-button" data-stock-tag="${row.code}">编辑标签</button>
              </div>
            </td>
            <td>${row.industry || "-"}</td>
            <td>${row.concepts || "-"}</td>
            <td>${formatDate(row.fetched_at)}</td>
          </tr>
        `;
      }).join("");

      bodyEl.querySelectorAll(".notice-count-btn").forEach((button) => {
        button.addEventListener("click", () => openNoticeModal(button.dataset.security, button.dataset.name));
      });
      bodyEl.querySelectorAll(".stock-tag-button").forEach((button) => {
        button.addEventListener("click", () => {
          const row = allRows.find((item) => item.code === button.dataset.stockTag);
          if (row) openStockTagModal(row);
        });
      });
      bodyEl.querySelectorAll(".stock-select-checkbox").forEach((checkbox) => {
        checkbox.addEventListener("change", () => {
          if (checkbox.checked) {
            selectedStockCodes.add(checkbox.dataset.stockSelect);
          } else {
            selectedStockCodes.delete(checkbox.dataset.stockSelect);
          }
          updateBatchSelectionUi();
          updateSelectPageCheckbox();
        });
      });
      updateSelectPageCheckbox();
    }

    function renderStockPage() {
      const total = filteredRows.length;
      const totalPages = Math.max(1, Math.ceil(total / pageSize));
      currentPage = Math.min(currentPage, totalPages);
      const start = (currentPage - 1) * pageSize;
      const end = start + pageSize;
      renderRows(filteredRows.slice(start, end));
      tableSummaryEl.textContent = `共 ${total} 条，当前第 ${currentPage} / ${totalPages} 页`;
      pageInfoEl.textContent = `${currentPage} / ${totalPages}`;
      prevPageButton.disabled = currentPage <= 1;
      nextPageButton.disabled = currentPage >= totalPages;
      setStatus(`已加载 ${total} 条记录`);
      updateSelectPageCheckbox();
      updateBatchSelectionUi();
    }

    function renderNoticeSummary(summary, pagination) {
      noticeSummaryEl.innerHTML = [
        `范围：${summary.security === "ALL" ? "全部公告" : `证券代码 ${summary.security || "-"}`}`,
        `公告数：${summary.total_count ?? 0}`,
        `已下载 PDF：${summary.downloaded_count ?? 0}`,
        `已完成 OCR：${summary.ocr_done_count ?? 0}`,
        `OCR 失败：${summary.ocr_failed_count ?? 0}`,
        `最近同步：${formatDate(summary.last_fetched_at)}`,
        `页码：${pagination.page} / ${Math.max(pagination.total_pages, 1)}`,
      ].map((item) => `<span>${item}</span>`).join("");
    }

    function renderNoticeList(items) {
      if (!items.length) {
        noticeListEl.innerHTML = `<div class="notice-item">当前范围内还没有匹配的公告。</div>`;
        return;
      }

      noticeListEl.innerHTML = items.map((item) => `
        <article class="notice-item">
          <h3>${item.notice_title}</h3>
          <div class="notice-meta">
            <span>代码：${item.security}</span>
            <span>日期：${item.notice_date}</span>
            <span>类型：${item.notice_type || "-"}</span>
            <span>下载：${item.download_status}</span>
            <span>OCR：${item.ocr_status || "-"}</span>
            <span>来源：${item.ocr_source || "-"}</span>
            <span>大小：${formatFileSize(item.file_size)}</span>
          </div>
          <div class="notice-links" style="margin-top: 8px;">
            ${item.pdf_url ? `<a href="${item.pdf_url}" target="_blank" rel="noreferrer">PDF 链接</a>` : ""}
            ${item.local_pdf_path ? `<span>本地文件：${item.local_pdf_path}</span>` : ""}
          </div>
          ${item.ocr_text_preview ? `<div class="notice-excerpt">${item.ocr_text_preview}</div>` : ""}
          ${item.ocr_error ? `<div class="notice-excerpt" style="background:#fee2e2;color:#991b1b;">${item.ocr_error}</div>` : ""}
        </article>
      `).join("");
    }

    async function loadNotices() {
      if (!noticeState.security && !noticeState.isGlobal) return;
      try {
        const params = new URLSearchParams({
          page: String(noticeState.page),
          page_size: String(noticeState.pageSize),
        });
        if (!noticeState.isGlobal && noticeState.security) params.set("security", noticeState.security);
        if (noticeState.keyword) params.set("q", noticeState.keyword);
        if (noticeState.dateFrom) params.set("date_from", noticeState.dateFrom);
        if (noticeState.dateTo) params.set("date_to", noticeState.dateTo);
        const response = await fetch(`/api/notices?${params.toString()}`);
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        const payload = await response.json();
        noticeState.totalPages = Math.max(payload.pagination.total_pages || 1, 1);
        renderNoticeSummary(payload.summary, payload.pagination);
        renderNoticeList(payload.items);
        noticePageInfoEl.textContent = `第 ${payload.pagination.page} / ${Math.max(payload.pagination.total_pages, 1)} 页，共 ${payload.pagination.total} 条`;
        noticePrevPageButton.disabled = payload.pagination.page <= 1;
        noticeNextPageButton.disabled = payload.pagination.page >= Math.max(payload.pagination.total_pages, 1);
      } catch (error) {
        noticeListEl.innerHTML = `<div class="notice-item">公告加载失败：${error.message}</div>`;
      }
    }

    function openNoticeModal(security, stockName) {
      noticeState.security = security || null;
      noticeState.stockName = stockName || security;
      noticeState.isGlobal = !security;
      noticeState.page = 1;
      noticeState.keyword = "";
      noticeState.dateFrom = "";
      noticeState.dateTo = "";
      noticeSearchInput.value = "";
      noticeDateFromInput.value = "";
      noticeDateToInput.value = "";
      noticeModalTitleEl.textContent = security ? `${stockName || security} 公告列表` : "全部公告全文检索";
      noticeModalEl.classList.add("open");
      noticeModalEl.setAttribute("aria-hidden", "false");
      loadNotices();
    }

    async function runNoticeOcr(security = null) {
      const targetButton = security ? ocrCurrentNoticesButton : ocrAllNoticesButton;
      targetButton.disabled = true;
      setStatus(security ? `正在 OCR ${security} 公告...` : "正在 OCR 全部已下载公告...");
      try {
        const params = new URLSearchParams();
        if (security) params.set("security", security);
        const response = await fetch(`/api/notices/ocr?${params.toString()}`, { method: "POST" });
        const payload = await response.json();
        if (!response.ok) throw new Error(payload.error || `HTTP ${response.status}`);
        setStatus(`OCR 完成：处理 ${payload.processed} 条，成功 ${payload.success} 条，失败 ${payload.failed} 条`);
        if (noticeModalEl.classList.contains("open")) {
          await loadNotices();
        }
      } catch (error) {
        setStatus(`OCR 失败：${error.message}`, true);
      } finally {
        targetButton.disabled = false;
      }
    }

    async function runBatchNoticeOcr() {
      const securities = Array.from(selectedStockCodes);
      if (!securities.length) return;
      batchNoticeOcrButton.disabled = true;
      setStatus(`正在处理 ${securities.length} 只股票的近两年公告与 OCR，这一步会比较久...`);
      try {
        const response = await fetch("/api/notices/batch-sync-ocr", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ securities }),
        });
        const payload = await response.json();
        if (!response.ok) throw new Error(payload.error || `HTTP ${response.status}`);
        await loadData();
        if (noticeModalEl.classList.contains("open")) {
          await loadNotices();
        }
        setStatus(
          `批量完成：${payload.count} 只股票，下载 ${payload.downloaded} 份公告，OCR 成功 ${payload.ocr_success} 份，失败 ${payload.ocr_failed} 份`
        );
      } catch (error) {
        setStatus(`批量处理失败：${error.message}`, true);
      } finally {
        updateBatchSelectionUi();
      }
    }

    function closeNoticeModal() {
      noticeModalEl.classList.remove("open");
      noticeModalEl.setAttribute("aria-hidden", "true");
    }

    async function loadData() {
      const category = filterEl.value;
      const params = new URLSearchParams();
      if (category) params.set("category", category);
      if (tagFilterEl.value) params.set("tag_id", tagFilterEl.value);
      const url = params.toString() ? `/api/stocks?${params.toString()}` : "/api/stocks";
      setStatus("正在读取本地数据库...");

      try {
        const response = await fetch(url);
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        const payload = await response.json();
        renderStats(payload.summary);
        allRows = payload.items;
        applyFiltersAndSort();
      } catch (error) {
        allRows = [];
        renderRows([]);
        setStatus(`加载失败：${error.message}`, true);
      }
    }

    async function loadTags() {
      try {
        const response = await fetch("/api/tags");
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        const payload = await response.json();
        allTags = payload.items || [];
        renderTagFilterOptions();
        renderTagManager();
        renderStockTagCheckboxes();
      } catch (error) {
        allTags = [];
        renderTagFilterOptions();
        renderTagManager();
        setStatus(`标签加载失败：${error.message}`, true);
      }
    }

    async function saveTag(tagId, tagName) {
      const trimmed = (tagName || "").trim();
      if (!trimmed) return;
      const url = tagId ? "/api/tags/update" : "/api/tags";
      const body = JSON.stringify(tagId ? { id: tagId, name: trimmed } : { name: trimmed });
      const response = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body,
      });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.error || `HTTP ${response.status}`);
      tagNameInput.value = "";
      await loadTags();
      await loadData();
      setStatus(tagId ? "标签已更新" : "标签已添加");
    }

    async function deleteTagById(tagId) {
      const response = await fetch("/api/tags/delete", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ id: tagId }),
      });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.error || `HTTP ${response.status}`);
      await loadTags();
      await loadData();
      setStatus("标签已删除");
    }

    async function saveCurrentStockTags() {
      saveStockTagsButton.disabled = true;
      stockTagModalStatusEl.textContent = "正在保存...";
      const selectedIds = Array.from(stockTagCheckboxListEl.querySelectorAll("input[type=checkbox]:checked"))
        .map((input) => Number(input.value))
        .filter((value) => Number.isFinite(value));
      try {
        const response = await fetch("/api/stocks/tags", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ security: stockTagState.security, tag_ids: selectedIds }),
        });
        const payload = await response.json();
        if (!response.ok) throw new Error(payload.error || `HTTP ${response.status}`);
        stockTagState.tagIds = selectedIds;
        stockTagModalSummaryEl.textContent = `已选 ${selectedIds.length} 个标签`;
        stockTagModalStatusEl.textContent = "保存完成";
        await loadTags();
        await loadData();
      } catch (error) {
        stockTagModalStatusEl.textContent = `保存失败：${error.message}`;
      } finally {
        saveStockTagsButton.disabled = false;
      }
    }

    async function refreshData() {
      refreshButton.disabled = true;
      setStatus("正在通过 AkShare 刷新数据并补充附加信息，这一步会稍慢一些...");

      try {
        const response = await fetch("/api/refresh", { method: "POST" });
        const payload = await response.json();
        if (!response.ok) {
          await loadData();
          const suffix = payload.has_cached_data ? "，已保留当前本地缓存数据" : "";
          throw new Error((payload.error || `HTTP ${response.status}`) + suffix);
        }
        await loadData();
        setStatus(`刷新完成，本次写入 ${payload.total_count} 条记录，来源 ${payload.source}`);
      } catch (error) {
        setStatus(`刷新失败：${error.message}`, true);
      } finally {
        refreshButton.disabled = false;
      }
    }

    filterEl.addEventListener("change", loadData);
    tagFilterEl.addEventListener("change", loadData);
    searchEl.addEventListener("input", applyFiltersAndSort);
    selectPageCheckbox.addEventListener("change", () => {
      const rows = getCurrentPageRows();
      rows.forEach((row) => {
        if (selectPageCheckbox.checked) {
          selectedStockCodes.add(row.code);
        } else {
          selectedStockCodes.delete(row.code);
        }
      });
      renderStockPage();
    });
    createTagButton.addEventListener("click", async () => {
      try {
        await saveTag(null, tagNameInput.value);
      } catch (error) {
        setStatus(`添加标签失败：${error.message}`, true);
      }
    });
    tagNameInput.addEventListener("keydown", async (event) => {
      if (event.key !== "Enter") return;
      event.preventDefault();
      try {
        await saveTag(null, tagNameInput.value);
      } catch (error) {
        setStatus(`添加标签失败：${error.message}`, true);
      }
    });
    prevPageButton.addEventListener("click", () => {
      if (currentPage > 1) {
        currentPage -= 1;
        renderStockPage();
      }
    });
    nextPageButton.addEventListener("click", () => {
      const totalPages = Math.max(1, Math.ceil(filteredRows.length / pageSize));
      if (currentPage < totalPages) {
        currentPage += 1;
        renderStockPage();
      }
    });
    sortableHeaders.forEach((header) => {
      header.dataset.label = header.textContent;
      header.addEventListener("click", () => {
        const key = header.dataset.key;
        const type = header.dataset.type || "string";
        if (sortState.key === key) {
          sortState.direction = sortState.direction === "asc" ? "desc" : "asc";
        } else {
          sortState = { key, direction: type === "number" ? "desc" : "asc", type };
        }
        applyFiltersAndSort();
      });
    });
    refreshButton.addEventListener("click", refreshData);
    batchNoticeOcrButton.addEventListener("click", runBatchNoticeOcr);
    openAllNoticesButton.addEventListener("click", () => openNoticeModal(null, "全部公告"));
    ocrAllNoticesButton.addEventListener("click", () => runNoticeOcr(null));
    openTagManagerButton.addEventListener("click", openTagManagerModal);
    closeNoticeModalButton.addEventListener("click", closeNoticeModal);
    closeTagManagerButton.addEventListener("click", closeTagManagerModal);
    closeStockTagModalButton.addEventListener("click", closeStockTagModal);
    noticeModalEl.addEventListener("click", (event) => {
      if (event.target === noticeModalEl) closeNoticeModal();
    });
    tagManagerModalEl.addEventListener("click", (event) => {
      if (event.target === tagManagerModalEl) closeTagManagerModal();
    });
    stockTagModalEl.addEventListener("click", (event) => {
      if (event.target === stockTagModalEl) closeStockTagModal();
    });
    noticeSearchInput.addEventListener("input", () => {
      noticeState.keyword = noticeSearchInput.value.trim();
      noticeState.page = 1;
      loadNotices();
    });
    noticeDateFromInput.addEventListener("change", () => {
      noticeState.dateFrom = noticeDateFromInput.value;
      noticeState.page = 1;
      loadNotices();
    });
    noticeDateToInput.addEventListener("change", () => {
      noticeState.dateTo = noticeDateToInput.value;
      noticeState.page = 1;
      loadNotices();
    });
    clearNoticeFiltersButton.addEventListener("click", () => {
      noticeState.keyword = "";
      noticeState.dateFrom = "";
      noticeState.dateTo = "";
      noticeState.page = 1;
      noticeSearchInput.value = "";
      noticeDateFromInput.value = "";
      noticeDateToInput.value = "";
      loadNotices();
    });
    ocrCurrentNoticesButton.addEventListener("click", () => {
      if (!noticeState.isGlobal && noticeState.security) {
        runNoticeOcr(noticeState.security);
      } else {
        runNoticeOcr(null);
      }
    });
    noticePrevPageButton.addEventListener("click", () => {
      if (noticeState.page > 1) {
        noticeState.page -= 1;
        loadNotices();
      }
    });
    noticeNextPageButton.addEventListener("click", () => {
      if (noticeState.page < noticeState.totalPages) {
        noticeState.page += 1;
        loadNotices();
      }
    });
    saveStockTagsButton.addEventListener("click", saveCurrentStockTags);
    updateHeaderState();
    updateBatchSelectionUi();
    loadTags().then(loadData);
  </script>
</body>
</html>
"""


class StockRequestHandler(BaseHTTPRequestHandler):
    server_version = "STStockServer/1.0"

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/":
            self.respond_html(INDEX_HTML)
            return
        if parsed.path == "/api/stocks":
            query = parse_qs(parsed.query)
            category = query.get("category", [None])[0]
            tag_id_value = query.get("tag_id", [""])[0].strip()
            tag_id = int(tag_id_value) if tag_id_value else None
            payload = {
                "items": load_stocks(category=category, tag_id=tag_id),
                "summary": get_sync_summary(),
            }
            self.respond_json(payload)
            return
        if parsed.path == "/api/tags":
            self.respond_json({"items": load_tags()})
            return
        if parsed.path == "/api/notices":
            query = parse_qs(parsed.query)
            security = query.get("security", [""])[0].strip() or None
            keyword = query.get("q", [""])[0].strip() or None
            date_from = query.get("date_from", [""])[0].strip() or None
            date_to = query.get("date_to", [""])[0].strip() or None
            page = int(query.get("page", ["1"])[0] or "1")
            page_size = int(query.get("page_size", ["10"])[0] or "10")
            notice_payload = load_announcements(
                security,
                keyword=keyword,
                date_from=date_from,
                date_to=date_to,
                page=page,
                page_size=page_size,
            )
            payload = {
                "items": notice_payload["items"],
                "summary": get_announcement_summary(security),
                "pagination": notice_payload["pagination"],
            }
            self.respond_json(payload)
            return
        self.respond_json({"error": "Not Found"}, status=HTTPStatus.NOT_FOUND)

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/api/refresh":
            try:
                result = sync_stocks()
                self.respond_json(result, status=HTTPStatus.CREATED)
            except Exception as exc:  # pragma: no cover
                summary = get_sync_summary()
                self.respond_json(
                    {
                        "error": str(exc),
                        "summary": summary,
                        "has_cached_data": bool(summary.get("total_count")),
                    },
                    status=HTTPStatus.INTERNAL_SERVER_ERROR,
                )
            return
        if parsed.path == "/api/tags":
            try:
                payload = self.read_json_body()
                result = create_tag(str(payload.get("name") or ""))
                self.respond_json(result, status=HTTPStatus.CREATED)
            except Exception as exc:  # pragma: no cover
                self.respond_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        if parsed.path == "/api/tags/update":
            try:
                payload = self.read_json_body()
                result = update_tag(int(payload.get("id")), str(payload.get("name") or ""))
                self.respond_json(result)
            except Exception as exc:  # pragma: no cover
                self.respond_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        if parsed.path == "/api/tags/delete":
            try:
                payload = self.read_json_body()
                delete_tag(int(payload.get("id")))
                self.respond_json({"ok": True})
            except Exception as exc:  # pragma: no cover
                self.respond_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        if parsed.path == "/api/stocks/tags":
            try:
                payload = self.read_json_body()
                security = str(payload.get("security") or "").strip()
                tag_ids = payload.get("tag_ids") or []
                if not isinstance(tag_ids, list):
                    raise ValueError("tag_ids 必须是数组")
                result = replace_stock_tags(security, tag_ids)
                self.respond_json(result)
            except Exception as exc:  # pragma: no cover
                self.respond_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        if parsed.path == "/api/notices/sync":
            query = parse_qs(parsed.query)
            security = query.get("security", ["000078"])[0]
            try:
                result = fetch_and_download_notices(
                    security=security,
                    begin_date=two_years_ago_today(),
                    end_date=utc_today(),
                )
                self.respond_json(result, status=HTTPStatus.CREATED)
            except Exception as exc:  # pragma: no cover
                self.respond_json(
                    {"error": f"{exc.__class__.__name__}: {exc}"},
                    status=HTTPStatus.INTERNAL_SERVER_ERROR,
                )
            return
        if parsed.path == "/api/notices/ocr":
            query = parse_qs(parsed.query)
            security = query.get("security", [""])[0].strip() or None
            force = query.get("force", ["0"])[0].strip() in {"1", "true", "True"}
            try:
                result = sync_notice_ocr(security=security, force=force)
                self.respond_json(result, status=HTTPStatus.CREATED)
            except Exception as exc:  # pragma: no cover
                self.respond_json(
                    {"error": f"{exc.__class__.__name__}: {exc}"},
                    status=HTTPStatus.INTERNAL_SERVER_ERROR,
                )
            return
        if parsed.path == "/api/notices/batch-sync-ocr":
            try:
                payload = self.read_json_body()
                securities = payload.get("securities") or []
                if not isinstance(securities, list):
                    raise ValueError("securities 必须是数组")
                result = batch_sync_notices_and_ocr(securities)
                self.respond_json(result, status=HTTPStatus.CREATED)
            except Exception as exc:  # pragma: no cover
                self.respond_json(
                    {"error": f"{exc.__class__.__name__}: {exc}"},
                    status=HTTPStatus.BAD_REQUEST,
                )
            return
        self.respond_json({"error": "Not Found"}, status=HTTPStatus.NOT_FOUND)

    def log_message(self, format: str, *args: object) -> None:
        return

    def read_json_body(self) -> dict[str, object]:
        content_length = int(self.headers.get("Content-Length", "0") or "0")
        if content_length <= 0:
            return {}
        raw = self.rfile.read(content_length)
        if not raw:
            return {}
        payload = json.loads(raw.decode("utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("请求体必须是 JSON 对象")
        return payload

    def respond_html(self, content: str, status: HTTPStatus = HTTPStatus.OK) -> None:
        encoded = content.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def respond_json(self, payload: dict[str, object], status: HTTPStatus = HTTPStatus.OK) -> None:
        encoded = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)


def main() -> None:
    ensure_database()
    server = ThreadingHTTPServer((HOST, PORT), StockRequestHandler)
    print(f"Server running at http://{HOST}:{PORT}")
    server.serve_forever()


if __name__ == "__main__":
    main()
