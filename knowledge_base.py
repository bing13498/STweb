from __future__ import annotations

import hashlib
import sqlite3
from datetime import datetime, timezone
from pathlib import Path


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def normalize_whitespace(text: str) -> str:
    lines = []
    for raw_line in (text or "").replace("\r", "\n").split("\n"):
        line = " ".join(raw_line.split()).strip()
        if line:
            lines.append(line)
    return "\n".join(lines)


def split_text_into_chunks(
    text: str,
    *,
    max_chars: int = 700,
    overlap_chars: int = 120,
) -> list[str]:
    cleaned = normalize_whitespace(text)
    if not cleaned:
        return []

    paragraphs = [part.strip() for part in cleaned.split("\n") if part.strip()]
    chunks: list[str] = []
    current = ""

    def flush_current() -> None:
        nonlocal current
        if current:
            chunks.append(current.strip())
            current = ""

    for paragraph in paragraphs:
        if len(paragraph) <= max_chars:
            candidate = f"{current}\n{paragraph}".strip() if current else paragraph
            if len(candidate) <= max_chars:
                current = candidate
            else:
                flush_current()
                current = paragraph
            continue

        flush_current()
        start = 0
        step = max(max_chars - overlap_chars, 1)
        while start < len(paragraph):
            end = min(start + max_chars, len(paragraph))
            chunk = paragraph[start:end].strip()
            if chunk:
                chunks.append(chunk)
            if end >= len(paragraph):
                break
            start += step

    flush_current()
    return chunks


def upsert_parse_task(
    conn: sqlite3.Connection,
    *,
    announcement_id: int,
    security: str,
    task_type: str,
    status: str,
    error_message: str | None = None,
    started_at: str | None = None,
    finished_at: str | None = None,
) -> None:
    now = utc_now_iso()
    row = conn.execute(
        """
        SELECT id FROM announcement_parse_tasks
        WHERE announcement_id = ? AND task_type = ?
        """,
        (announcement_id, task_type),
    ).fetchone()
    if row:
        conn.execute(
            """
            UPDATE announcement_parse_tasks
            SET security = ?, status = ?, error_message = ?, started_at = ?, finished_at = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (security, status, error_message, started_at, finished_at, now, row[0]),
        )
    else:
        conn.execute(
            """
            INSERT INTO announcement_parse_tasks (
                announcement_id, security, task_type, status, error_message,
                started_at, finished_at, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                announcement_id,
                security,
                task_type,
                status,
                error_message,
                started_at,
                finished_at,
                now,
                now,
            ),
        )


def get_kb_summary(db_path: str | Path) -> dict[str, object]:
    with sqlite3.connect(db_path) as conn:
        announcement_count = conn.execute(
            "SELECT COUNT(*) FROM announcements WHERE COALESCE(ocr_text, '') <> ''"
        ).fetchone()[0]
        chunk_count = conn.execute(
            "SELECT COUNT(*) FROM announcement_chunks"
        ).fetchone()[0]
        parsed_count = conn.execute(
            """
            SELECT COUNT(DISTINCT announcement_id)
            FROM announcement_parse_tasks
            WHERE task_type = 'parse' AND status = 'done'
            """
        ).fetchone()[0]
        latest_chunk_at = conn.execute(
            "SELECT MAX(updated_at) FROM announcement_chunks"
        ).fetchone()[0]
    return {
        "announcements_with_text": announcement_count,
        "chunk_count": chunk_count,
        "parsed_announcement_count": parsed_count,
        "latest_chunked_at": latest_chunk_at,
    }


def rebuild_knowledge_base(
    db_path: str | Path,
    *,
    security: str | None = None,
    force: bool = False,
    limit: int | None = None,
) -> dict[str, object]:
    db_path = Path(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        params: list[object] = []
        where = [
            "COALESCE(a.ocr_text, '') <> ''",
            "COALESCE(a.ocr_status, 'pending') = 'done'",
        ]
        if security:
            where.append("a.security = ?")
            params.append(security)
        sql = f"""
            SELECT
                a.id,
                a.security,
                a.stock_name,
                a.notice_title,
                a.notice_type,
                a.notice_date,
                a.ocr_text,
                a.ocr_source
            FROM announcements a
            WHERE {' AND '.join(where)}
            ORDER BY a.notice_date DESC, a.id DESC
        """
        if limit is not None:
            sql += " LIMIT ?"
            params.append(max(int(limit), 1))
        announcements = conn.execute(sql, params).fetchall()

        processed = 0
        skipped = 0
        failed = 0
        chunk_count = 0

        for row in announcements:
            announcement_id = int(row["id"])
            stock_security = str(row["security"] or "")
            existing_count = conn.execute(
                "SELECT COUNT(*) FROM announcement_chunks WHERE announcement_id = ?",
                (announcement_id,),
            ).fetchone()[0]
            if existing_count and not force:
                skipped += 1
                continue

            started_at = utc_now_iso()
            upsert_parse_task(
                conn,
                announcement_id=announcement_id,
                security=stock_security,
                task_type="parse",
                status="running",
                started_at=started_at,
                finished_at=None,
            )

            try:
                chunks = split_text_into_chunks(str(row["ocr_text"] or ""))
                if force and existing_count:
                    conn.execute(
                        "DELETE FROM announcement_chunks WHERE announcement_id = ?",
                        (announcement_id,),
                    )
                now = utc_now_iso()
                for index, chunk_text in enumerate(chunks, start=1):
                    chunk_hash = hashlib.sha1(
                        f"{announcement_id}:{index}:{chunk_text}".encode("utf-8")
                    ).hexdigest()
                    conn.execute(
                        """
                        INSERT INTO announcement_chunks (
                            announcement_id, security, stock_name, notice_title, notice_type,
                            notice_date, chunk_index, chunk_text, chunk_hash, char_count,
                            parse_source, created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(announcement_id, chunk_index) DO UPDATE SET
                            security = excluded.security,
                            stock_name = excluded.stock_name,
                            notice_title = excluded.notice_title,
                            notice_type = excluded.notice_type,
                            notice_date = excluded.notice_date,
                            chunk_text = excluded.chunk_text,
                            chunk_hash = excluded.chunk_hash,
                            char_count = excluded.char_count,
                            parse_source = excluded.parse_source,
                            updated_at = excluded.updated_at
                        """,
                        (
                            announcement_id,
                            stock_security,
                            row["stock_name"],
                            row["notice_title"],
                            row["notice_type"],
                            row["notice_date"],
                            index,
                            chunk_text,
                            chunk_hash,
                            len(chunk_text),
                            row["ocr_source"] or "ocr",
                            now,
                            now,
                        ),
                    )

                upsert_parse_task(
                    conn,
                    announcement_id=announcement_id,
                    security=stock_security,
                    task_type="parse",
                    status="done",
                    error_message=None,
                    started_at=started_at,
                    finished_at=utc_now_iso(),
                )
                processed += 1
                chunk_count += len(chunks)
            except Exception as exc:
                failed += 1
                upsert_parse_task(
                    conn,
                    announcement_id=announcement_id,
                    security=stock_security,
                    task_type="parse",
                    status="failed",
                    error_message=f"{exc.__class__.__name__}: {exc}",
                    started_at=started_at,
                    finished_at=utc_now_iso(),
                )

        conn.commit()

    return {
        "ok": True,
        "security": security,
        "force": force,
        "processed": processed,
        "skipped": skipped,
        "failed": failed,
        "chunk_count": chunk_count,
        "summary": get_kb_summary(db_path),
    }
