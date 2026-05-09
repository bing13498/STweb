from __future__ import annotations

import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path


EVENT_RULES: list[dict[str, object]] = [
    {
        "event_type": "major_illegal_risk",
        "risk_level": "high",
        "subject": "重大违法风险",
        "keywords": ["重大违法", "涉嫌违法", "违法违规"],
    },
    {
        "event_type": "case_filing",
        "risk_level": "high",
        "subject": "立案调查",
        "keywords": ["立案调查", "被立案", "证监会立案", "涉嫌信息披露违法"],
    },
    {
        "event_type": "administrative_penalty",
        "risk_level": "high",
        "subject": "行政处罚",
        "keywords": ["行政处罚", "处罚决定", "罚款", "警告并罚款"],
    },
    {
        "event_type": "regulatory_letter",
        "risk_level": "medium",
        "subject": "监管函/问询函",
        "keywords": ["监管函", "问询函", "关注函", "工作函", "督促函"],
    },
    {
        "event_type": "capital_occupation",
        "risk_level": "high",
        "subject": "资金占用",
        "keywords": ["资金占用", "非经营性占用", "占用上市公司资金"],
    },
    {
        "event_type": "illegal_guarantee",
        "risk_level": "high",
        "subject": "违规担保/财务资助",
        "keywords": ["违规担保", "对外担保", "财务资助", "质押担保", "未及时履行", "审批程序"],
    },
    {
        "event_type": "internal_control_defect",
        "risk_level": "high",
        "subject": "内部控制缺陷",
        "keywords": ["内部控制缺陷", "内控缺陷", "否定意见", "内部控制审计报告"],
    },
    {
        "event_type": "non_standard_audit",
        "risk_level": "high",
        "subject": "非标审计意见",
        "keywords": ["保留意见", "无法表示意见", "否定意见", "非标准审计意见", "审计报告"],
    },
    {
        "event_type": "going_concern_uncertainty",
        "risk_level": "high",
        "subject": "持续经营重大不确定性",
        "keywords": ["持续经营", "重大不确定性", "偿债能力", "债务逾期"],
    },
    {
        "event_type": "restructuring_or_bankruptcy",
        "risk_level": "high",
        "subject": "重整/破产风险",
        "keywords": ["重整", "预重整", "破产", "破产清算"],
    },
]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip())


def build_excerpt(text: str, keyword: str, context: int = 90) -> str:
    cleaned = normalize_text(text)
    if not cleaned:
        return ""
    index = cleaned.find(keyword)
    if index < 0:
        return cleaned[: context * 2]
    start = max(index - context, 0)
    end = min(index + len(keyword) + context, len(cleaned))
    prefix = "..." if start > 0 else ""
    suffix = "..." if end < len(cleaned) else ""
    return f"{prefix}{cleaned[start:end]}{suffix}"


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
            SET security = ?, status = ?, error_message = ?, started_at = ?, finished_at = ?, updated_at = ?
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


def extract_events_from_text(text: str) -> list[dict[str, str]]:
    merged_text = normalize_text(text)
    events: list[dict[str, str]] = []
    if not merged_text:
        return events

    for rule in EVENT_RULES:
        for keyword in rule["keywords"]:
            if keyword in merged_text:
                events.append(
                    {
                        "event_type": str(rule["event_type"]),
                        "risk_level": str(rule["risk_level"]),
                        "subject": str(rule["subject"]),
                        "summary": f"命中事件规则：{rule['subject']}（关键词：{keyword}）",
                        "evidence_text": build_excerpt(merged_text, keyword),
                    }
                )
                break
    return events


def rebuild_announcement_events(
    db_path: str | Path,
    *,
    security: str | None = None,
    force: bool = False,
    limit: int | None = None,
) -> dict[str, object]:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        params: list[object] = []
        where = ["COALESCE(a.ocr_text, '') <> ''"]
        if security:
            where.append("a.security = ?")
            params.append(security)
        sql = f"""
            SELECT
                a.id,
                a.security,
                a.stock_name,
                a.notice_title,
                a.notice_date,
                a.notice_type,
                a.ocr_text
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
        total_events = 0

        for row in announcements:
            announcement_id = int(row["id"])
            stock_security = str(row["security"] or "")
            existing_count = conn.execute(
                "SELECT COUNT(*) FROM announcement_events WHERE announcement_id = ?",
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
                task_type="event_extract",
                status="running",
                started_at=started_at,
                finished_at=None,
            )

            try:
                text = f"{row['notice_title'] or ''}\n{row['ocr_text'] or ''}"
                events = extract_events_from_text(text)
                if force and existing_count:
                    conn.execute(
                        "DELETE FROM announcement_events WHERE announcement_id = ?",
                        (announcement_id,),
                    )
                now = utc_now_iso()
                for event in events:
                    conn.execute(
                        """
                        INSERT INTO announcement_events (
                            announcement_id, security, stock_name, event_type, risk_level,
                            event_date, subject, summary, evidence_text, created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            announcement_id,
                            stock_security,
                            row["stock_name"],
                            event["event_type"],
                            event["risk_level"],
                            row["notice_date"],
                            event["subject"],
                            event["summary"],
                            event["evidence_text"],
                            now,
                            now,
                        ),
                    )
                upsert_parse_task(
                    conn,
                    announcement_id=announcement_id,
                    security=stock_security,
                    task_type="event_extract",
                    status="done",
                    error_message=None,
                    started_at=started_at,
                    finished_at=utc_now_iso(),
                )
                processed += 1
                total_events += len(events)
            except Exception as exc:
                failed += 1
                upsert_parse_task(
                    conn,
                    announcement_id=announcement_id,
                    security=stock_security,
                    task_type="event_extract",
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
        "event_count": total_events,
    }


def load_announcement_events(
    db_path: str | Path,
    *,
    security: str | None = None,
    event_type: str | None = None,
    limit: int = 100,
) -> dict[str, object]:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        params: list[object] = []
        where = ["1 = 1"]
        if security:
            where.append("security = ?")
            params.append(security)
        if event_type:
            where.append("event_type = ?")
            params.append(event_type)
        rows = conn.execute(
            f"""
            SELECT
                id,
                announcement_id,
                security,
                stock_name,
                event_type,
                risk_level,
                event_date,
                subject,
                summary,
                evidence_text,
                created_at,
                updated_at
            FROM announcement_events
            WHERE {' AND '.join(where)}
            ORDER BY event_date DESC, id DESC
            LIMIT ?
            """,
            [*params, max(int(limit), 1)],
        ).fetchall()
    return {"items": [dict(row) for row in rows]}
