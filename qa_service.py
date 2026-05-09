from __future__ import annotations

import json
import re
import sqlite3
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path


RISK_KEYWORDS: dict[str, list[str]] = {
    "major_illegal_risk": [
        "重大违法",
        "违法",
        "违规",
        "处罚",
        "行政处罚",
        "立案",
        "调查",
        "监管",
        "监管函",
        "问询",
        "担保",
        "资金占用",
        "信息披露",
        "披露义务",
        "内控缺陷",
    ],
    "going_concern_risk": [
        "持续经营",
        "重大不确定性",
        "债务",
        "逾期",
        "重整",
        "预重整",
        "破产",
    ],
    "audit_risk": [
        "非标",
        "保留意见",
        "无法表示意见",
        "否定意见",
        "审计",
        "会计差错",
    ],
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def normalize_question(question: str) -> str:
    text = re.sub(r"\s+", " ", (question or "").strip())
    if not text:
        raise ValueError("问题不能为空")
    if len(text) > 500:
        raise ValueError("问题不能超过 500 个字符")
    return text


def classify_question(question: str) -> tuple[str, list[str]]:
    text = question.lower()
    if "重大违法" in question or ("违法" in question and "可能" in question):
        return "major_illegal_risk", RISK_KEYWORDS["major_illegal_risk"]
    if "持续经营" in question or "债务" in question or "重整" in question:
        return "going_concern_risk", RISK_KEYWORDS["going_concern_risk"]
    if "审计" in question or "非标" in question or "保留意见" in question:
        return "audit_risk", RISK_KEYWORDS["audit_risk"]

    terms = [part for part in re.split(r"[\s,，。！？；;、]+", question) if len(part) >= 2]
    if not terms and len(text) >= 2:
        terms = [question]
    return "general_search", terms[:8]


def score_text(text: str, terms: list[str]) -> int:
    total = 0
    haystack = text or ""
    for term in terms:
        if not term:
            continue
        count = haystack.count(term)
        if count:
            total += count * max(2, len(term))
    return total


def search_chunks(
    db_path: str | Path,
    *,
    question: str,
    security: str | None = None,
    limit: int = 12,
) -> dict[str, object]:
    question = normalize_question(question)
    question_type, terms = classify_question(question)

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        params: list[object] = []
        where = ["1 = 1"]
        if security:
            where.append("c.security = ?")
            params.append(security)
        rows = conn.execute(
            f"""
            SELECT
                c.id AS chunk_id,
                c.announcement_id,
                c.security,
                c.stock_name,
                c.notice_title,
                c.notice_type,
                c.notice_date,
                c.chunk_index,
                c.chunk_text,
                a.pdf_url,
                a.local_pdf_path
            FROM announcement_chunks c
            JOIN announcements a
              ON a.id = c.announcement_id
            WHERE {' AND '.join(where)}
            ORDER BY c.notice_date DESC, c.announcement_id DESC, c.chunk_index ASC
            """,
            params,
        ).fetchall()

    candidates: list[dict[str, object]] = []
    for row in rows:
        item = dict(row)
        title = str(item.get("notice_title") or "")
        body = str(item.get("chunk_text") or "")
        score = score_text(title, terms) * 2 + score_text(body, terms)
        if score <= 0 and question_type != "general_search":
            score = score_text(title + "\n" + body, terms)
        if score > 0:
            item["score"] = score
            candidates.append(item)

    candidates.sort(
        key=lambda item: (
            -int(item["score"]),
            str(item.get("notice_date") or ""),
            int(item.get("chunk_id") or 0),
        ),
        reverse=False,
    )
    candidates = sorted(
        candidates,
        key=lambda item: (
            -int(item["score"]),
            str(item.get("notice_date") or ""),
            int(item.get("chunk_id") or 0),
        ),
    )[: max(limit, 1)]

    stock_map: dict[str, dict[str, object]] = defaultdict(
        lambda: {
            "security": "",
            "stock_name": "",
            "score": 0,
            "evidence_count": 0,
            "latest_notice_date": None,
            "matched_terms": set(),
        }
    )
    for item in candidates:
        security_code = str(item.get("security") or "")
        stock_item = stock_map[security_code]
        stock_item["security"] = security_code
        stock_item["stock_name"] = item.get("stock_name") or ""
        stock_item["score"] = int(stock_item["score"]) + int(item["score"])
        stock_item["evidence_count"] = int(stock_item["evidence_count"]) + 1
        latest_notice_date = item.get("notice_date")
        if latest_notice_date and (
            not stock_item["latest_notice_date"]
            or str(latest_notice_date) > str(stock_item["latest_notice_date"])
        ):
            stock_item["latest_notice_date"] = latest_notice_date
        joined_text = f"{item.get('notice_title') or ''}\n{item.get('chunk_text') or ''}"
        for term in terms:
            if term and term in joined_text:
                stock_item["matched_terms"].add(term)

    stock_candidates = sorted(
        [
            {
                **stock_item,
                "matched_terms": sorted(stock_item["matched_terms"]),
            }
            for stock_item in stock_map.values()
        ],
        key=lambda item: (-int(item["score"]), -int(item["evidence_count"])),
    )

    if stock_candidates:
        lines = []
        for item in stock_candidates[:5]:
            terms_text = "、".join(item["matched_terms"]) if item["matched_terms"] else "无明确关键词"
            lines.append(
                f"{item['security']} {item['stock_name']}：命中 {item['evidence_count']} 段，"
                f"综合分 {item['score']}，关键词 {terms_text}，最近公告 {item['latest_notice_date'] or '-'}"
            )
        answer = "基于当前公告分段检索，优先关注这些股票：\n" + "\n".join(lines)
    else:
        answer = "当前知识库分段里没有检索到足够相关的公告证据。"

    return {
        "question": question,
        "question_type": question_type,
        "query_terms": terms,
        "answer": answer,
        "stock_candidates": stock_candidates,
        "evidence_chunks": candidates,
    }


def save_qa_history(db_path: str | Path, result: dict[str, object]) -> dict[str, object]:
    announcement_ids = []
    chunk_ids = []
    for item in result.get("evidence_chunks", []):
        announcement_ids.append(item.get("announcement_id"))
        chunk_ids.append(item.get("chunk_id"))
    announcement_ids = [item for item in announcement_ids if item is not None]
    chunk_ids = [item for item in chunk_ids if item is not None]

    now = utc_now_iso()
    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute(
            """
            INSERT INTO qa_history (
                question, question_type, retrieved_chunks, retrieved_announcements,
                answer, created_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                result.get("question") or "",
                result.get("question_type") or "",
                json.dumps(chunk_ids, ensure_ascii=False),
                json.dumps(announcement_ids, ensure_ascii=False),
                result.get("answer") or "",
                now,
            ),
        )
        conn.commit()
        history_id = cursor.lastrowid
    return {
        "id": history_id,
        "created_at": now,
    }


def ask_question(
    db_path: str | Path,
    *,
    question: str,
    security: str | None = None,
    limit: int = 12,
) -> dict[str, object]:
    result = search_chunks(
        db_path,
        question=question,
        security=security,
        limit=limit,
    )
    history = save_qa_history(db_path, result)
    result["history"] = history
    return result


def load_qa_history(db_path: str | Path, limit: int = 20) -> dict[str, object]:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT id, question, question_type, retrieved_chunks,
                   retrieved_announcements, answer, created_at
            FROM qa_history
            ORDER BY id DESC
            LIMIT ?
            """,
            (max(int(limit), 1),),
        ).fetchall()
    return {"items": [dict(row) for row in rows]}
