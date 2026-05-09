from __future__ import annotations

import json
import math
import os
import sqlite3
from collections import Counter
from pathlib import Path

import requests


ES_INDEX_ANNOUNCEMENTS = "st_announcements"
ES_INDEX_CHUNKS = "st_announcement_chunks"
CHROMA_COLLECTION = "st_notice_chunks"
EMBEDDING_DIMENSIONS = 256


def get_index_settings(base_dir: str | Path) -> dict[str, object]:
    base_path = Path(base_dir)
    return {
        "elasticsearch_url": os.environ.get("ST_ES_URL", "http://127.0.0.1:9200").strip(),
        "chroma_path": str(base_path / "data" / "chroma"),
        "es_index_announcements": os.environ.get(
            "ST_ES_INDEX_ANNOUNCEMENTS", ES_INDEX_ANNOUNCEMENTS
        ).strip()
        or ES_INDEX_ANNOUNCEMENTS,
        "es_index_chunks": os.environ.get(
            "ST_ES_INDEX_CHUNKS", ES_INDEX_CHUNKS
        ).strip()
        or ES_INDEX_CHUNKS,
        "chroma_collection": os.environ.get(
            "ST_CHROMA_COLLECTION", CHROMA_COLLECTION
        ).strip()
        or CHROMA_COLLECTION,
    }


def is_elasticsearch_available(base_dir: str | Path) -> tuple[bool, str | None]:
    settings = get_index_settings(base_dir)
    url = str(settings["elasticsearch_url"]).rstrip("/")
    try:
        response = requests.get(url, timeout=3)
        response.raise_for_status()
        return True, None
    except Exception as exc:
        return False, f"{exc.__class__.__name__}: {exc}"


def is_chroma_available() -> tuple[bool, str | None]:
    try:
        import chromadb  # noqa: F401

        return True, None
    except Exception as exc:
        return False, f"{exc.__class__.__name__}: {exc}"


def get_index_status(base_dir: str | Path, db_path: str | Path) -> dict[str, object]:
    settings = get_index_settings(base_dir)
    es_ok, es_error = is_elasticsearch_available(base_dir)
    chroma_ok, chroma_error = is_chroma_available()
    with sqlite3.connect(db_path) as conn:
        announcement_count = conn.execute(
            "SELECT COUNT(*) FROM announcements"
        ).fetchone()[0]
        chunk_count = conn.execute(
            "SELECT COUNT(*) FROM announcement_chunks"
        ).fetchone()[0]
    return {
        "settings": settings,
        "sources": {
            "announcements": announcement_count,
            "chunks": chunk_count,
        },
        "elasticsearch": {
            "available": es_ok,
            "error": es_error,
        },
        "chroma": {
            "available": chroma_ok,
            "error": chroma_error,
        },
    }


def build_hash_embedding(text: str, dimensions: int = EMBEDDING_DIMENSIONS) -> list[float]:
    tokens = [token for token in text.lower().split() if token]
    if not tokens:
        return [0.0] * dimensions
    counts = Counter(tokens)
    vector = [0.0] * dimensions
    for token, weight in counts.items():
        index = hash(token) % dimensions
        vector[index] += float(weight)
    norm = math.sqrt(sum(value * value for value in vector))
    if norm <= 0:
        return vector
    return [value / norm for value in vector]


def ensure_es_index(base_dir: str | Path, *, index_name: str, mapping: dict[str, object]) -> None:
    settings = get_index_settings(base_dir)
    url = str(settings["elasticsearch_url"]).rstrip("/")
    response = requests.put(
        f"{url}/{index_name}",
        headers={"Content-Type": "application/json"},
        data=json.dumps(mapping, ensure_ascii=False),
        timeout=10,
    )
    if response.status_code not in {200, 201}:
        body = response.text
        if response.status_code == 400 and "resource_already_exists_exception" in body:
            return
        response.raise_for_status()


def bulk_index_to_es(
    base_dir: str | Path,
    *,
    index_name: str,
    docs: list[dict[str, object]],
    id_field: str,
) -> int:
    if not docs:
        return 0
    settings = get_index_settings(base_dir)
    url = str(settings["elasticsearch_url"]).rstrip("/")
    lines: list[str] = []
    for doc in docs:
        lines.append(json.dumps({"index": {"_index": index_name, "_id": doc[id_field]}}))
        lines.append(json.dumps(doc, ensure_ascii=False))
    payload = "\n".join(lines) + "\n"
    response = requests.post(
        f"{url}/_bulk",
        headers={"Content-Type": "application/x-ndjson"},
        data=payload.encode("utf-8"),
        timeout=30,
    )
    response.raise_for_status()
    result = response.json()
    if result.get("errors"):
        raise RuntimeError("Elasticsearch bulk index returned errors")
    return len(docs)


def load_announcement_docs(db_path: str | Path, security: str | None = None) -> list[dict[str, object]]:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        params: list[object] = []
        where = ["1 = 1"]
        if security:
            where.append("security = ?")
            params.append(security)
        rows = conn.execute(
            f"""
            SELECT
                id AS announcement_id,
                security,
                stock_name,
                notice_title,
                notice_type,
                notice_date,
                infocode,
                pdf_url,
                local_pdf_path,
                detail_url,
                COALESCE(ocr_text, '') AS ocr_text
            FROM announcements
            WHERE {' AND '.join(where)}
            ORDER BY notice_date DESC, id DESC
            """,
            params,
        ).fetchall()
    return [dict(row) for row in rows]


def load_chunk_docs(db_path: str | Path, security: str | None = None) -> list[dict[str, object]]:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        params: list[object] = []
        where = ["1 = 1"]
        if security:
            where.append("security = ?")
            params.append(security)
        rows = conn.execute(
            f"""
            SELECT
                id AS chunk_id,
                announcement_id,
                security,
                stock_name,
                notice_title,
                notice_type,
                notice_date,
                chunk_index,
                chunk_text,
                chunk_hash,
                parse_source
            FROM announcement_chunks
            WHERE {' AND '.join(where)}
            ORDER BY notice_date DESC, announcement_id DESC, chunk_index ASC
            """,
            params,
        ).fetchall()
    return [dict(row) for row in rows]


def rebuild_elasticsearch_indexes(
    base_dir: str | Path,
    db_path: str | Path,
    *,
    security: str | None = None,
) -> dict[str, object]:
    settings = get_index_settings(base_dir)
    ensure_es_index(
        base_dir,
        index_name=str(settings["es_index_announcements"]),
        mapping={
            "mappings": {
                "properties": {
                    "announcement_id": {"type": "integer"},
                    "security": {"type": "keyword"},
                    "stock_name": {"type": "text"},
                    "notice_title": {"type": "text"},
                    "notice_type": {"type": "keyword"},
                    "notice_date": {"type": "keyword"},
                    "infocode": {"type": "keyword"},
                    "pdf_url": {"type": "keyword", "index": False},
                    "local_pdf_path": {"type": "keyword", "index": False},
                    "detail_url": {"type": "keyword", "index": False},
                    "ocr_text": {"type": "text"},
                }
            }
        },
    )
    ensure_es_index(
        base_dir,
        index_name=str(settings["es_index_chunks"]),
        mapping={
            "mappings": {
                "properties": {
                    "chunk_id": {"type": "integer"},
                    "announcement_id": {"type": "integer"},
                    "security": {"type": "keyword"},
                    "stock_name": {"type": "text"},
                    "notice_title": {"type": "text"},
                    "notice_type": {"type": "keyword"},
                    "notice_date": {"type": "keyword"},
                    "chunk_index": {"type": "integer"},
                    "chunk_text": {"type": "text"},
                    "chunk_hash": {"type": "keyword"},
                    "parse_source": {"type": "keyword"},
                }
            }
        },
    )
    announcement_docs = load_announcement_docs(db_path, security=security)
    chunk_docs = load_chunk_docs(db_path, security=security)
    announcement_total = bulk_index_to_es(
        base_dir,
        index_name=str(settings["es_index_announcements"]),
        docs=announcement_docs,
        id_field="announcement_id",
    )
    chunk_total = bulk_index_to_es(
        base_dir,
        index_name=str(settings["es_index_chunks"]),
        docs=chunk_docs,
        id_field="chunk_id",
    )
    return {
        "ok": True,
        "engine": "elasticsearch",
        "announcement_index": settings["es_index_announcements"],
        "chunk_index": settings["es_index_chunks"],
        "announcement_docs": announcement_total,
        "chunk_docs": chunk_total,
        "security": security,
    }


def rebuild_chroma_index(
    base_dir: str | Path,
    db_path: str | Path,
    *,
    security: str | None = None,
) -> dict[str, object]:
    try:
        import chromadb
    except Exception as exc:
        return {
            "ok": False,
            "engine": "chroma",
            "error": f"{exc.__class__.__name__}: {exc}",
            "indexed": 0,
        }

    settings = get_index_settings(base_dir)
    chroma_path = Path(str(settings["chroma_path"]))
    chroma_path.mkdir(parents=True, exist_ok=True)
    client = chromadb.PersistentClient(path=str(chroma_path))
    collection = client.get_or_create_collection(name=str(settings["chroma_collection"]))
    chunk_docs = load_chunk_docs(db_path, security=security)
    if not chunk_docs:
        return {
            "ok": True,
            "engine": "chroma",
            "collection": settings["chroma_collection"],
            "indexed": 0,
            "security": security,
        }

    ids = [str(doc["chunk_id"]) for doc in chunk_docs]
    embeddings = [build_hash_embedding(str(doc["chunk_text"] or "")) for doc in chunk_docs]
    documents = [str(doc["chunk_text"] or "") for doc in chunk_docs]
    metadatas = [
        {
            "chunk_id": int(doc["chunk_id"]),
            "announcement_id": int(doc["announcement_id"]),
            "security": str(doc["security"] or ""),
            "stock_name": str(doc["stock_name"] or ""),
            "notice_title": str(doc["notice_title"] or ""),
            "notice_type": str(doc["notice_type"] or ""),
            "notice_date": str(doc["notice_date"] or ""),
            "chunk_index": int(doc["chunk_index"]),
            "parse_source": str(doc["parse_source"] or ""),
        }
        for doc in chunk_docs
    ]
    collection.upsert(ids=ids, embeddings=embeddings, documents=documents, metadatas=metadatas)
    return {
        "ok": True,
        "engine": "chroma",
        "collection": settings["chroma_collection"],
        "indexed": len(chunk_docs),
        "security": security,
    }


def rebuild_search_indexes(
    base_dir: str | Path,
    db_path: str | Path,
    *,
    security: str | None = None,
) -> dict[str, object]:
    es_ok, es_error = is_elasticsearch_available(base_dir)
    chroma_ok, chroma_error = is_chroma_available()
    result: dict[str, object] = {
        "security": security,
        "elasticsearch": {
            "ok": False,
            "error": es_error,
        },
        "chroma": {
            "ok": False,
            "error": chroma_error,
        },
    }
    if es_ok:
        result["elasticsearch"] = rebuild_elasticsearch_indexes(
            base_dir, db_path, security=security
        )
    if chroma_ok:
        result["chroma"] = rebuild_chroma_index(base_dir, db_path, security=security)
    return result
