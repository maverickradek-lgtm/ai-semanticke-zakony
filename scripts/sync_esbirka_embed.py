"""
FAZE B: denni dobihajici vypocet embeddingu pro useky (chunks), ktere je
jeste nemaji (embedding IS NULL) - typicky proto, ze je tam faze A
(sync_esbirka_text.py) teprve nedavno nahrala.

Bezplatny Gemini klic ma denni limit poctu volani, proto se tu zpracovava
jen omezena denni davka (DAILY_EMBED_BUDGET) a zbytek pockej na dalsi den -
tento skript je navrzeny tak, aby bezel na cronu kazdy den a postupne
dobihal, dokud neni vse ohodnoceno; pak uz jen drzi krok s novinkami.

POZNAMKA: Supabase/PostgREST vraci na jeden dotaz max PAGE_SIZE radku bez
ohledu na pozadovany "limit" parametr, proto se cela denni davka stahuje
a zpracovava po strankach (viz main()).
"""

import os
import time

import requests

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SERVICE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
ADMIN_USER_ID = os.environ["ADMIN_USER_ID"]

DAILY_EMBED_BUDGET = int(os.environ.get("DAILY_EMBED_BUDGET", "900"))
PAGE_SIZE = int(os.environ.get("EMBED_PAGE_SIZE", "1000"))

EMBED_MODEL = "gemini-embedding-001"
EMBED_DIM = 768

SESSION = requests.Session()


def log(*a):
    print(*a, flush=True)


def get_admin_gemini_key():
    r = SESSION.post(
        f"{SUPABASE_URL}/rest/v1/rpc/get_user_gemini_key",
        headers={
            "apikey": SERVICE_KEY,
            "Authorization": f"Bearer {SERVICE_KEY}",
            "Content-Type": "application/json",
        },
        json={"p_user_id": ADMIN_USER_ID},
        timeout=30,
    )
    r.raise_for_status()
    key = r.json()
    if not key:
        raise RuntimeError("Admin nema ulozeny Gemini klic.")
    return key


def embed_text(text, gemini_key, task_type="RETRIEVAL_DOCUMENT"):
    for attempt in range(5):
        r = SESSION.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/{EMBED_MODEL}:embedContent",
            headers={"Content-Type": "application/json", "x-goog-api-key": gemini_key},
            json={
                "content": {"parts": [{"text": text[:8000]}]},
                "taskType": task_type,
                "outputDimensionality": EMBED_DIM,
            },
            timeout=60,
        )
        if r.status_code == 429:
            wait = 15 * (attempt + 1)
            log(f"   rate limit, cekam {wait}s...")
            time.sleep(wait)
            continue
        if not r.ok:
            log("   embed chyba:", r.status_code, r.text[:300])
            return None
        values = r.json()["embedding"]["values"]
        norm = sum(v * v for v in values) ** 0.5
        if norm > 0:
            values = [v / norm for v in values]
        return values
    log("   embedding se nepovedlo ziskat po 5 pokusech (rate limit)")
    return None


def fetch_pending_chunks(limit):
    r = SESSION.get(
        f"{SUPABASE_URL}/rest/v1/chunks",
        headers={"apikey": SERVICE_KEY, "Authorization": f"Bearer {SERVICE_KEY}"},
        params={
            "select": "id,heading,content,document_id",
            "embedding": "is.null",
            "order": "created_at.asc",
            "limit": str(limit),
        },
        timeout=60,
    )
    r.raise_for_status()
    return r.json()


def fetch_document_titles(document_ids):
    if not document_ids:
        return {}
    ids_param = "in.(" + ",".join(document_ids) + ")"
    r = SESSION.get(
        f"{SUPABASE_URL}/rest/v1/documents",
        headers={"apikey": SERVICE_KEY, "Authorization": f"Bearer {SERVICE_KEY}"},
        params={"select": "id,title", "id": ids_param},
        timeout=60,
    )
    r.raise_for_status()
    return {d["id"]: d["title"] for d in r.json()}


def update_chunk_embedding(chunk_id, embedding):
    r = SESSION.patch(
        f"{SUPABASE_URL}/rest/v1/chunks",
        headers={
            "apikey": SERVICE_KEY,
            "Authorization": f"Bearer {SERVICE_KEY}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal",
        },
        params={"id": f"eq.{chunk_id}"},
        json={"embedding": embedding},
        timeout=30,
    )
    r.raise_for_status()


def main():
    log("=== Embedding dobihani (faze B): start ===")
    log(f"DAILY_EMBED_BUDGET={DAILY_EMBED_BUDGET}, PAGE_SIZE={PAGE_SIZE}")
    gemini_key = get_admin_gemini_key()

    total_done = 0
    total_failed = 0
    remaining = DAILY_EMBED_BUDGET

    while remaining > 0:
        batch_limit = min(PAGE_SIZE, remaining)
        chunks = fetch_pending_chunks(batch_limit)
        if not chunks:
            log("Zadne dalsi useky bez embeddingu, hotovo.")
            break

        log(f"Stranka: nalezeno {len(chunks)} useku bez embeddingu (zbyva z denni davky: {remaining})")
        titles = fetch_document_titles(list({c["document_id"] for c in chunks}))

        for c in chunks:
            title = titles.get(c["document_id"], "")
            text = f"{title} {c['heading']}: {c['content']}"
            embedding = embed_text(text, gemini_key)
            if embedding is None:
                total_failed += 1
                continue
            update_chunk_embedding(c["id"], embedding)
            total_done += 1
            if total_done % 100 == 0:
                log(f" ...ohodnoceno celkem {total_done}")

        remaining -= len(chunks)

        if len(chunks) < batch_limit:
            log("Posledni neuplna stranka vracena, dalsi kolo by bylo prazdne - koncim.")
            break

    log(f"=== Embedding dobihani: hotovo, ohodnoceno {total_done}, selhalo {total_failed} ===")


if __name__ == "__main__":
    main()
