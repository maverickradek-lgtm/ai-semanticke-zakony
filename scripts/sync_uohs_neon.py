"""
Fetcher pro otevrena data UOHS (Urad pro ochranu hospodarske souteze) -
PRIMY zapis do samostatne Neon databaze (narozdil od puvodniho
scripts/sync_uohs.py, ktery pise do hlavni Supabase).

Zdroj: https://uohs.gov.cz/opendata/rozhodnuti.jsonld - jeden velky JSON-LD
soubor se vsemi rozhodnutimi (metadata), plny text je ale az v pripojenem PDF
(pole "dokument"."url"), ne primo v JSON. Proto se u kazdeho noveho zaznamu
PDF stahne a text se z nej extrahuje pomoci pypdf.

Na rozdil od puvodniho skriptu tenhle dela FAZI A i FAZI B v jednom behu
(import textu + embedding), protoze uz neexistuje sdileny
sync_esbirka_embed.py bezici proti Neonu - kazdy Neon pipeline si embedding
resi sam (stejny vzor jako sync_celni_metodiky.py / sync_fs_metodiky.py).

CUTOFF_DATE je schvalne PEVNE datum (1.10.2016 - ucinnost zakona 134/2016 Sb.
o zadavani verejnych zakazek), ne posuvne okno posledních N let - jinak by
casem zacalo unikat i rozhodnuti z prvnich let noveho zakona, jak by se
posuvalo "dnes".

Vsechny retezce v tomto souboru jsou schvalne bez zpetnych lomitek (pouzito
chr() tam, kde je potreba odradkovani/tabulator) kvuli riziku poskozeni pri
prenosu pres GitHub Contents API / JS template literal.
"""

import os
import re
import time
from datetime import date
from io import BytesIO

import requests
from pypdf import PdfReader
import psycopg2

NL = chr(10)
TAB = chr(9)

NEON_DB_URL = os.environ["NEON_UOHS_DB_URL"]
SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SERVICE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
ADMIN_USER_ID = "2648f5db-bea6-4cac-b490-ad0ec59723df"
GEMINI_API_KEY_OVERRIDE = os.environ.get("GEMINI_API_KEY_OVERRIDE")
GEMINI_API_KEY_POOL = [k.strip() for k in os.environ.get("GEMINI_API_KEY_POOL", "").split(",") if k.strip()]

EMBED_MODEL = "gemini-embedding-001"
EMBED_DIM = 256

UOHS_JSONLD_URL = "https://uohs.gov.cz/opendata/rozhodnuti.jsonld"
MAX_CHARS_PER_CHUNK = int(os.environ.get("MAX_CHARS_PER_CHUNK", "4000"))
MAX_ITEMS = int(os.environ.get("MAX_ITEMS", "300"))
TIME_BUDGET_SECONDS = int(os.environ.get("TIME_BUDGET_SECONDS", "3000"))

# Pevne datum ucinnosti zakona 134/2016 Sb. - narozdil od puvodniho
# CUTOFF_YEARS (posuvne okno) tohle NEERODUJE s casem.
CUTOFF_DATE = os.environ.get("CUTOFF_DATE", "2016-10-01")

SESSION = requests.Session()
REQ_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; AIsemantickeZakony/1.0)"}

START_TIME = time.time()
_key_rotation_idx = [0]


def log(*a):
    print(*a, flush=True)


def time_left():
    return TIME_BUDGET_SECONDS - (time.time() - START_TIME)


def sb_headers():
    return {
        "apikey": SERVICE_KEY,
        "Authorization": "Bearer " + SERVICE_KEY,
        "Content-Type": "application/json",
    }


def get_admin_gemini_key():
    if GEMINI_API_KEY_OVERRIDE:
        return GEMINI_API_KEY_OVERRIDE
    r = requests.post(
        SUPABASE_URL + "/rest/v1/rpc/get_user_gemini_key",
        headers=sb_headers(),
        json={"p_user_id": ADMIN_USER_ID},
        timeout=30,
    )
    r.raise_for_status()
    key = r.json()
    if not key:
        raise RuntimeError("Admin Gemini key not available")
    return key


def get_gemini_keys():
    """Vraci seznam klicu pro rotaci - pokud je nastaven GEMINI_API_KEY_POOL
    (carkou oddeleny seznam), pouzije se ten (vice klicu = vic denni kvoty
    Gemini API rozprostrene mezi vic ucty). Jinak fallback na jediny admin
    klic jako doteď."""
    if GEMINI_API_KEY_POOL:
        return GEMINI_API_KEY_POOL
    return [get_admin_gemini_key()]


def next_gemini_key(keys):
    idx = _key_rotation_idx[0] % len(keys)
    _key_rotation_idx[0] += 1
    return keys[idx]


def ensure_conn(conn):
    if conn is not None and conn.closed == 0:
        try:
            with conn.cursor() as cur:
                cur.execute("select 1")
            return conn
        except Exception:
            try:
                conn.close()
            except Exception:
                pass
    new_conn = psycopg2.connect(NEON_DB_URL, connect_timeout=15)
    log("(znovu navazano spojeni)")
    return new_conn


def ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute("create extension if not exists pgcrypto")
        cur.execute("create extension if not exists vector")
        cur.execute(
            """
            create table if not exists documents (
                id uuid primary key default gen_random_uuid(),
                source text not null default 'uohs',
                external_id text not null,
                title text not null,
                issuer text,
                url text,
                decision_date date,
                status text,
                is_current boolean not null default true,
                fetched_at timestamptz default now(),
                created_at timestamptz default now(),
                unique(source, external_id)
            )
            """
        )
        cur.execute(
            """
            create table if not exists chunks (
                id uuid primary key default gen_random_uuid(),
                document_id uuid not null references documents(id) on delete cascade,
                chunk_index int not null,
                heading text,
                content text,
                embedding vector(256),
                created_at timestamptz default now()
            )
            """
        )
        cur.execute("create index if not exists chunks_document_id_idx on chunks(document_id)")
    conn.commit()


def fetch_existing_external_ids(conn):
    with conn.cursor() as cur:
        cur.execute("select external_id from documents where source = 'uohs'")
        return set(row[0] for row in cur.fetchall())


def extract_pdf_text(pdf_bytes):
    reader = PdfReader(BytesIO(pdf_bytes))
    parts = []
    for page in reader.pages:
        t = page.extract_text() or ""
        parts.append(t)
    text = NL.join(parts)
    text = re.sub(r"[ " + TAB + "]+", " ", text)
    text = re.sub(NL + "{3,}", NL + NL, text)
    return text.strip()


def split_into_chunks(text, max_chars=None):
    if max_chars is None:
        max_chars = MAX_CHARS_PER_CHUNK
    sep = NL + NL
    paragraphs = [p.strip() for p in text.split(sep) if p.strip()]
    chunks = []
    current = ""
    for para in paragraphs:
        if current and len(current) + len(para) + 2 > max_chars:
            chunks.append(current)
            current = para
        else:
            current = (current + sep + para) if current else para
    if current:
        chunks.append(current)
    return chunks


def embed_text(text, gemini_key, retries=3):
    for attempt in range(retries):
        try:
            resp = requests.post(
                "https://generativelanguage.googleapis.com/v1beta/models/" + EMBED_MODEL + ":embedContent",
                headers={"Content-Type": "application/json", "x-goog-api-key": gemini_key},
                json={
                    "content": {"parts": [{"text": text[:8000]}]},
                    "taskType": "RETRIEVAL_DOCUMENT",
                    "outputDimensionality": EMBED_DIM,
                },
                timeout=30,
            )
            if resp.status_code == 429:
                time.sleep(5 * (attempt + 1))
                continue
            resp.raise_for_status()
            vec = resp.json().get("embedding", {}).get("values")
            if not vec:
                return None
            return vec
        except requests.RequestException:
            if attempt == retries - 1:
                raise
            time.sleep(3 * (attempt + 1))
    return None


def upsert_document(conn, item):
    external_id = item.get("číslo_jednací") or item.get("spisová_značka")
    title = (item.get("věc") or {}).get("cs") or item.get("spisová_značka") or external_id
    dokumenty = item.get("dokument") or []
    if not dokumenty:
        return None, None
    pdf_url = dokumenty[0].get("url")
    if not pdf_url:
        return None, None

    r = SESSION.get(pdf_url, headers=REQ_HEADERS, timeout=60)
    if not r.ok:
        log("   PDF stahovani selhalo (" + str(r.status_code) + "): " + pdf_url)
        return None, None
    try:
        text = extract_pdf_text(r.content)
    except Exception as e:
        log("   PDF extrakce selhala: " + str(e))
        return None, None
    if not text or len(text) < 50:
        log("   Prazdny/kratky text po extrakci, preskakuji: " + str(external_id))
        return None, None

    datum = (item.get("datum_právní_moci") or {}).get("datum")

    with conn.cursor() as cur:
        cur.execute(
            "insert into documents (source, external_id, title, issuer, url, decision_date, status, is_current) values (%s,%s,%s,%s,%s,%s,%s,%s) returning id",
            ("uohs", external_id, title, "UOHS", item.get("iri") or pdf_url, datum, "platny", True),
        )
        doc_id = cur.fetchone()[0]
        chunk_list = split_into_chunks(text)
        for i, chunk_text in enumerate(chunk_list):
            heading = "cast " + str(i + 1) + "/" + str(len(chunk_list)) if len(chunk_list) > 1 else None
            cur.execute(
                "insert into chunks (document_id, chunk_index, heading, content) values (%s,%s,%s,%s)",
                (doc_id, i, heading, chunk_text[:8000]),
            )
    conn.commit()
    return doc_id, len(chunk_list)


def import_new_documents(conn):
    existing = fetch_existing_external_ids(conn)
    log("Jiz v databazi: " + str(len(existing)) + " rozhodnuti UOHS")

    log("Stahuji rozhodnuti.jsonld (muze trvat, cca 16 MB)...")
    r = SESSION.get(UOHS_JSONLD_URL, headers=REQ_HEADERS, timeout=180)
    r.raise_for_status()
    data = r.json()
    items = data.get("rozhodnutí", [])
    log("Nalezeno " + str(len(items)) + " rozhodnuti v datove sade")

    imported = 0
    skipped = 0
    too_old = 0
    errors = 0
    for item in items:
        conn = ensure_conn(conn)
        external_id = item.get("číslo_jednací") or item.get("spisová_značka")
        if not external_id or external_id in existing:
            skipped += 1
            continue
        item_date = (item.get("datum_právní_moci") or {}).get("datum")
        if item_date and item_date < CUTOFF_DATE:
            too_old += 1
            continue
        if MAX_ITEMS and imported >= MAX_ITEMS:
            log("Dosazen MAX_ITEMS=" + str(MAX_ITEMS) + ", koncim (zbytek doplni dalsi beh).")
            break
        if time_left() <= 0:
            log("Casovy rozpocet vycerpan, koncim cistě (zbytek doplni dalsi beh).")
            break
        try:
            doc_id, n_chunks = upsert_document(conn, item)
            if doc_id:
                imported += 1
                existing.add(external_id)
                if imported % 25 == 0:
                    log("   ...naimportovano " + str(imported))
            else:
                errors += 1
        except Exception as e:
            log("   Chyba u " + str(external_id) + ": " + str(e))
            errors += 1

    log(
        "Import done: " + str(imported) + " new, skipped=" + str(skipped)
        + " too_old=" + str(too_old) + " errors=" + str(errors)
    )
    return conn


def embed_pending(conn, gemini_keys):
    embedded = 0
    while time_left() > 30:
        conn = ensure_conn(conn)
        with conn.cursor() as cur:
            cur.execute("select id, content from chunks where embedding is null order by created_at asc limit 20")
            rows = cur.fetchall()
        if not rows:
            break
        for chunk_id, content in rows:
            if time_left() <= 0:
                break
            gemini_key = next_gemini_key(gemini_keys)
            try:
                vec = embed_text(content, gemini_key)
            except Exception as e:
                log("WARN: embed failed: " + str(chunk_id) + " " + str(e))
                continue
            if not vec:
                continue
            vec_str = "[" + ",".join("%.8f" % x for x in vec) + "]"
            conn = ensure_conn(conn)
            with conn.cursor() as cur2:
                cur2.execute("update chunks set embedding = %s::vector where id = %s", (vec_str, chunk_id))
            conn.commit()
            embedded += 1
    log("Embedding done: " + str(embedded) + " chunks")
    return conn


def main():
    log("=== UOHS sync (Neon): start ===")
    log("MAX_ITEMS=" + str(MAX_ITEMS) + ", CUTOFF_DATE=" + CUTOFF_DATE)
    conn = psycopg2.connect(NEON_DB_URL, connect_timeout=15)
    try:
        ensure_schema(conn)
        conn = import_new_documents(conn)
        gemini_keys = get_gemini_keys()
        log("Pocet Gemini klicu v rotaci: " + str(len(gemini_keys)))
        conn = embed_pending(conn, gemini_keys)
    finally:
        conn.close()
    log("=== UOHS sync (Neon): hotovo ===")


if __name__ == "__main__":
    main()
