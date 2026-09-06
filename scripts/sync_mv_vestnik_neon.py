"""
Sync Vestnik vlady pro organy kraju a organy obci (Ministerstvo vnitra) do
samostatne Neon databaze. mv.gov.cz ma vlastni souborovy server trvale
rozbity (vsechny /soubor/*.aspx odkazy vraci 404 - overeno 2026-08-24 i
znovu 2026-09-05), ale existuje jina oficialni cesta: portal.gov.cz
("Vestniky na gov.cz", provozuje Digitalni a informacni agentura, ne MV) ma
kompletni, cistý seznam vsech vydani na jedne strance
(https://portal.gov.cz/vestniky/6bnaawp/?all=True) a kazde vydani ma primy
verejny PDF odkaz bez prihlaseni. Zdroj je male velikosti (cca 55 zaznamu
celkem, 2013-soucasnost), takze cely backlog se stahne v jednom behu.

Vsechny retezce v tomto souboru jsou schvalne bez zpetnych lomitek (pouzito
chr() tam, kde je potreba odradkovani/tabulator) kvuli riziku poskozeni pri
prenosu pres GitHub Contents API / JS template literal - stejna konvence
jako sync_celni_metodiky.py / sync_uoou_neon.py.
"""

import os
import re
import time
from io import BytesIO

import requests
from bs4 import BeautifulSoup
from pypdf import PdfReader
import psycopg2


def db_connect(url, timeout=15):
    """Pripoji se k Neonu se 4 pokusy - NAS self-hosted runner ma obcas
    docasny DNS vypadek (Temporary failure in name resolution), jednorazovy
    pokus bez retry pak shodi cely beh zbytecne."""
    last_err = None
    for attempt in range(4):
        try:
            return psycopg2.connect(url, connect_timeout=timeout)
        except Exception as e:
            last_err = e
            print("db_connect selhalo (pokus " + str(attempt + 1) + "/4): " + str(e), flush=True)
            time.sleep(3)
    raise last_err


NL = chr(10)
TAB = chr(9)

NEON_DB_URL = os.environ["NEON_MV_DB_URL"]
SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SERVICE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
ADMIN_USER_ID = "2648f5db-bea6-4cac-b490-ad0ec59723df"
GEMINI_API_KEY_OVERRIDE = os.environ.get("GEMINI_API_KEY_OVERRIDE")

EMBED_MODEL = "gemini-embedding-001"
EMBED_DIM = 256
MAX_CHARS_PER_CHUNK = 3000
MAX_NEW_DOCS_PER_RUN = int(os.environ.get("MAX_NEW_DOCS_PER_RUN", "100"))
TIME_BUDGET_SECONDS = int(os.environ.get("TIME_BUDGET_SECONDS", "3000"))

PORTAL_BASE = "https://portal.gov.cz"
LISTING_URL = PORTAL_BASE + "/vestniky/6bnaawp/?all=True"
DETAIL_HREF_RE = re.compile(r"^/vestniky/6bnaawp/(\d+)$")
DATE_RE = re.compile(r"(\d{2})\.(\d{2})\.(\d{4})")

REQ_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; AIsemantickeZakony/1.0)"}
SESSION = requests.Session()

START_TIME = time.time()


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


def ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute("create extension if not exists pgcrypto")
        cur.execute("create extension if not exists vector")
        cur.execute(
            """
            create table if not exists documents (
                id uuid primary key default gen_random_uuid(),
                source text not null default 'mv_vestnik',
                series text,
                external_id text not null,
                title text not null,
                url text not null,
                pdf_url text,
                published_date date,
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


def fetch_existing_external_ids(conn, source="mv_vestnik"):
    with conn.cursor() as cur:
        cur.execute("select external_id from documents where source = %s", (source,))
        return set(row[0] for row in cur.fetchall())


def parse_iso_date(ddmmyyyy_match):
    d, m, y = ddmmyyyy_match.group(1), ddmmyyyy_match.group(2), ddmmyyyy_match.group(3)
    return y + "-" + m + "-" + d


def fetch_listing():
    resp = SESSION.get(LISTING_URL, headers=REQ_HEADERS, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    items = []
    seen_ids = set()
    for a in soup.find_all("a", href=True):
        m = DETAIL_HREF_RE.match(a["href"])
        if not m:
            continue
        record_id = m.group(1)
        if record_id in seen_ids:
            continue
        title = a.get_text(strip=True)
        if not title:
            continue
        row = a.find_parent("tr") or a.find_parent("div")
        row_text = row.get_text(" ", strip=True) if row else ""
        dates = DATE_RE.findall(row_text)
        published = None
        if dates:
            d, m2, y = dates[0]
            published = y + "-" + m2 + "-" + d
        seen_ids.add(record_id)
        items.append({"record_id": record_id, "title": title, "published": published})
    return items


def extract_pdf_text(pdf_bytes):
    reader = PdfReader(BytesIO(pdf_bytes))
    parts = []
    for page in reader.pages:
        t = page.extract_text() or ""
        parts.append(t)
    text = NL.join(parts)
    text = text.replace(TAB, " ")
    while "  " in text:
        text = text.replace("  ", " ")
    while (NL + NL + NL) in text:
        text = text.replace(NL + NL + NL, NL + NL)
    return text.strip()


def split_into_chunks(text, max_chars=MAX_CHARS_PER_CHUNK):
    sep = NL + NL
    paragraphs = [p.strip() for p in text.split(sep) if p.strip()]
    chunks = []
    current = []
    current_len = 0
    for p in paragraphs:
        if current_len + len(p) > max_chars and current:
            chunks.append(sep.join(current))
            current = []
            current_len = 0
        current.append(p)
        current_len += len(p)
    if current:
        chunks.append(sep.join(current))
    if not chunks and text.strip():
        chunks = [text[i:i + max_chars] for i in range(0, len(text), max_chars)]
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
    new_conn = db_connect(NEON_DB_URL)
    log("(znovu navazano spojeni)")
    return new_conn


def import_new_documents(conn):
    existing = fetch_existing_external_ids(conn)
    log("Jiz v databazi: " + str(len(existing)) + " zaznamu MV vestniku")

    items = fetch_listing()
    log("Nalezeno na seznamu (portal.gov.cz): " + str(len(items)) + " zaznamu celkem")

    new_count = 0
    errors = 0
    with conn.cursor() as cur:
        for item in items:
            if time_left() <= 0 or new_count >= MAX_NEW_DOCS_PER_RUN:
                log("Dosazen limit (cas nebo MAX_NEW_DOCS_PER_RUN), zbytek doplni dalsi beh.")
                break
            external_id = "mv:" + item["record_id"]
            if external_id in existing:
                continue
            detail_url = PORTAL_BASE + "/vestniky/6bnaawp/" + item["record_id"]
            pdf_url = detail_url + ".pdf"
            try:
                resp = SESSION.get(pdf_url, headers=REQ_HEADERS, timeout=30)
                if resp.status_code != 200 or (resp.content[:4] != b"%PDF" and "pdf" not in resp.headers.get("Content-Type", "").lower()):
                    log("SKIP (neni PDF): " + pdf_url + " status=" + str(resp.status_code))
                    errors += 1
                    continue
                body_text = extract_pdf_text(resp.content)
            except Exception as e:
                log("WARN: stahovani/extrakce selhala: " + pdf_url + " " + str(e))
                errors += 1
                continue

            if not body_text or len(body_text) < 20:
                log("SKIP (malo textu): " + pdf_url)
                errors += 1
                continue

            title = item["title"]
            conn = ensure_conn(conn)
            with conn.cursor() as cur:
                cur.execute(
                    "insert into documents (source, series, external_id, title, url, pdf_url, published_date, is_current) "
                    "values (%s,%s,%s,%s,%s,%s,%s,%s) returning id",
                    ("mv_vestnik", "vestnik_vlady", external_id, title, detail_url, pdf_url, item["published"], True),
                )
                doc_id = cur.fetchone()[0]
                chunk_list = split_into_chunks(body_text)
                for i, ch in enumerate(chunk_list):
                    cur.execute(
                        "insert into chunks (document_id, chunk_index, heading, content) values (%s,%s,%s,%s)",
                        (doc_id, i, title, ch),
                    )
            conn.commit()
            existing.add(external_id)
            new_count += 1
            log("IMPORTED: " + external_id + " " + title + " chunks=" + str(len(chunk_list)))

    log("Import done: " + str(new_count) + " new, errors=" + str(errors))
    return conn


def embed_pending(conn, gemini_key):
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
    log("=== MV Vestnik sync (Neon, portal.gov.cz): start ===")
    conn = db_connect(NEON_DB_URL)
    try:
        ensure_schema(conn)
        conn = import_new_documents(conn)
        gemini_key = get_admin_gemini_key()
        conn = embed_pending(conn, gemini_key)
    finally:
        conn.close()
    log("=== MV Vestnik sync (Neon): hotovo ===")


if __name__ == "__main__":
    main()
