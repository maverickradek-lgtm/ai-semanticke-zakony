"""
Sync Celni sprava (MI = Metodicke informace, VP = Vnitrni pokyny) do
samostatne Neon databaze. Index stranka /cz/Metodika/ je login-walled
(SharePoint), ale jednotliva PDF jsou verejne dostupna na predvidatelne
URL adrese bez prihlaseni - viz projektova pamet
new_source_feasibility_financni_celni_mv_2026-08-08. Proto misto
parsovani seznamu pouzivame brute-force enumeraci cisel v ramci kazdeho
roku a kazde serie, dokud nenarazime na dostatek chybejicich cisel za
sebou (heuristika - nepresne, ale funkcni bez prihlaseni).

Vsechny retezce v tomto souboru jsou schvalne bez zpetnych lomitek
(pouzito chr() tam, kde je potreba odradkovani/tabulator) kvuli riziku
poskozeni pri prenosu pres GitHub Contents API / JS template literal
(viz projektova pamet - stejna disciplina jako sync_fs_metodiky.py).
"""

import os
import time
from datetime import datetime
from io import BytesIO

import requests
from pypdf import PdfReader
import psycopg2

NL = chr(10)
TAB = chr(9)

NEON_DB_URL = os.environ["NEON_CELNI_DB_URL"]
SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SERVICE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
ADMIN_USER_ID = "2648f5db-bea6-4cac-b490-ad0ec59723df"
GEMINI_API_KEY_OVERRIDE = os.environ.get("GEMINI_API_KEY_OVERRIDE")

EMBED_MODEL = "gemini-embedding-001"
EMBED_DIM = 256
MAX_CHARS_PER_CHUNK = 3000
MAX_NEW_DOCS_PER_RUN = int(os.environ.get("MAX_NEW_DOCS_PER_RUN", "60"))
TIME_BUDGET_SECONDS = int(os.environ.get("TIME_BUDGET_SECONDS", "3000"))
START_YEAR = int(os.environ.get("START_YEAR", "2015"))
MAX_NUMBER_PER_YEAR = int(os.environ.get("MAX_NUMBER_PER_YEAR", "150"))
MISS_LIMIT = int(os.environ.get("MISS_LIMIT", "12"))

BASE_URL = "https://celnisprava.gov.cz"
SERIES_CONFIG = {
    "mi": {
        "label": "Metodicka informace",
        "url_pattern": BASE_URL + "/cz/Metodika/MI-{year}-{num:03d}.pdf",
    },
    "vp": {
        "label": "Vnitrni pokyn",
        "url_pattern": BASE_URL + "/cz/vyhlasky/Documents/VP-{num:03d}-{year}.pdf",
    },
}
REQ_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; AIsemantickeZakony/1.0)"}

START_TIME = time.time()


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
                source text not null default 'celni',
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


def fetch_existing_external_ids(conn, source="celni"):
    with conn.cursor() as cur:
        cur.execute("select external_id from documents where source = %s", (source,))
        return set(row[0] for row in cur.fetchall())


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


def is_letter_spaced(line):
    tokens = line.split()
    if len(tokens) < 3:
        return False
    single_char = sum(1 for t in tokens if len(t) == 1)
    return (single_char / len(tokens)) > 0.5


def guess_title(text, fallback):
    for line in text.split(NL):
        line = line.strip()
        if len(line) < 8:
            continue
        if is_letter_spaced(line):
            continue
        return line[:200]
    return fallback


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


def probe_pdf(url):
    try:
        resp = requests.get(url, headers=REQ_HEADERS, timeout=20, allow_redirects=True)
    except requests.RequestException:
        return None
    if resp.status_code != 200:
        return None
    ctype = resp.headers.get("Content-Type", "")
    if "pdf" not in ctype.lower() and resp.content[:4] != b"%PDF":
        return None
    return resp.content


def import_new_documents(conn):
    existing = fetch_existing_external_ids(conn)
    current_year = datetime.now().year
    new_count = 0
    with conn.cursor() as cur:
        for series, cfg in SERIES_CONFIG.items():
            for year in range(current_year, START_YEAR - 1, -1):
                if time_left() <= 0 or new_count >= MAX_NEW_DOCS_PER_RUN:
                    break
                consecutive_misses = 0
                for num in range(1, MAX_NUMBER_PER_YEAR + 1):
                    if time_left() <= 0 or new_count >= MAX_NEW_DOCS_PER_RUN:
                        break
                    external_id = series + ":" + str(year) + "-" + str(num).zfill(3)
                    file_url = cfg["url_pattern"].format(year=year, num=num)
                    if external_id in existing:
                        consecutive_misses = 0
                        continue
                    content = probe_pdf(file_url)
                    if content is None:
                        consecutive_misses += 1
                        if consecutive_misses >= MISS_LIMIT:
                            break
                        continue
                    consecutive_misses = 0
                    try:
                        body_text = extract_pdf_text(content)
                    except Exception as e:
                        print("WARN: pdf extract failed: " + file_url + " " + str(e))
                        existing.add(external_id)
                        continue
                    if not body_text or len(body_text) < 20:
                        print("SKIP (no text): " + file_url)
                        existing.add(external_id)
                        continue
                    fallback_title = cfg["label"] + " " + str(num).zfill(3) + "/" + str(year)
                    title = guess_title(body_text, fallback_title)
                    cur.execute(
                        "insert into documents (source, series, external_id, title, url, pdf_url, published_date, is_current) values (%s,%s,%s,%s,%s,%s,%s,%s) returning id",
                        ("celni", series, external_id, title, file_url, file_url, None, True),
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
                    print("IMPORTED: " + external_id + " " + title + " chunks=" + str(len(chunk_list)))
    print("Import done: " + str(new_count) + " new documents")


def embed_pending(conn, gemini_key):
    embedded = 0
    while time_left() > 30:
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
                print("WARN: embed failed: " + str(chunk_id) + " " + str(e))
                continue
            if not vec:
                continue
            vec_str = "[" + ",".join("%.8f" % x for x in vec) + "]"
            with conn.cursor() as cur2:
                cur2.execute("update chunks set embedding = %s::vector where id = %s", (vec_str, chunk_id))
            conn.commit()
            embedded += 1
    print("Embedding done: " + str(embedded) + " chunks")


def main():
    conn = psycopg2.connect(NEON_DB_URL, connect_timeout=15)
    try:
        ensure_schema(conn)
        import_new_documents(conn)
        gemini_key = get_admin_gemini_key()
        embed_pending(conn, gemini_key)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
