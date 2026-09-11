"""
Sync metodiky MMR k novemu stavebnimu zakonu (283/2021 Sb.) do samostatne
Neon databaze. Zdroj: https://mmr.gov.cz/cs/ministerstvo/stavebni-pravo/
pravo-a-legislativa/novy-stavebni-zakon/metodiky (vlastni CMS, ne Drupal,
3 stranky vypisu pres ?page=N, cca 29 zaznamu). Kazdy zaznam ma na detailni
strance jeden nebo vice PDF odkazu typu /getattachment/.../nazev.pdf.aspx
(pripona ".pdf.aspx" ne primy ".pdf" soubor, ale obsah je skutecne PDF).

Datum zverejneni se bere primo z vypisu (div.date, format "D. M. YYYY"),
detailni stranka navic ukazuje "Aktualizovano ke dni D. mesice YYYY" (cesky
nazev mesice), ale to se nepouziva - vypis ma uz numericke datum, jednodussi
a spolehlivejsi parsovat.

Pipeline mirrors scripts/sync_eru_metodiky.py (stejne schema, stejny
admin-Gemini-key embedding pattern, stejny retry-on-DNS-blip db_connect).
"""

import os
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
            conn = psycopg2.connect(url, connect_timeout=timeout)
            conn.autocommit = False
            return conn
        except Exception as e:
            last_err = e
            print("db_connect selhalo (pokus " + str(attempt + 1) + "/4): " + str(e), flush=True)
            time.sleep(3)
    raise last_err


def ensure_conn(conn):
    try:
        with conn.cursor() as cur:
            cur.execute("select 1")
        return conn
    except Exception:
        try:
            conn.close()
        except Exception:
            pass
        return db_connect(NEON_DB_URL)


NL = chr(10)
TAB = chr(9)

NEON_DB_URL = os.environ["NEON_MMR_DB_URL"]
SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SERVICE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
ADMIN_USER_ID = "2648f5db-bea6-4cac-b490-ad0ec59723df"
GEMINI_API_KEY_OVERRIDE = os.environ.get("GEMINI_API_KEY_OVERRIDE")

EMBED_MODEL = "gemini-embedding-001"
EMBED_DIM = 256
MAX_CHARS_PER_CHUNK = 3000
MAX_NEW_DOCS_PER_RUN = int(os.environ.get("MAX_NEW_DOCS_PER_RUN", "100"))
TIME_BUDGET_SECONDS = int(os.environ.get("TIME_BUDGET_SECONDS", "3000"))

BASE = "https://mmr.gov.cz"
LISTING_URL = BASE + "/cs/ministerstvo/stavebni-pravo/pravo-a-legislativa/novy-stavebni-zakon/metodiky"
MAX_LISTING_PAGES = 6

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
    last_err = None
    for attempt in range(4):
        try:
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
        except Exception as e:
            last_err = e
            print("get_admin_gemini_key selhalo (pokus " + str(attempt + 1) + "/4): " + str(e), flush=True)
            time.sleep(3)
    raise last_err


def ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute("create extension if not exists pgcrypto")
        cur.execute("create extension if not exists vector")
        cur.execute(
            """
            create table if not exists documents (
                id uuid primary key default gen_random_uuid(),
                source text not null default 'metodika_mmr',
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
                chunk_index integer not null,
                content text not null,
                embedding vector(""" + str(EMBED_DIM) + """),
                created_at timestamptz default now()
            )
            """
        )
        cur.execute("create index if not exists chunks_document_id_idx on chunks(document_id)")
        cur.execute(
            "create index if not exists chunks_embedding_hnsw_idx on chunks using hnsw (embedding vector_cosine_ops)"
        )
    conn.commit()


def fetch_existing_external_ids(conn, source="metodika_mmr"):
    with conn.cursor() as cur:
        cur.execute("select external_id from documents where source = %s", (source,))
        return set(row[0] for row in cur.fetchall())


def parse_ddmmyyyy(text):
    text = text.strip()
    parts = text.split(".")
    if len(parts) != 3:
        return None
    d, m, y = parts[0].strip(), parts[1].strip(), parts[2].strip()
    if not (d.isdigit() and m.isdigit() and y.isdigit()):
        return None
    return y + "-" + m.zfill(2) + "-" + d.zfill(2)


def fetch_listing():
    items = []
    seen = set()
    for page in range(1, MAX_LISTING_PAGES + 1):
        url = LISTING_URL if page == 1 else LISTING_URL + "?page=" + str(page)
        resp = SESSION.get(url, headers=REQ_HEADERS, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        lis = soup.select("ul.multiple-article-table li")
        if not lis:
            break
        found_new = False
        for li in lis:
            a = li.find("a", href=True)
            if a is None:
                continue
            href = a.get("href")
            if not href or href in seen:
                continue
            seen.add(href)
            found_new = True
            h3 = a.find("h3")
            title = h3.get_text(" ", strip=True) if h3 is not None else href.rstrip("/").split("/")[-1]
            date_div = a.find("div", class_="date")
            date_text = date_div.get_text(strip=True) if date_div is not None else ""
            published = parse_ddmmyyyy(date_text)
            external_id = href.rstrip("/").split("/")[-1]
            items.append({
                "href": href,
                "external_id": external_id,
                "title": title,
                "published": published,
            })
        if not found_new:
            break
        time.sleep(1)
    return items


def fetch_detail(href):
    detail_url = BASE + href if href.startswith("/") else href
    resp = SESSION.get(detail_url, headers=REQ_HEADERS, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    pdf_urls = []
    for a in soup.find_all("a", href=True):
        h = a.get("href")
        if "getattachment" in h.lower():
            full = h if h.startswith("http") else BASE + h
            if full not in pdf_urls:
                pdf_urls.append(full)
    return {"detail_url": detail_url, "pdf_urls": pdf_urls}


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
        if len(p) > max_chars:
            start = 0
            while start < len(p):
                chunks.append(p[start:start + max_chars])
                start += max_chars
            continue
        current.append(p)
        current_len += len(p)
    if current:
        chunks.append(sep.join(current))
    return chunks if chunks else [text[:max_chars]]


def embed_text(text, gemini_key, retries=3):
    payload = {
        "model": "models/" + EMBED_MODEL,
        "content": {"parts": [{"text": text[:8000]}]},
        "outputDimensionality": EMBED_DIM,
    }
    url = "https://generativelanguage.googleapis.com/v1beta/models/" + EMBED_MODEL + ":embedContent"
    for attempt in range(retries):
        try:
            r = requests.post(url, headers={"x-goog-api-key": gemini_key, "Content-Type": "application/json"}, json=payload, timeout=30)
            if r.status_code == 429:
                wait = 20 * (attempt + 1)
                log("Gemini 429, cekam " + str(wait) + "s")
                time.sleep(wait)
                continue
            r.raise_for_status()
            data = r.json()
            return data.get("embedding", {}).get("values")
        except Exception as e:
            log("embed_text chyba (pokus " + str(attempt + 1) + "/" + str(retries) + "): " + str(e))
            time.sleep(5)
    return None


def import_new_documents(conn):
    existing = fetch_existing_external_ids(conn, source="metodika_mmr")
    listing = fetch_listing()
    log("Nalezeno " + str(len(listing)) + " zaznamu v seznamu")
    new_count = 0
    for item in listing:
        if new_count >= MAX_NEW_DOCS_PER_RUN:
            log("Dosazen limit MAX_NEW_DOCS_PER_RUN, koncim import")
            break
        if time_left() < 120:
            log("Dochazi casovy rozpocet, koncim import")
            break
        ext_id = item["external_id"]
        if ext_id in existing:
            continue
        try:
            detail = fetch_detail(item["href"])
        except Exception as e:
            log("Chyba pri stahovani detailu " + item["href"] + ": " + str(e))
            continue
        pdf_urls = detail["pdf_urls"]
        if not pdf_urls:
            log("Preskakuji (bez PDF): " + item["title"])
            existing.add(ext_id)
            continue
        doc_idx = 0
        for pdf_url in pdf_urls:
            doc_idx += 1
            this_ext_id = ext_id if doc_idx == 1 else ext_id + "-" + str(doc_idx)
            if this_ext_id in existing:
                continue
            try:
                pr = SESSION.get(pdf_url, headers=REQ_HEADERS, timeout=60)
                pr.raise_for_status()
                text = extract_pdf_text(pr.content)
            except Exception as e:
                log("Chyba pri stahovani/extrakci PDF " + pdf_url + ": " + str(e))
                continue
            if not text or len(text) < 50:
                log("Preskakuji (prazdny text): " + item["title"])
                existing.add(this_ext_id)
                continue
            title = item["title"] if doc_idx == 1 else item["title"] + " (priloha " + str(doc_idx) + ")"
            conn = ensure_conn(conn)
            with conn.cursor() as cur:
                cur.execute(
                    "insert into documents (source, series, external_id, title, url, pdf_url, published_date, is_current) values (%s,%s,%s,%s,%s,%s,%s,%s) on conflict (source, external_id) do nothing returning id",
                    ("metodika_mmr", None, this_ext_id, title, detail["detail_url"], pdf_url, item["published"], True),
                )
                row = cur.fetchone()
            conn.commit()
            existing.add(this_ext_id)
            if row is None:
                continue
            doc_id = row[0]
            chunks = split_into_chunks(text)
            with conn.cursor() as cur:
                for i, chunk_text in enumerate(chunks):
                    cur.execute(
                        "insert into chunks (document_id, chunk_index, content) values (%s,%s,%s)",
                        (doc_id, i, chunk_text),
                    )
            conn.commit()
            new_count += 1
            log("Naimportovano: " + title + " (" + str(len(chunks)) + " chunku)")
        time.sleep(1)
    log("Import hotovo, novych dokumentu: " + str(new_count))
    return conn


def embed_pending(conn, gemini_key):
    embedded = 0
    while time_left() > 60:
        conn = ensure_conn(conn)
        with conn.cursor() as cur:
            cur.execute(
                "select c.id, c.content from chunks c join documents d on d.id = c.document_id where c.embedding is null and d.is_current = true order by c.created_at limit 20"
            )
            rows = cur.fetchall()
        if not rows:
            break
        for chunk_id, content in rows:
            if time_left() < 30:
                break
            try:
                vec = embed_text(content, gemini_key)
            except Exception as e:
                log("Embed selhal pro chunk " + str(chunk_id) + ": " + str(e))
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
    log("=== MMR metodiky sync (Neon): start ===")
    conn = db_connect(NEON_DB_URL)
    try:
        ensure_schema(conn)
        conn = import_new_documents(conn)
        gemini_key = get_admin_gemini_key()
        conn = embed_pending(conn, gemini_key)
    finally:
        conn.close()
    log("=== MMR metodiky sync (Neon): hotovo ===")


if __name__ == "__main__":
    main()
