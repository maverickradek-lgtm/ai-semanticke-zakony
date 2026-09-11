"""
Sync Vestnik vlady - metodiky pro obce (Ministerstvo vnitra) do stejne
Neon databaze jako mv_metodika_sr (spravni rad) a mv_vestnik. Zdroj: ctyri
pevne HTML stranky na mv.gov.cz s primymi PDF odkazy (dokumenty s
disposition=attachment), zadne strankovani, zadny list+detail pruchod jako
u Vestniku vlady - jednodussi scraping.

Cast dat uz existuje v tabulce documents (38 radku, source=mv_metodika_obce,
vlozeno rucnim importem pres prohlizec) ale chunks temer vsem chybi (jen 1
dokument mel 25 chunku, zbylych 37 melo 0). Tento skript:
  1) nejdriv dobehne embedovani jakychkoliv chunku s embedding is null
     (embed_pending pass, kryje starou i novou frontu),
  2) pak projde 4 listing stranky, upsertne (source, external_id) radky v
     documents (aktualizuje title/url/pdf_url pri zmene stranky),
  3) pro dokumenty, ktere jeste nemaji zadne chunky, stahne PDF, rozseka na
     chunky a vlozi je,
  4) znovu zavola embed_pending, aby se nove vlozene chunky embedovaly hned
     v ramci stejneho behu, pokud zbyva casovy rozpocet.

Vsechny retezce v tomto souboru jsou schvalne bez zpetnych lomitek a bez
regexu (pouzity plain string metody split/strip) - stejna konvence jako
sync_celni_metodiky.py / sync_uoou_neon.py, kvuli riziku poskozeni pri
prenosu pres GitHub Contents API / JS template literal.
"""

import os
import time
from io import BytesIO

import requests
from bs4 import BeautifulSoup
from pypdf import PdfReader
import psycopg2


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
MAX_NEW_DOCS_PER_RUN = int(os.environ.get("MAX_NEW_DOCS_PER_RUN", "40"))
TIME_BUDGET_SECONDS = int(os.environ.get("TIME_BUDGET_SECONDS", "3000"))

SOURCE = "mv_metodika_obce"

PAGES = [
    {"url": "https://mv.gov.cz/cinnost-organu-obce-2", "series": "metodika_organy_obce"},
    {"url": "https://mv.gov.cz/obecne-zavazne-vyhlasky-obce-2", "series": "metodika_ozv"},
    {"url": "https://mv.gov.cz/odmenovani-clenu-zastupitelstev-2", "series": "metodika_odmenovani"},
    {"url": "https://mv.gov.cz/dalsi-metodicke-materialy-2", "series": "metodika_dalsi_materialy"},
]

# Tento konkretni slug se objevuje i na strance odmenovani-clenu-zastupitelstev-2
# jako duplicita dokumentu c. 5.6 ze stranky cinnost-organu-obce-2: tam ma slug
# "usc5-6", tady "uscc5-6" (o jedno "c" navic) - jde o stejny dokument publikovany
# 2x pod jinym URL slugem. Databaze uz obsahuje jen verzi usc5-6 (metodika_organy_obce),
# takze tuto uscc5-6 variantu pri scrapovani odmenovani stranky preskocime.
DUPLICATE_SLUG_SKIP = set([
    "metodickedoporucenikcinnostiuscc5-6odmenovaniclenuzastupitelstevobci-20241003",
])

REQ_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; AIsemantickeZakony/1.0)"}
SESSION = requests.Session()

START_TIME = time.time()


def log(*a):
    print(*a, flush=True)


def time_left():
    return TIME_BUDGET_SECONDS - (time.time() - START_TIME)


def db_connect(url, timeout=15):
    last_err = None
    for attempt in range(4):
        try:
            return psycopg2.connect(url, connect_timeout=timeout)
        except Exception as e:
            last_err = e
            log("db_connect selhalo (pokus " + str(attempt + 1) + "/4): " + str(e))
            time.sleep(3)
    raise last_err


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
            log("get_admin_gemini_key selhalo (pokus " + str(attempt + 1) + "/4): " + str(e))
            time.sleep(3)
    raise last_err


def ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute("create extension if not exists pgcrypto")
        cur.execute("create extension if not exists vector")
        cur.execute(
            "create table if not exists documents ("
            "id uuid primary key default gen_random_uuid(), "
            "source text not null default 'mv_metodika_obce', "
            "series text, "
            "external_id text not null, "
            "title text not null, "
            "url text not null, "
            "pdf_url text, "
            "published_date date, "
            "is_current boolean not null default true, "
            "fetched_at timestamptz default now(), "
            "created_at timestamptz default now(), "
            "unique(source, external_id)"
            ")"
        )
        cur.execute(
            "create table if not exists chunks ("
            "id uuid primary key default gen_random_uuid(), "
            "document_id uuid not null references documents(id) on delete cascade, "
            "chunk_index int not null, "
            "heading text, "
            "content text, "
            "embedding vector(256), "
            "created_at timestamptz default now()"
            ")"
        )
    conn.commit()


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


def fetch_page_items(page_url, series, seen_slugs):
    resp = SESSION.get(page_url, headers=REQ_HEADERS, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    items = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/documents/" not in href:
            continue
        slug = href.split("/documents/")[-1].split("?")[0]
        if not slug:
            continue
        if slug in DUPLICATE_SLUG_SKIP:
            continue
        if slug in seen_slugs:
            continue
        title = a.get_text(" ", strip=True)
        if "(Typ:" in title:
            title = title.split("(Typ:")[0].strip()
        if not title:
            continue
        seen_slugs.add(slug)
        items.append({
            "slug": slug,
            "title": title,
            "pdf_url": href,
            "page_url": page_url,
            "series": series,
        })
    return items


def upsert_document(conn, item):
    external_id = "mv:" + item["slug"]
    with conn.cursor() as cur:
        cur.execute(
            "insert into documents (source, series, external_id, title, url, pdf_url, is_current) "
            "values (%s,%s,%s,%s,%s,%s,%s) "
            "on conflict (source, external_id) do update set "
            "series=excluded.series, title=excluded.title, url=excluded.url, "
            "pdf_url=excluded.pdf_url, is_current=true, fetched_at=now() "
            "returning id",
            (SOURCE, item["series"], external_id, item["title"], item["page_url"], item["pdf_url"], True),
        )
        doc_id = cur.fetchone()[0]
    conn.commit()
    return doc_id, external_id


def chunk_count_for(conn, doc_id):
    with conn.cursor() as cur:
        cur.execute("select count(*) from chunks where document_id = %s", (doc_id,))
        return cur.fetchone()[0]


def download_and_chunk(conn, doc_id, external_id, item):
    try:
        resp = SESSION.get(item["pdf_url"], headers=REQ_HEADERS, timeout=30)
        if resp.status_code != 200 or (resp.content[:4] != b"%PDF" and "pdf" not in resp.headers.get("Content-Type", "").lower()):
            log("SKIP (neni PDF): " + item["pdf_url"] + " status=" + str(resp.status_code))
            return 0
        body_text = extract_pdf_text(resp.content)
    except Exception as e:
        log("WARN: stahovani/extrakce selhala: " + item["pdf_url"] + " " + str(e))
        return 0

    if not body_text or len(body_text) < 20:
        log("SKIP (malo textu): " + item["pdf_url"])
        return 0

    chunk_list = split_into_chunks(body_text)
    with conn.cursor() as cur:
        for i, ch in enumerate(chunk_list):
            cur.execute(
                "insert into chunks (document_id, chunk_index, heading, content) values (%s,%s,%s,%s)",
                (doc_id, i, item["title"], ch),
            )
    conn.commit()
    log("CHUNKED: " + external_id + " " + item["title"] + " chunks=" + str(len(chunk_list)))
    return len(chunk_list)


def sync_documents(conn):
    seen_slugs = set()
    all_items = []
    for page in PAGES:
        try:
            items = fetch_page_items(page["url"], page["series"], seen_slugs)
            log("Nalezeno na strance " + page["url"] + ": " + str(len(items)) + " PDF odkazu")
            all_items.extend(items)
        except Exception as e:
            log("WARN: nepodarilo se nacist stranku " + page["url"] + ": " + str(e))

    new_chunks_docs = 0
    for item in all_items:
        if time_left() <= 0:
            log("Dosazen casovy rozpocet, zbytek doplni dalsi beh.")
            break
        conn = ensure_conn(conn)
        try:
            doc_id, external_id = upsert_document(conn, item)
        except Exception as e:
            log("WARN: upsert selhal pro " + item["slug"] + ": " + str(e))
            continue
        existing_chunks = chunk_count_for(conn, doc_id)
        if existing_chunks > 0:
            continue
        if new_chunks_docs >= MAX_NEW_DOCS_PER_RUN:
            continue
        conn = ensure_conn(conn)
        n = download_and_chunk(conn, doc_id, external_id, item)
        if n > 0:
            new_chunks_docs += 1

    log("Sync dokumentu hotovo: " + str(len(all_items)) + " celkem na strankach, " + str(new_chunks_docs) + " dokumentu nove rozsekano na chunky")
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
    log("=== MV metodiky pro obce sync (Neon): start ===")
    conn = db_connect(NEON_DB_URL)
    try:
        ensure_schema(conn)
        gemini_key = get_admin_gemini_key()
        conn = embed_pending(conn, gemini_key)
        conn = sync_documents(conn)
        conn = embed_pending(conn, gemini_key)
    finally:
        conn.close()
    log("=== MV metodiky pro obce sync (Neon): hotovo ===")


if __name__ == "__main__":
    main()
