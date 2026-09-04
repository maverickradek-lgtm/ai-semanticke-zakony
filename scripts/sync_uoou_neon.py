"""
Fetcher pro metodiky a doporuceni UOOU (Urad pro ochranu osobnich udaju) -
PRIMY zapis do samostatne Neon databaze (stejny vzor jako
scripts/sync_uohs_neon.py / scripts/sync_celni_metodiky.py).

Zdroj: uoou.gov.cz nema zadne strukturovane opendata ani JSON API - obsah je
cist HTML. Skript proto funguje jako maly cilenych crawler: ze sady rucne
vybranych "rubrik" (hlavnich rozcestniku v sekci /profesional a
zakladni prirucky v /verejnost) nasbira odkazy na jednotlive clanky
(metodiky, doporuceni, Q&A, atd.) a kazdy stahne jako HTML detail stranku -
stejnou technikou jako HTML fallback v sync_uohs_neon.py (BeautifulSoup,
strip nav/header/footer, text z <main> nebo <body>).

Zamerne VYNECHANO: rozcestnik "Predavani osobnich udaju do tretich zemi" ma
jednu podstranku se stovkami byrokratickych schvaleni BCR/stanovisek EDPB ke
konkretnim firmam (napr. "Stanovisko 28/2021 k navrhu rozhodnuti... BCR-C
spolecnosti X") - nizka relevance pro obecne pravni vyhledavani, vysoky
objem. URL_EXCLUDE_PATTERNS tohle vyfiltruje.

Vsechny retezce v tomto souboru jsou schvalne bez zpetnych lomitek (pouzito
chr() tam, kde je potreba odradkovani/tabulator) kvuli riziku poskozeni pri
prenosu pres GitHub Contents API / JS template literal - stejna konvence
jako sync_uohs_neon.py.
"""

import os
import re
import time
from urllib.parse import urljoin, urlparse

import requests
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
from bs4 import BeautifulSoup

NL = chr(10)
TAB = chr(9)

NEON_DB_URL = os.environ["NEON_UOOU_DB_URL"]
GEMINI_API_KEY_OVERRIDE = os.environ.get("GEMINI_API_KEY_OVERRIDE")
GEMINI_API_KEY_POOL = [k.strip() for k in os.environ.get("GEMINI_API_KEY_POOL", "").split(",") if k.strip()]

EMBED_MODEL = "gemini-embedding-001"
EMBED_DIM = 256

BASE = "https://uoou.gov.cz"

# Rucne vybrane rozcestniky - viz [[new_authority_expansion_research_2026-08-28]]
# pro duvod vyberu (profesional/predavani-osobnich-udaju-do-tretich-zemi-1 je
# zahrnuty jen jako rozcestnik, ne jeho velky "stanoviska" podstrom).
INDEX_PAGES = [
    BASE + "/profesional/qa-otazky-a-odpovedi",
    BASE + "/profesional/poverenec-pro-ochranu-osobnich-udaju",
    BASE + "/profesional/poruseni-zabezpeceni-osobnich-udaju",
    BASE + "/profesional/predavani-osobnich-udaju-do-tretich-zemi-1",
    BASE + "/profesional/metodiky-a-doporuceni-pro-spravce",
    BASE + "/profesional/posouzeni-vlivu-na-ochranu-osobnich-udaju-dpia",
    BASE + "/profesional/hodnoceni-shody-s-gdpr",
    BASE + "/verejnost/zakladni-prirucka-k-ochrane-udaju",
]

# Odkazy obsahujici tyto retezce se nikdy nenasledujou (velky nizko-relevantni
# BCR/stanoviska podstrom, ktery by jinak nafoukl objem na stovky zaznamu).
URL_EXCLUDE_PATTERNS = [
    "/pokyny-doporuceni-a-stanoviska-sboru-k-predavani/stanoviska",
    "/media-publikace/",
    "/urad/",
    "/pravni-ramec/",
    "/kontakt",
    "/cookies",
    "/sitemap",
    "/newsletter",
    "/en/",
    "/protikorupcni-opatreni",
    "/prohlaseni-o-pristupnosti",
    "/informace-o-zpracovani-osobnich-udaju",
]

MAX_CHARS_PER_CHUNK = int(os.environ.get("MAX_CHARS_PER_CHUNK", "4000"))
MAX_ITEMS = int(os.environ.get("MAX_ITEMS", "300"))
TIME_BUDGET_SECONDS = int(os.environ.get("TIME_BUDGET_SECONDS", "3000"))

SESSION = requests.Session()
REQ_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; AIsemantickeZakony/1.0)"}

START_TIME = time.time()
_key_rotation_idx = [0]


def log(*a):
    print(*a, flush=True)


def time_left():
    return TIME_BUDGET_SECONDS - (time.time() - START_TIME)


def get_gemini_keys():
    if GEMINI_API_KEY_POOL:
        return GEMINI_API_KEY_POOL
    if GEMINI_API_KEY_OVERRIDE:
        return [GEMINI_API_KEY_OVERRIDE]
    raise RuntimeError("Zadny Gemini klic k dispozici (GEMINI_API_KEY_POOL / GEMINI_API_KEY_OVERRIDE)")


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
    new_conn = db_connect(NEON_DB_URL)
    log("(znovu navazano spojeni)")
    return new_conn


def fetch_existing_external_ids(conn):
    with conn.cursor() as cur:
        cur.execute("select external_id from documents where source = 'uoou'")
        return set(row[0] for row in cur.fetchall())


def clean_html_text(soup):
    for tag in soup(["script", "style", "nav", "header", "footer", "form"]):
        tag.decompose()
    main = soup.find("main") or soup.find("body")
    if main is None:
        return None
    text = main.get_text(separator=NL)
    text = re.sub(r"[ " + TAB + "]+", " ", text)
    text = re.sub(NL + "{3,}", NL + NL, text)
    return text.strip()


def fetch_page(url):
    try:
        r = SESSION.get(url, headers=REQ_HEADERS, timeout=30)
        if not r.ok:
            return None
        return r.text
    except Exception:
        return None


def is_excluded(url):
    for pat in URL_EXCLUDE_PATTERNS:
        if pat in url:
            return True
    return False


def discover_article_links(index_url):
    html = fetch_page(index_url)
    if not html:
        log("  Nepodarilo se stahnout rozcestnik: " + index_url)
        return []
    soup = BeautifulSoup(html, "html.parser")
    main = soup.find("main") or soup.find("body")
    if main is None:
        return []
    links = []
    seen = set()
    for a in main.find_all("a", href=True):
        href = a["href"]
        full = urljoin(index_url, href)
        parsed = urlparse(full)
        if parsed.netloc and parsed.netloc != urlparse(BASE).netloc:
            continue
        full = full.split("#")[0].rstrip("/")
        if full == index_url.rstrip("/"):
            continue
        if is_excluded(full):
            continue
        if full in seen:
            continue
        seen.add(full)
        links.append(full)
    return links


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


def upsert_document(conn, url, title_hint):
    html = fetch_page(url)
    if not html:
        log("  Stahovani selhalo: " + url)
        return None, None
    soup = BeautifulSoup(html, "html.parser")
    title_tag = soup.find("h1")
    title = (title_tag.get_text(strip=True) if title_tag else None) or title_hint or url

    text = clean_html_text(soup)
    if not text or len(text) < 200:
        log("  Malo textu, preskakuji: " + url)
        return None, None

    external_id = urlparse(url).path.strip("/")

    with conn.cursor() as cur:
        cur.execute(
            "insert into documents (source, series, external_id, title, url, is_current) "
            "values (%s,%s,%s,%s,%s,%s) returning id",
            ("uoou", "metodika", external_id, title, url, True),
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
    log("Jiz v databazi: " + str(len(existing)) + " dokumentu UOOU")

    all_links = {}
    for idx_url in INDEX_PAGES:
        log("Prochazim rozcestnik: " + idx_url)
        links = discover_article_links(idx_url)
        log("  nalezeno " + str(len(links)) + " odkazu")
        for link in links:
            all_links.setdefault(link, idx_url)

    log("Celkem unikatnich kandidatnich stranek: " + str(len(all_links)))

    imported = 0
    skipped = 0
    errors = 0
    for url, idx_url in all_links.items():
        external_id = urlparse(url).path.strip("/")
        if external_id in existing:
            skipped += 1
            continue
        if MAX_ITEMS and imported >= MAX_ITEMS:
            log("Dosazen MAX_ITEMS=" + str(MAX_ITEMS) + ", koncim (zbytek doplni dalsi beh).")
            break
        if time_left() <= 0:
            log("Casovy rozpocet vycerpan, koncim cist (zbytek doplni dalsi beh).")
            break
        conn = ensure_conn(conn)
        try:
            doc_id, n_chunks = upsert_document(conn, url, None)
            if doc_id:
                imported += 1
                existing.add(external_id)
                log("  + " + url + " (" + str(n_chunks) + " chunku)")
            else:
                errors += 1
        except Exception as e:
            log("  Chyba u " + url + ": " + str(e))
            errors += 1

    log("Import done: " + str(imported) + " new, skipped=" + str(skipped) + " errors=" + str(errors))
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
    log("=== UOOU sync (Neon): start ===")
    log("MAX_ITEMS=" + str(MAX_ITEMS))
    conn = db_connect(NEON_DB_URL)
    try:
        conn = import_new_documents(conn)
        gemini_keys = get_gemini_keys()
        log("Pocet Gemini klicu v rotaci: " + str(len(gemini_keys)))
        conn = embed_pending(conn, gemini_keys)
    finally:
        conn.close()
    log("=== UOOU sync (Neon): hotovo ===")


if __name__ == "__main__":
    main()
