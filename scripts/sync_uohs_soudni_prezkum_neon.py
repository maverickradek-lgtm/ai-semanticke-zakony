"""
Fetcher pro soudni prezkum rozhodnuti UOHS (rozsudky krajskych soudu a
nalezy Ustavniho soudu, ktere prezkoumavaly rozhodnuti UOHS ve verejnych
zakazkach) - PRIMY zapis do stavajici Neon databaze UOHS (stejny projekt
jako sync_uohs_neon.py, jen jiny "source").

Zdroj: HTML tabulka na
https://uohs.gov.cz/cs/verejne-zakazky/soudni-prezkum-rozhodnuti.html
(rozsudky od r. 2003 dosud) a
https://uohs.gov.cz/cs/verejne-zakazky/soudni-prezkum-rozhodnuti/rozsudky-do-roku-2003.html
(pred r. 2003). Tabulka paruje cisla jednani u soudu (Ustavni/NSS/Krajsky)
s rozhodnutimi UOHS a ucastniky rizeni. Vetsina rozsudku krajskych soudu a
nalezu Ustavniho soudu je tam primo jako PDF odkaz hostovany na
www.uohs.cz/download/sbirky_rozhodnuti/rozsudky_VZ/*.pdf - zadne scrapovani
cizich portalu.

DULEZITE: rozsudky Nejvyssiho spravniho soudu (NSS) v teto tabulce jsou
ZAMERNE VYNECHANY (hostovane na nssoud.cz) - ty uz mame z vlastniho NSS
zdroje (sync_sbirka_ns*), pridavat je znovu by byla duplicita.

Existujici radky v teto Neon databazi maji source='uohs' (rozhodnuti UOHS
samotneho, viz sync_uohs_neon.py). Tento skript pise se source='soudni_prezkum'
aby se dalo v UI/ai-query rozlisit - stejny Neon projekt, jiny "kanal".

Idempotentni: pred vlozenim kontroluje, jestli uz dany PDF (podle url) v DB
neni - bezpecne pouzitelne jako opakovany (tydenni) sync na nove pribyle
zaznamy, ne jen jednorazovy import.
"""
import os
import re
import sys
import time
import signal
import hashlib
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup
import psycopg2
import psycopg2.extras
from pypdf import PdfReader
from io import BytesIO


class HardTimeout(Exception):
    """Vyvolano, kdyz sitove volani prekroci tvrdy wall-clock limit (napr.
    DNS resolver se zasekne - to standardni 'timeout=' parametr v requests
    nepokryje, protoze getaddrinfo() neni omezen socket timeoutem)."""
    pass


def _hard_timeout_handler(signum, frame):
    raise HardTimeout("tvrdy timeout - sitove volani se zaseklo (mozna DNS)")


def call_with_hard_timeout(seconds, fn, *args, **kwargs):
    """Spusti fn(*args, **kwargs) s tvrdym wall-clock limitem pres SIGALRM.
    Funguje i kdyz se zasekne DNS resolver uvnitr requests (coz obycejny
    'timeout=' parametr nezachyti). Jen na Linuxu (self-hosted runner je
    Linux Docker kontejner, takze OK)."""
    old_handler = signal.signal(signal.SIGALRM, _hard_timeout_handler)
    signal.alarm(seconds)
    try:
        return fn(*args, **kwargs)
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old_handler)

DB_URL = os.environ["NEON_UOHS_DB_URL"]
GEMINI_KEY_POOL = [k.strip() for k in os.environ.get("GEMINI_API_KEY_POOL", os.environ.get("GEMINI_API_KEY", "")).split(",") if k.strip()]

EMBED_MODEL = "gemini-embedding-001"
EMBED_DIM = 256
MAX_CHARS_PER_CHUNK = int(os.environ.get("MAX_CHARS_PER_CHUNK", "4000"))
MAX_ITEMS = int(os.environ.get("MAX_ITEMS", "150"))
TIME_BUDGET_SECONDS = int(os.environ.get("TIME_BUDGET_SECONDS", "3000"))
MIN_PDF_TEXT_CHARS = 200  # skenovane/nedigitalizovane PDF -> preskocit, ne rozbit

LIST_URLS = [
    "https://uohs.gov.cz/cs/verejne-zakazky/soudni-prezkum-rozhodnuti.html",
    "https://uohs.gov.cz/cs/verejne-zakazky/soudni-prezkum-rozhodnuti/rozsudky-do-roku-2003.html",
]

SESSION = requests.Session()
REQ_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; AIsemantickeZakony/1.0)"}

START_TIME = time.time()
_key_rotation_idx = [0]


def log(*a):
    print(*a, flush=True)


def time_left():
    return TIME_BUDGET_SECONDS - (time.time() - START_TIME)


def next_gemini_key():
    if not GEMINI_KEY_POOL:
        return None
    k = GEMINI_KEY_POOL[_key_rotation_idx[0] % len(GEMINI_KEY_POOL)]
    _key_rotation_idx[0] += 1
    return k


def embed_text(text, retries=3):
    last_err = None
    for attempt in range(retries):
        key = next_gemini_key()
        if not key:
            return None
        try:
            resp = call_with_hard_timeout(
                45,
                requests.post,
                f"https://generativelanguage.googleapis.com/v1beta/models/{EMBED_MODEL}:embedContent",
                headers={"Content-Type": "application/json", "x-goog-api-key": key},
                json={
                    "content": {"parts": [{"text": (text or "")[:8000]}]},
                    "taskType": "RETRIEVAL_DOCUMENT",
                    "outputDimensionality": EMBED_DIM,
                },
                timeout=30,
            )
            if resp.status_code == 429:
                last_err = "429 rate limited"
                time.sleep(2)
                continue
            resp.raise_for_status()
            data = resp.json()
            return data["embedding"]["values"]
        except Exception as e:
            last_err = str(e)
            time.sleep(2)
    log(f"  ! embed failed after {retries} retries: {last_err}")
    return None


def db_connect():
    last_err = None
    for attempt in range(4):
        try:
            conn = psycopg2.connect(DB_URL, connect_timeout=15)
            conn.autocommit = True
            return conn
        except Exception as e:
            last_err = e
            log(f"  ! db_connect selhalo (pokus {attempt + 1}/4): {e}")
            time.sleep(3)
    raise last_err


def ensure_conn(conn):
    try:
        cur = conn.cursor()
        cur.execute("select 1")
        cur.close()
        return conn
    except Exception:
        try:
            conn.close()
        except Exception:
            pass
        log("  (reconnecting to Neon...)")
        return db_connect()


COURT_AGENDA_KRAJSKY = {"Af", "Ca", "A"}  # pomocne, jen pro dohledatelnost, neni prisne pouzito


def classify_issuer(case_num: str) -> str:
    if "ÚS" in case_num or "US" in case_num.upper().replace(" ", ""):
        return "Ústavní soud"
    return "Krajský soud"


def clean_case_num(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


# Radek 2026-09-04: jen rozhodnuti od roku 2016 (stejny cutoff jako u
# samotnych rozhodnuti UOHS - ucinnost zakona 134/2016 Sb.) - stare
# skenovane rozsudky (2003-2015) se uz nemaji ani stahovat, ani embedovat.
CUTOFF_YEAR = int(os.environ.get("CUTOFF_YEAR", "2016"))

def extract_year(case_num):
    m = re.search(r"/(\d{4})\b", case_num or "")
    if m:
        return int(m.group(1))
    m2 = re.search(r"/(\d{2})\b", case_num or "")
    if m2:
        yy = int(m2.group(1))
        return 2000 + yy if yy <= 79 else 1900 + yy
    return None

def fetch_pdf_links():
    """Vrati list dictu {case_num, url, issuer}, dedup podle url, bez nssoud.cz odkazu."""
    seen_urls = set()
    items = []
    skipped_old_year = 0
    for list_url in LIST_URLS:
        try:
            resp = call_with_hard_timeout(45, SESSION.get, list_url, headers=REQ_HEADERS, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            log(f"! nepodarilo se nacist {list_url}: {e}")
            continue
        soup = BeautifulSoup(resp.text, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if not href.lower().endswith(".pdf"):
                continue
            if "nssoud.cz" in href:
                continue  # NSS uz mame z vlastniho zdroje - zamerne preskoceno
            if "uohs.cz" not in href and "uohs.gov.cz" not in href:
                continue
            if href in seen_urls:
                continue
            seen_urls.add(href)
            case_num = clean_case_num(a.get_text())
            if not case_num:
                continue
            year = extract_year(case_num)
            if year is not None and year < CUTOFF_YEAR:
                skipped_old_year += 1
                continue
            items.append({
                "case_num": case_num,
                "url": href,
                "issuer": classify_issuer(case_num),
                "year": year,
            })
    log(f"Nalezeno {len(items)} unikatnich PDF odkazu od roku {CUTOFF_YEAR} (krajske soudy + Ustavni soud, NSS vynechan). Preskoceno (pred rokem {CUTOFF_YEAR}): {skipped_old_year}.")
    return items


def existing_urls(conn, urls):
    """Vraci (mnozina existujicich url, aktualni/pripadne obnovene spojeni)."""
    if not urls:
        return set(), conn
    out = set()
    CHUNK = 500
    urls = list(urls)
    for i in range(0, len(urls), CHUNK):
        batch = urls[i:i + CHUNK]
        for attempt in range(2):
            try:
                conn = ensure_conn(conn)
                cur = conn.cursor()
                cur.execute("select url from documents where source = 'soudni_prezkum' and url = any(%s)", (batch,))
                out.update(r[0] for r in cur.fetchall())
                cur.close()
                break
            except Exception as e:
                if attempt == 0:
                    log(f"  ! existing_urls dotaz selhal, zkousim znovu: {e}")
                    conn = ensure_conn(conn)
                else:
                    raise
    return out, conn


def extract_pdf_text(pdf_bytes: bytes) -> str:
    try:
        reader = PdfReader(BytesIO(pdf_bytes))
        parts = []
        for page in reader.pages:
            try:
                parts.append(page.extract_text() or "")
            except Exception:
                continue
        return "\n\n".join(parts).strip()
    except Exception as e:
        log(f"  ! pypdf selhalo: {e}")
        return ""


def chunk_text(text: str, max_chars=MAX_CHARS_PER_CHUNK):
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    if not text:
        return []
    paras = re.split(r"\n\s*\n", text)
    chunks = []
    cur = ""
    for p in paras:
        p = p.strip()
        if not p:
            continue
        if len(cur) + len(p) + 2 > max_chars and cur:
            chunks.append(cur.strip())
            cur = p
        else:
            cur = (cur + "\n\n" + p) if cur else p
        while len(cur) > max_chars:
            chunks.append(cur[:max_chars].strip())
            cur = cur[max_chars:]
    if cur.strip():
        chunks.append(cur.strip())
    return chunks


def insert_document(conn, item, chunks):
    cur = conn.cursor()
    title = f"Rozsudek {item['issuer']} – sp. zn. {item['case_num']}" if item["issuer"] == "Krajský soud" \
        else f"Nález {item['issuer']} – sp. zn. {item['case_num']}"
    ext_id = hashlib.sha1(item["url"].encode("utf-8")).hexdigest()[:16]
    cur.execute(
        """
        insert into documents (source, external_id, title, issuer, url, decision_date, status, is_current, fetched_at, created_at)
        values ('soudni_prezkum', %s, %s, %s, %s, null, 'platny', true, now(), now())
        returning id
        """,
        (ext_id, title, item["issuer"], item["url"]),
    )
    doc_id = cur.fetchone()[0]
    rows = [(doc_id, i, None, c) for i, c in enumerate(chunks)]
    psycopg2.extras.execute_values(
        cur,
        "insert into chunks (document_id, chunk_index, heading, content) values %s",
        rows,
    )
    cur.close()
    return doc_id


def main():
    conn = db_connect()
    items = fetch_pdf_links()
    urls_to_check = [it["url"] for it in items]
    already, conn = existing_urls(conn, urls_to_check)
    new_items = [it for it in items if it["url"] not in already]
    log(f"Uz v databazi: {len(already)}. Novych ke zpracovani: {len(new_items)} (limit tohoto behu: {MAX_ITEMS}).")

    processed = 0
    skipped_no_text = 0
    failed_fetch = 0

    for item in new_items:
        if processed >= MAX_ITEMS:
            log("Dosazen MAX_ITEMS limit pro tento beh, zbytek na priste.")
            break
        if time_left() < 60:
            log("Dochazi casovy rozpocet, koncim tento beh.")
            break

        conn = ensure_conn(conn)
        try:
            pdf_resp = call_with_hard_timeout(45, SESSION.get, item["url"], headers=REQ_HEADERS, timeout=30)
            pdf_resp.raise_for_status()
        except Exception as e:
            log(f"! stazeni selhalo ({item['case_num']}): {e}")
            failed_fetch += 1
            continue

        text = extract_pdf_text(pdf_resp.content)
        if len(text) < MIN_PDF_TEXT_CHARS:
            log(f"- preskakuji {item['case_num']} (extrahovany text jen {len(text)} znaku - pravdepodobne sken bez textove vrstvy)")
            skipped_no_text += 1
            continue

        chunks = chunk_text(text)
        if not chunks:
            skipped_no_text += 1
            continue

        try:
            conn = ensure_conn(conn)
            doc_id = insert_document(conn, item, chunks)
        except Exception as e:
            log(f"! insert selhal ({item['case_num']}): {e}")
            conn = ensure_conn(conn)
            continue

        embedded = 0
        conn = ensure_conn(conn)
        cur = conn.cursor()
        cur.execute("select id, content from chunks where document_id = %s order by chunk_index", (doc_id,))
        rows = cur.fetchall()
        cur.close()
        for chunk_id, content in rows:
            vec = embed_text(content)
            if vec is None:
                continue
            vec_str = "[" + ",".join(f"{x:.8f}" for x in vec) + "]"
            try:
                conn = ensure_conn(conn)
                cur2 = conn.cursor()
                cur2.execute("update chunks set embedding = %s::vector where id = %s", (vec_str, chunk_id))
                cur2.close()
                embedded += 1
            except Exception as e:
                log(f"  ! update embeddingu selhal pro chunk {chunk_id}: {e}")
                conn = ensure_conn(conn)

        log(f"+ {item['case_num']} ({item['issuer']}): {len(chunks)} chunku, {embedded} naembedovano")
        processed += 1
        time.sleep(0.3)

    log(f"\nHotovo. Zpracovano: {processed}, preskoceno (bez textu): {skipped_no_text}, chyba stazeni: {failed_fetch}.")
    conn.close()


if __name__ == "__main__":
    main()
