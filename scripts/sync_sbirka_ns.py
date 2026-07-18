#!/usr/bin/env python3
"""
sync_sbirka_ns.py — Import judikátů ze Sbírky soudních rozhodnutí a stanovisek
Nejvyššího soudu ČR (sbirka.nsoud.cz) do Supabase.

Fáze A — ukládá jen text (documents + chunks), embedding je vždy NULL,
doplní ho samostatný skript (Fáze B), který zatím teprve postavíme.

POZOR: běží proti DRUHÉMU (samostatnému) Supabase projektu pro judikaturu.
Workflow předává secrets SUPABASE_URL_NS / SUPABASE_SERVICE_ROLE_KEY_NS
jako env proměnné SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY.
"""

import os
import re
import time
from html import unescape
from threading import Lock
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SERVICE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
MAX_CHARS_PER_CHUNK = int(os.environ.get("MAX_CHARS_PER_CHUNK", "4000"))
MAX_WORKERS = int(os.environ.get("MAX_WORKERS", "8"))
CUTOFF_YEAR = int(os.environ.get("CUTOFF_YEAR", "2019"))
MAX_ITEMS = int(os.environ.get("MAX_ITEMS", "80"))

BASE_URL = "https://sbirka.nsoud.cz"
SITEMAP_URLS = [
    f"{BASE_URL}/year-collection-sitemap.xml",
    f"{BASE_URL}/year-collection-sitemap2.xml",
]

SESSION = requests.Session()
_adapter = HTTPAdapter(pool_connections=MAX_WORKERS * 2, pool_maxsize=MAX_WORKERS * 2)
SESSION.mount("https://", _adapter)
SESSION.mount("http://", _adapter)
_log_lock = Lock()


def log(*a):
    with _log_lock:
        print(*a, flush=True)


def _get(url, params=None, max_retries=5):
    for attempt in range(max_retries):
        try:
            r = SESSION.get(url, params=params, timeout=60, headers={"User-Agent": "Mozilla/5.0"})
            if r.status_code == 429:
                wait = 5 * (attempt + 1)
                log(f"  429 rate limit, cekam {wait}s...")
                time.sleep(wait)
                continue
            if r.status_code == 404:
                return None
            r.raise_for_status()
            return r
        except requests.RequestException as e:
            if attempt == max_retries - 1:
                log(f"  chyba pri fetch {url}: {e}")
                return None
            time.sleep(3)
    return None


def supabase_upsert(table, rows, on_conflict):
    if not rows:
        return []
    r = SESSION.post(
        f"{SUPABASE_URL}/rest/v1/{table}?on_conflict={on_conflict}",
        headers={
            "apikey": SERVICE_KEY,
            "Authorization": f"Bearer {SERVICE_KEY}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates,return=representation",
        },
        json=rows,
        timeout=120,
    )
    if not r.ok:
        log("Supabase chyba:", r.status_code, r.text[:500])
        r.raise_for_status()
    return r.json()


def get_or_create_source():
    rows = supabase_upsert(
        "sources",
        [{
            "code": "sbirka_nsoud",
            "name": "Sbírka soudních rozhodnutí a stanovisek (Nejvyšší soud)",
            "base_url": BASE_URL,
        }],
        on_conflict="code",
    )
    return rows[0]["id"]


def get_existing_external_ids(source_id):
    """Načte všechna už uložená external_id — stránkovaně po 1000,
    protože PostgREST vrací default max 1000 řádků."""
    ids = set()
    offset = 0
    page_size = 1000
    while True:
        r = SESSION.get(
            f"{SUPABASE_URL}/rest/v1/documents",
            headers={
                "apikey": SERVICE_KEY,
                "Authorization": f"Bearer {SERVICE_KEY}",
                "Range-Unit": "items",
                "Range": f"{offset}-{offset + page_size - 1}",
            },
            params={"select": "external_id", "source_id": f"eq.{source_id}"},
            timeout=60,
        )
        r.raise_for_status()
        rows = r.json()
        ids.update(row["external_id"] for row in rows)
        if len(rows) < page_size:
            break
        offset += page_size
    return ids


def chunk_text(text, max_chars=MAX_CHARS_PER_CHUNK):
    """Rozdělí text na kusy o max max_chars znacích, pokud možno na hranicích
    odstavců (\n\n) nebo vět ('. '), ať se neřeže uprostřed slova."""
    text = text.strip()
    if len(text) <= max_chars:
        return [text] if text else []
    chunks = []
    start = 0
    n = len(text)
    while start < n:
        end = start + max_chars
        if end >= n:
            chunks.append(text[start:].strip())
            break
        window = text[start:end]
        cut = window.rfind("\n\n")
        if cut < max_chars // 4:
            cut = window.rfind(". ")
            if cut >= max_chars // 4:
                cut += 1
        if cut < max_chars // 4:
            cut = window.rfind(" ")
        if cut < max_chars // 4:
            cut = max_chars
        piece = text[start:start + cut].strip()
        if piece:
            chunks.append(piece)
        start += cut
        while start < n and text[start] in " \n":
            start += 1
    return chunks


_RE_DROP_BLOCKS = re.compile(
    r"(?is)<(script|style|nav|header|footer)\b[^>]*>.*?</\1\s*>"
)
_RE_COMMENTS = re.compile(r"(?s)<!--.*?-->")
_RE_BLOCK_END = re.compile(
    r"(?i)<(?:br\s*/?|/p|/div|/h[1-6]|/li|/tr|/table|/section|/article|/blockquote)>"
)
_RE_TAG = re.compile(r"<[^>]+>")


def html_to_text(html):
    """Odstraní script/style/nav/header/footer, tagy převede na whitespace
    a vrátí čistý viditelný text se zachovanými odstavci."""
    html = _RE_COMMENTS.sub(" ", html)
    html = _RE_DROP_BLOCKS.sub(" ", html)
    html = _RE_BLOCK_END.sub("\n", html)
    text = _RE_TAG.sub(" ", html)
    text = unescape(text)
    lines = [re.sub(r"[ \t\xa0]+", " ", ln).strip() for ln in text.splitlines()]
    out = []
    for ln in lines:
        if ln:
            out.append(ln)
        elif out and out[-1] != "":
            out.append("")
    return "\n".join(out).strip()


def parse_decision_page(html, decision_id):
    """Z HTML stránky rozhodnutí vytáhne metadata a plný text.
    Vrací dict, nebo None pokud je text prázdný/příliš krátký."""
    nazev = None
    m = re.search(r"(?is)<title[^>]*>(.*?)</title>", html)
    if m:
        nazev = unescape(re.sub(r"\s+", " ", m.group(1))).strip()
        nazev = re.sub(r"\s*[-–|]\s*Nejvyšší soud\s*$", "", nazev).strip()
    if not nazev:
        m = re.search(r"(?is)<h1[^>]*>(.*?)</h1>", html)
        if m:
            nazev = unescape(re.sub(r"\s+", " ", _RE_TAG.sub(" ", m.group(1)))).strip()
    if not nazev:
        nazev = f"Rozhodnutí Nejvyššího soudu (sbírka {decision_id})"

    text = html_to_text(html)

    plny_text = text
    needle = nazev[:40]
    if needle:
        idx = text.find(needle)
        if idx > 0:
            plny_text = text[idx:]
    plny_text = plny_text.strip()

    datum_rozhodnuti = None
    m = re.search(r"Datum rozhodnutí:\s*(\d{1,2})\.\s*(\d{1,2})\.\s*(\d{4})", text)
    if m:
        d, mo, y = m.groups()
        datum_rozhodnuti = f"{y}-{int(mo):02d}-{int(d):02d}"

    spisova_znacka = None
    m = re.search(r"Spisová značka:\s*([^\n]+)", text)
    if m:
        spisova_znacka = m.group(1).strip()

    soud = "Nejvyšší soud"
    m = re.search(r"Soud:\s*([^\n]+)", text)
    if m and m.group(1).strip():
        soud = m.group(1).strip()

    if len(plny_text) < 200:
        return None

    return {
        "nazev": nazev,
        "datum_rozhodnuti": datum_rozhodnuti,
        "spisova_znacka": spisova_znacka,
        "soud": soud,
        "plny_text": plny_text,
    }


def get_sesit_urls():
    """Stáhne obě sitemapy, vyfiltruje sešity s rokem >= CUTOFF_YEAR
    a vrátí je seřazené od nejnovějších (rok, číslo sešitu sestupně)."""
    sesity = []
    seen = set()
    for sm_url in SITEMAP_URLS:
        r = _get(sm_url)
        if r is None:
            log(f"  sitemapa {sm_url} nedostupna, preskakuji")
            continue
        for url in re.findall(r"<loc>\s*(.*?)\s*</loc>", r.text):
            url = unescape(url).strip()
            if "/year-collection/" not in url or url in seen:
                continue
            m = re.search(r"-(\d{4})/?$", url)
            if not m:
                continue
            rok = int(m.group(1))
            if rok < CUTOFF_YEAR:
                continue
            seen.add(url)
            mc = re.search(r"-c-(\d+)", url)
            cislo = int(mc.group(1)) if mc else 0
            sesity.append((rok, cislo, url))
    sesity.sort(key=lambda t: (t[0], t[1]), reverse=True)
    return [t[2] for t in sesity]


def get_decision_ids_from_sesit(sesit_url):
    """Z HTML sešit-stránky vytáhne ID jednotlivých rozhodnutí."""
    r = _get(sesit_url)
    if r is None:
        return set()
    return set(re.findall(r"https://sbirka\.nsoud\.cz/sbirka/(\d+)/", r.text))


def process_decision(decision_id, source_id):
    """Stáhne, naparsuje a uloží jedno rozhodnutí.
    Vrací ('ulozeno' | 'preskoceno' | 'chyba', decision_id)."""
    url = f"{BASE_URL}/sbirka/{decision_id}/"
    r = _get(url)
    if r is None:
        log(f"  [{decision_id}] stranka nedostupna")
        return ("chyba", decision_id)

    parsed = parse_decision_page(r.text, decision_id)
    if parsed is None:
        log(f"  [{decision_id}] prazdny nebo prilis kratky text (<200 znaku), preskakuji")
        return ("preskoceno", decision_id)

    doc_rows = supabase_upsert(
        "documents",
        [{
            "source_id": source_id,
            "external_id": str(decision_id),
            "doc_type": "judikat_ns",
            "title": parsed["nazev"],
            "issuer": parsed["soud"],
            "decision_date": parsed["datum_rozhodnuti"],
            "url": url,
            "status": "platny",
        }],
        on_conflict="source_id,external_id",
    )
    doc_id = doc_rows[0]["id"]

    chunks = chunk_text(parsed["plny_text"])
    chunk_rows = [
        {"document_id": doc_id, "chunk_index": i, "content": ch, "embedding": None}
        for i, ch in enumerate(chunks)
    ]
    supabase_upsert("chunks", chunk_rows, on_conflict="document_id,chunk_index")

    sp = parsed["spisova_znacka"] or "?"
    log(f"  [{decision_id}] ulozeno: {sp} ({len(parsed['plny_text'])} znaku, {len(chunks)} chunku)")
    return ("ulozeno", decision_id)


def main():
    log(f"=== sync_sbirka_ns start (CUTOFF_YEAR={CUTOFF_YEAR}, MAX_ITEMS={MAX_ITEMS}) ===")

    source_id = get_or_create_source()
    log(f"source_id = {source_id}")

    existing_ids = get_existing_external_ids(source_id)
    log(f"V databazi uz je {len(existing_ids)} rozhodnuti")

    sesit_urls = get_sesit_urls()
    log(f"Nalezeno {len(sesit_urls)} sesitu s rokem >= {CUTOFF_YEAR}")

    fronta = []
    fronta_set = set()
    for sesit_url in sesit_urls:
        if len(fronta) >= MAX_ITEMS:
            break
        ids = get_decision_ids_from_sesit(sesit_url)
        nove = [i for i in sorted(ids, key=int, reverse=True)
                if i not in existing_ids and i not in fronta_set]
        if nove:
            log(f"  {sesit_url} -> {len(ids)} rozhodnuti, {len(nove)} novych")
        fronta.extend(nove)
        fronta_set.update(nove)

    fronta = fronta[:MAX_ITEMS]
    log(f"Ke zpracovani: {len(fronta)} novych rozhodnuti")

    ulozeno = 0
    preskoceno = 0
    chyby = 0

    if fronta:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(process_decision, did, source_id): did for did in fronta}
            for fut in as_completed(futures):
                did = futures[fut]
                try:
                    status, _ = fut.result()
                    if status == "ulozeno":
                        ulozeno += 1
                    elif status == "preskoceno":
                        preskoceno += 1
                    else:
                        chyby += 1
                except Exception as e:
                    chyby += 1
                    log(f"  [{did}] neocekavana chyba: {e}")

    log(f"=== Hotovo: zpracovano {len(fronta)}, ulozeno {ulozeno}, "
        f"preskoceno {preskoceno}, chyb {chyby} ===")


if __name__ == "__main__":
    main()
