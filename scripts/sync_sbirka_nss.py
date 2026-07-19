#!/usr/bin/env python3
"""
sync_sbirka_nss.py — Import judikátů ze Sbírky rozhodnutí Nejvyššího
správního soudu ČR (sbirka.nssoud.cz) do Supabase.

Fáze A — ukládá jen text (documents + chunks), embedding je vždy NULL,
doplní ho samostatný skript (Fáze B).

Struktura webu (ověřeno 2026-07): sbirka.nssoud.cz NENÍ stejná platforma
jako sbirka.nsoud.cz (běží na Cream Webshape CMS) a nemá žádnou sitemapu.
Enumerace jde přes ročníky a vydání:
  /cz/{rok}             -> odkazy na vydání /cz/{rok}-{cislo}
  /cz/{rok}-{cislo}     -> seznam rozhodnutí vč. metadat v podobě
                           "Datum: DD.MM.RRRR · Sbírkové č.: N/RRRR ·
                            Sp. zn.: ... · Typ: ... · Autor: <soud> ..."
  /cz/{slug}.p{id}.html -> plný text rozhodnutí (server-rendered HTML)
external_id = numerické {id} z URL rozhodnutí (.p{id}.html).

Metadata bereme primárně ze stránky vydání (na stránce rozhodnutí žádné
štítky "Datum rozhodnutí:" / "Spisová značka:" nejsou); stránka rozhodnutí
slouží pro plný text, s fallbackem na citační řádek
"(Podle rozsudku ... ze dne D. M. RRRR, čj. ...)".

Sbírka vychází od roku 2003 (č. 1/2003), obsahuje i vybraná rozhodnutí
krajských soudů (pole Autor).

POZOR: běží proti DRUHÉMU (samostatnému) Supabase projektu pro judikaturu,
sdílenému s NS. Workflow předává secrets SUPABASE_URL_NS /
SUPABASE_SERVICE_ROLE_KEY_NS jako env SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY.
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

BASE_URL = "https://sbirka.nssoud.cz"
FIRST_YEAR = 2003  # Sbírka NSS vychází od roku 2003

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
            "code": "sbirka_nssoud",
            "name": "Sbírka rozhodnutí Nejvyššího správního soudu",
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


def _clean_inline(html_fragment):
    """Text z vnitřku HTML elementu (bez tagů, jednořádkově)."""
    txt = _RE_TAG.sub(" ", html_fragment)
    txt = unescape(txt)
    return re.sub(r"\s+", " ", txt).strip()


# Odkaz na rozhodnutí: /cz/{slug}.p{id}.html (href může být relativní i absolutní)
_RE_DECISION_A = re.compile(r'(?is)<a[^>]+href="([^"]*?\.p(\d+)\.html)"[^>]*>(.*?)</a>')


def get_vydani_urls():
    """Projde ročníkové stránky /cz/{rok} a vrátí URL všech vydání
    /cz/{rok}-{cislo}, seřazené od nejnovějšího. Sitemapa neexistuje."""
    vydani = []
    current_year = time.gmtime().tm_year
    for rok in range(max(CUTOFF_YEAR, FIRST_YEAR), current_year + 1):
        r = _get(f"{BASE_URL}/cz/{rok}")
        if r is None:
            log(f"  rocnik {rok} nedostupny, preskakuji")
            continue
        cisla = set()
        for m in re.finditer(rf'href="[^"]*/cz/{rok}-(\d+)/?"', r.text):
            cisla.add(int(m.group(1)))
        for cislo in cisla:
            vydani.append((rok, cislo, f"{BASE_URL}/cz/{rok}-{cislo}"))
    vydani.sort(key=lambda t: (t[0], t[1]), reverse=True)
    return [t[2] for t in vydani]


def get_decision_entries_from_vydani(vydani_url):
    """Ze stránky vydání vytáhne rozhodnutí vč. metadat. Formát na stránce
    (ověřeno pro 2003 i 2025):
      <a href=".../{slug}.p{id}.html">4710/2025 Nazev...</a>
      Datum: 02.09.2025 · Sbírkové č.: 4710/2025 · Sp. zn.: 1 As 48/2025 - 112
      · Typ: Rozsudek (SJS) · Autor: Nejvyšší správní soud - senát (ostatní) ...
    """
    r = _get(vydani_url)
    if r is None:
        return []
    html = r.text
    matches = list(_RE_DECISION_A.finditer(html))
    entries = {}
    order = []
    for i, m in enumerate(matches):
        href, pid, inner = m.group(1), m.group(2), m.group(3)
        title = _clean_inline(inner)
        seg_end = matches[i + 1].start() if i + 1 < len(matches) else min(len(html), m.end() + 6000)
        meta_text = html_to_text(html[m.end():seg_end])

        datum = None
        mm = re.search(r"Datum:\s*(\d{1,2})\.\s*(\d{1,2})\.\s*(\d{4})", meta_text)
        if mm:
            d, mo, y = mm.groups()
            datum = f"{y}-{int(mo):02d}-{int(d):02d}"
        mm = re.search(r"Sbírkové č\.:\s*(\d+/\d{4})", meta_text)
        cislo = mm.group(1) if mm else None
        mm = re.search(r"Sp\.\s*zn\.:\s*([^·\n]+)", meta_text)
        sp_zn = mm.group(1).strip() if mm else None
        mm = re.search(r"Autor:\s*([^·\n]+)", meta_text)
        autor = mm.group(1).strip() if mm else None

        if href.startswith("http"):
            full_url = href
        elif href.startswith("/"):
            full_url = BASE_URL + href
        else:
            full_url = f"{BASE_URL}/{href}"

        e = entries.get(pid)
        if e is None:
            entries[pid] = {
                "id": pid, "url": full_url, "title": title or None,
                "datum": datum, "cislo": cislo, "sp_zn": sp_zn, "autor": autor,
            }
            order.append(pid)
        else:
            if not e["title"] and title:
                e["title"] = title
            for k, v in (("datum", datum), ("cislo", cislo),
                         ("sp_zn", sp_zn), ("autor", autor)):
                if not e[k] and v:
                    e[k] = v
    return [entries[pid] for pid in order]


def parse_decision_page(html, entry):
    """Z detailu rozhodnutí vytáhne plný text. Metadata bere primárně
    z entry (stránka vydání); fallback je citační řádek
    "(Podle rozsudku ... ze dne D. M. RRRR, čj. ...)" a <meta keywords>."""
    nazev = entry.get("title")
    if not nazev:
        m = re.search(r"(?is)<title[^>]*>(.*?)</title>", html)
        if m:
            nazev = unescape(re.sub(r"\s+", " ", m.group(1))).strip()
            nazev = re.sub(r"\s*\|\s*Sbírka rozhodnutí Nejvyššího správního soudu\s*$",
                           "", nazev).strip()
    if not nazev:
        nazev = f"Rozhodnutí Nejvyššího správního soudu (sbírka {entry['id']})"

    text = html_to_text(html)

    # Konec textu: za rozhodnutím následuje v DOM vyhledávací formulář
    # ("Vyhledávání" / "Hledaný výraz") a patička — odřízneme.
    for marker in ("\nHledaný výraz", "\nZasílání aktuálního vydání"):
        idx = text.find(marker)
        if idx > 0:
            text = text[:idx]
            break
    text = re.sub(r"\n+Vyhledávání\s*$", "", text).strip()

    # Začátek textu: samostatný řádek se sbírkovým číslem (např. "4747/2026")
    # předchází nadpisu rozhodnutí; fallback = první výskyt názvu bez čísla.
    start_idx = None
    cislo = entry.get("cislo")
    if cislo:
        m = re.search(rf"(?m)^\s*{re.escape(cislo)}\s*$", text)
        if m:
            start_idx = m.start()
    if start_idx is None:
        needle = re.sub(r"^\d+/\d{4}\s+", "", nazev)[:40]
        if needle:
            idx = text.find(needle)
            if idx > 0:
                start_idx = idx
    plny_text = text[start_idx:].strip() if start_idx else text.strip()

    datum_rozhodnuti = entry.get("datum")
    if not datum_rozhodnuti:
        m = re.search(r"\(Podle[^\n]*?ze dne\s+(\d{1,2})\.\s*(\d{1,2})\.\s*(\d{4})", text)
        if m:
            d, mo, y = m.groups()
            datum_rozhodnuti = f"{y}-{int(mo):02d}-{int(d):02d}"

    spisova_znacka = entry.get("sp_zn")
    if not spisova_znacka:
        m = re.search(r"\(Podle[^\n]*?(?:čj|č\.\s*j|sp\.\s*zn)\.?\s*([^\n)]+)\)", text)
        if m:
            spisova_znacka = m.group(1).strip().rstrip(",")

    soud = entry.get("autor")
    if not soud:
        m = re.search(r'(?is)<meta[^>]*name="keywords"[^>]*content="([^"]*)"', html)
        if m and m.group(1).strip():
            soud = unescape(m.group(1)).split(",")[0].strip() or None
    if not soud:
        soud = "Nejvyšší správní soud"

    if len(plny_text) < 200:
        return None

    return {
        "nazev": nazev,
        "datum_rozhodnuti": datum_rozhodnuti,
        "spisova_znacka": spisova_znacka,
        "soud": soud,
        "plny_text": plny_text,
    }


def process_decision(entry, source_id):
    decision_id = entry["id"]
    url = entry["url"]
    r = _get(url)
    if r is None:
        log(f"  [{decision_id}] stranka nedostupna")
        return ("chyba", decision_id)

    parsed = parse_decision_page(r.text, entry)
    if parsed is None:
        log(f"  [{decision_id}] prazdny nebo prilis kratky text (<200 znaku), preskakuji")
        return ("preskoceno", decision_id)

    doc_rows = supabase_upsert(
        "documents",
        [{
            "source_id": source_id,
            "external_id": str(decision_id),
            "doc_type": "judikat_nss",
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
    log(f"=== sync_sbirka_nss start (CUTOFF_YEAR={CUTOFF_YEAR}, MAX_ITEMS={MAX_ITEMS}) ===")
    source_id = get_or_create_source()
    log(f"source_id = {source_id}")
    existing_ids = get_existing_external_ids(source_id)
    log(f"V databazi uz je {len(existing_ids)} rozhodnuti")
    vydani_urls = get_vydani_urls()
    log(f"Nalezeno {len(vydani_urls)} vydani s rokem >= {CUTOFF_YEAR}")
    fronta = []
    fronta_set = set()
    for vydani_url in vydani_urls:
        if len(fronta) >= MAX_ITEMS:
            break
        entries = get_decision_entries_from_vydani(vydani_url)
        nove = [e for e in entries
                if e["id"] not in existing_ids and e["id"] not in fronta_set]
        if nove:
            log(f"  {vydani_url} -> {len(entries)} rozhodnuti, {len(nove)} novych")
            fronta.extend(nove)
            fronta_set.update(e["id"] for e in nove)
    fronta = fronta[:MAX_ITEMS]
    log(f"Ke zpracovani: {len(fronta)} novych rozhodnuti")
    ulozeno = 0
    preskoceno = 0
    chyby = 0
    if fronta:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(process_decision, e, source_id): e["id"] for e in fronta}
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
