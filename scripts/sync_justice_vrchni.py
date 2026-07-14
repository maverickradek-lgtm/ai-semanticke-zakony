"""
Import rozhodnuti Vrchnich soudu (Praha + Olomouc) z otevrenych dat
rozhodnuti.justice.cz do Supabase (documents + chunks).

Zdroj: https://rozhodnuti.justice.cz/api/opendata/{rok}/{mesic}/{den}
Rozhodnuti jsou jiz anonymizovana primo justici a obsahuji cisty text
(verdictText, justificationText) - zadne PDF parsovani neni potreba.

DULEZITE: tento skript NEPOCITA embeddingy. Radky v "chunks" se ukladaji
s embedding = NULL a doplni je existujici sync_esbirka_embed.py, ktery
kazdy den zpracovava VSECHNY chunky s embedding IS NULL bez ohledu na to,
z jakeho zdroje (zakony, judikatura, ...) pochazeji.

Beh: kazdy den se prochazi jen poslednich BACKFILL_DAYS dni (kvuli
prekryvu a pripadnym pozdejsim opravam/publikacim), diky upsertu na
(source_id, external_id) je import idempotentni. Pro prvni velky historicky
naplneni lze BACKFILL_DAYS docasne zvysit (viz workflow_dispatch input).

Jednotlive dokumenty (fetch textu + zapis do DB) se zpracovavaji soubezne
pres MAX_WORKERS vlaken, protoze jde o cistou I/O cekaci praci (HTTP) -
diky tomu je import radove rychlejsi nez sekvencni zpracovani.
"""

import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from threading import Lock

import requests
from requests.adapters import HTTPAdapter

BASE = "https://rozhodnuti.justice.cz/api"
SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SERVICE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
BACKFILL_DAYS = int(os.environ.get("BACKFILL_DAYS", "5"))
MAX_CHARS_PER_CHUNK = int(os.environ.get("MAX_CHARS_PER_CHUNK", "4000"))
MAX_WORKERS = int(os.environ.get("MAX_WORKERS", "8"))

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
            r = SESSION.get(url, params=params, timeout=60)
            if r.status_code == 429:
                wait = 5 * (attempt + 1)
                log(f"  429 rate limit, cekam {wait}s...")
                time.sleep(wait)
                continue
            if r.status_code == 404:
                return None
            r.raise_for_status()
            return r.json()
        except requests.exceptions.RequestException as e:
            if attempt == max_retries - 1:
                log(f"  chyba site, vzdavam se: {e}")
                return None
            log(f"  chyba site ({e}), zkousim znovu...")
            time.sleep(3)
    return None


def fetch_day(d):
    """Vrati vsechny zaznamy pro dany den (projde vsechny stranky)."""
    items = []
    page = 0
    while True:
        params = {"page": page} if page else None
        data = _get(f"{BASE}/opendata/{d.year}/{d.month}/{d.day}", params=params)
        if not data:
            break
        items.extend(data.get("items", []))
        total_pages = data.get("totalPages", 1)
        page += 1
        if page >= total_pages:
            break
    return items


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
            "code": "justice_vrchni",
            "name": "rozhodnuti.justice.cz - Vrchni soudy",
            "base_url": BASE,
        }],
        on_conflict="code",
    )
    return rows[0]["id"]


def split_into_chunks(text, max_chars):
    """Rozdeli dlouhy text na kusy podle odstavcu tak, aby zadny kus
    nebyl prilis velky pro embedding."""
    if not text:
        return []
    paragraphs = [p.strip() for p in text.split("\n") if p.strip()]
    chunks = []
    current = ""
    for p in paragraphs:
        if current and len(current) + len(p) + 1 > max_chars:
            chunks.append(current)
            current = p
        else:
            current = (current + "\n" + p) if current else p
    if current:
        chunks.append(current)
    return chunks or ([text[:max_chars]] if text else [])


def process_item(item, source_id):
    """Zpracuje jedno rozhodnuti: stahne text a zapise do Supabase.
    Vraci 'imported' / 'skipped' / 'error' - nikdy nevyhazuje vyjimku ven,
    aby jedna spatna polozka nezastavila cely dychove-basen zpracovani."""
    try:
        odkaz = item.get("odkaz")
        if not odkaz:
            return "skipped"
        external_id = odkaz.rstrip("/").rsplit("/", 1)[-1]

        doc = _get(odkaz)
        if not doc:
            return "skipped"

        verdict = (doc.get("verdictText") or "").strip()
        justification = (doc.get("justificationText") or "").strip()
        full_text = (verdict + "\n\n" + justification).strip()
        if not full_text:
            return "skipped"

        title = (str(item.get("soud", "")) + " - " + str(item.get("jednaciCislo", ""))).strip(" -")

        doc_rows = supabase_upsert(
            "documents",
            [{
                "source_id": source_id,
                "external_id": external_id,
                "doc_type": "judikat",
                "title": title,
                "issuer": item.get("soud"),
                "decision_date": item.get("datumVydani"),
                "url": odkaz,
                "status": "platny",
            }],
            on_conflict="source_id,external_id",
        )
        document_id = doc_rows[0]["id"]

        chunk_texts = split_into_chunks(full_text, MAX_CHARS_PER_CHUNK)
        chunk_rows = [
            {
                "document_id": document_id,
                "chunk_index": i,
                "heading": None,
                "content": c,
                "embedding": None,
            }
            for i, c in enumerate(chunk_texts)
        ]
        supabase_upsert("chunks", chunk_rows, on_conflict="document_id,chunk_index")
        return "imported"
    except Exception as e:
        log(f"  chyba pri zpracovani polozky: {e}")
        return "error"


def main():
    log("=== Sync judikatura (Vrchni soudy): start ===")
    log(f"BACKFILL_DAYS={BACKFILL_DAYS}, MAX_WORKERS={MAX_WORKERS}")
    source_id = get_or_create_source()

    today = date.today()
    dates = [today - timedelta(days=i) for i in range(BACKFILL_DAYS)]

    matched = []
    for d in dates:
        items = fetch_day(d)
        day_vrchni = [i for i in items if "Vrchn\u00ed soud" in (i.get("soud") or "")]
        matched.extend(day_vrchni)
        log(f"{d.isoformat()}: {len(items)} zaznamu celkem, {len(day_vrchni)} od vrchnich soudu")

    log(f"Celkem k importu: {len(matched)} rozhodnuti vrchnich soudu")

    imported = 0
    skipped = 0
    errors = 0
    done_count = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_item, item, source_id) for item in matched]
        for fut in as_completed(futures):
            result = fut.result()
            done_count += 1
            if result == "imported":
                imported += 1
            elif result == "error":
                errors += 1
            else:
                skipped += 1
            if done_count % 50 == 0:
                log(f"  ...zpracovano {done_count}/{len(matched)} (importovano {imported})")

    log(f"=== Hotovo: importovano {imported}, preskoceno {skipped}, chyb {errors} ===")


if __name__ == "__main__":
    main()
