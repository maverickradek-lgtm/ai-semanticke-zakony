"""
Fetcher pro rozhodnuti Ustavniho soudu CR (NALUS) -
PRIMY zapis do samostatne Neon databaze (mirror vzoru sync_uohs_neon.py /
sync_mv_vestnik_neon.py).

Zdroj: https://nalus.usoud.cz/Search/Search.aspx - klasicka ASP.NET WebForms
aplikace (postback, __VIEWSTATE/__EVENTVALIDATION). Zjisteno pruzkumem
2026-09-06:
  1) GET Search.aspx -> vytahnout __VIEWSTATE/__VIEWSTATEGENERATOR/
     __EVENTVALIDATION z hidden inputu.
  2) POST Search.aspx (stejna session, cookies) s temito poli:
       nalezy=on, usneseni=on, stanoviska_plena=on   (vsechny 3 formy rozhodnuti)
       decidedFrom=D.M.YYYY, decidedTo=D.M.YYYY       (rozsah pro backfill),
       nebo pro prirustkovy sync:
       dle_data_zpristupneni=on, zpristupneno_pred=<pocet dni>
       razeni=3                                        (data rozhodnuti vzestupne)
       resultsPageSize=80
       ctl00$MainContent$but_search=Vyhledat
     Odpoved presmeruje/vrati Results.aspx s hlavickou
     "Vysledky 1 - 80 z celkem N".
  3) Vysledna sada je ulozena server-side v session (cookies) - dalsi stranky
     se stahuji prostym GET Results.aspx?page=N (N od 1), beze potreby
     znovu postovat VIEWSTATE.
  4) Kazdy radek vysledku odkazuje na
     ResultDetail.aspx?id=<interni_id>&pos=<poradi>&cnt=<celkem>&typ=result.
     Tato stranka NEVYZADUJE session/cookies z kroku 2 - lze ji natahnout
     primo (overeno: cerstva session, zadny predchozi POST, fungovalo).
     Obsahuje kompletni "Kartu zaznamu" (metadata) i plny "Text dokumentu"
     primo inline v HTML - neni potreba volat zvlast GetText.aspx.

Diky bodu 4) je cely pipeline mnohem jednodussi nez klasicky VIEWSTATE-replay:
POST/paging potrebuje pouze pro ZISKANI SEZNAMU platnych `id`, samotne
stazeni obsahu je pak nezavisly prosty GET na ResultDetail.aspx?id=X.

Rezimy:
  --mode backfill --year-from Y --year-to Y2   (vychozi: 1993 - aktualni rok)
      Pro kazdy rok zvlast provede POST search (aby cnt neprekrocil rozumne
      meze) a projede vsechny stranky/zaznamy.
  --mode incremental --days N (vychozi 3)
      Pouzije "Jen prirustky za N dni" (dle_data_zpristupneni +
      zpristupneno_pred) - urceno pro pozdejsi denni cron, KDYZ bude
      pipeline nasazena.

DULEZITE (Radek 2026-09-06): tento skript i prislusny workflow jsou POUZE
PRIPRAVENY, NENASAZENI - workflow ma jen workflow_dispatch (zadny cron),
neni napojen do ai-query/index.html/Databaze tabu. Priorita NAS runneru
zustava na dobehnuti embed_zakony_neon.py. Nespoustet dokud Radek nerekne
"nasad'".
"""
import hashlib
import os
import re
import sys
import time
from datetime import date

import psycopg2
import psycopg2.extras
import requests

NEON_DB_URL = os.environ["NEON_US_DB_URL"]
SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SERVICE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
ADMIN_USER_ID = "2648f5db-bea6-4cac-b490-ad0ec59723df"
GEMINI_API_KEY_OVERRIDE = os.environ.get("GEMINI_API_KEY_OVERRIDE")
GEMINI_API_KEY_POOL = [k.strip() for k in os.environ.get("GEMINI_API_KEY_POOL", "").split(",") if k.strip()]

BASE = "https://nalus.usoud.cz"
SEARCH_URL = BASE + "/Search/Search.aspx"
RESULTS_URL = BASE + "/Search/Results.aspx"
DETAIL_URL = BASE + "/Search/ResultDetail.aspx"

CHUNK_SIZE = 3000
EMBED_MODEL = "gemini-embedding-001"
EMBED_DIM = 256

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; ParagrAlfBot/1.0; +https://paragralf.cz)",
}

VIEWSTATE_RE = re.compile(r'id="__VIEWSTATE"[^>]*value="([^"]*)"')
VIEWSTATEGEN_RE = re.compile(r'id="__VIEWSTATEGENERATOR"[^>]*value="([^"]*)"')
EVENTVALIDATION_RE = re.compile(r'id="__EVENTVALIDATION"[^>]*value="([^"]*)"')
TOTAL_RE = re.compile(r'celkem\s+(\d+)', re.IGNORECASE)
DETAIL_ID_RE = re.compile(r'ResultDetail\.aspx\?id=(\d+)')


def log(msg):
    print(f"[sync_us_neon] {msg}", flush=True)


def db_connect():
    last_err = None
    for attempt in range(4):
        try:
            return psycopg2.connect(NEON_DB_URL, connect_timeout=15)
        except Exception as e:
            last_err = e
            log(f"db_connect selhalo (pokus {attempt + 1}/4): {e}")
            time.sleep(3)
    raise last_err


def get_admin_gemini_key():
    if GEMINI_API_KEY_OVERRIDE:
        return GEMINI_API_KEY_OVERRIDE
    last_err = None
    for attempt in range(4):
        try:
            resp = requests.post(
                f"{SUPABASE_URL}/rest/v1/rpc/get_admin_gemini_key",
                headers={
                    "apikey": SERVICE_KEY,
                    "Authorization": f"Bearer {SERVICE_KEY}",
                    "Content-Type": "application/json",
                },
                json={"p_user_id": ADMIN_USER_ID},
                timeout=15,
            )
            resp.raise_for_status()
            key = resp.json()
            if not key:
                raise RuntimeError("Admin Gemini key neni k dispozici")
            return key
        except Exception as e:
            last_err = e
            log(f"get_admin_gemini_key selhalo (pokus {attempt + 1}/4): {e}")
            time.sleep(3)
    raise last_err


def parse_cz_date(s):
    """'3. 1. 2024' -> date(2024, 1, 3). Vraci None pro prazdne/neplatne."""
    if not s:
        return None
    m = re.search(r'(\d{1,2})\.\s*(\d{1,2})\.\s*(\d{4})', s)
    if not m:
        return None
    d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
    try:
        return date(y, mo, d)
    except ValueError:
        return None


def chunk_text(text, size=CHUNK_SIZE):
    text = text.strip()
    if not text:
        return []
    return [text[i:i + size] for i in range(0, len(text), size)]


def embed_texts(texts, api_key):
    """Zavola Gemini embedding API pro seznam textu, vrati seznam vektoru."""
    out = []
    for t in texts:
        for attempt in range(4):
            try:
                resp = requests.post(
                    f"https://generativelanguage.googleapis.com/v1beta/models/{EMBED_MODEL}:embedContent?key={api_key}",
                    json={
                        "model": f"models/{EMBED_MODEL}",
                        "content": {"parts": [{"text": t}]},
                        "outputDimensionality": EMBED_DIM,
                    },
                    timeout=30,
                )
                resp.raise_for_status()
                out.append(resp.json()["embedding"]["values"])
                break
            except Exception as e:
                log(f"embed_texts selhalo (pokus {attempt + 1}/4): {e}")
                time.sleep(3)
        else:
            raise RuntimeError("embed_texts: vycerpany vsechny pokusy")
    return out


class NalusSession:
    """requests.Session wrapper pro NALUS search postback + paging."""

    def __init__(self):
        self.s = requests.Session()
        self.s.headers.update(HEADERS)

    def _get_hidden_fields(self, html):
        vs = VIEWSTATE_RE.search(html)
        vg = VIEWSTATEGEN_RE.search(html)
        ev = EVENTVALIDATION_RE.search(html)
        return {
            "__VIEWSTATE": vs.group(1) if vs else "",
            "__VIEWSTATEGENERATOR": vg.group(1) if vg else "",
            "__EVENTVALIDATION": ev.group(1) if ev else "",
        }

    def search_by_date_range(self, decided_from, decided_to):
        """decided_from/to ve tvaru 'D.M.YYYY'. Vraci (total_count, first_page_html)."""
        r = self.s.get(SEARCH_URL, timeout=30)
        r.raise_for_status()
        hidden = self._get_hidden_fields(r.text)
        payload = {
            **hidden,
            "__EVENTTARGET": "",
            "__EVENTARGUMENT": "",
            "ctl00$MainContent$nalezy": "on",
            "ctl00$MainContent$usneseni": "on",
            "ctl00$MainContent$stanoviska_plena": "on",
            "ctl00$MainContent$decidedFrom": decided_from,
            "ctl00$MainContent$decidedTo": decided_to,
            "ctl00$MainContent$razeni": "3",
            "ctl00$MainContent$resultsPageSize": "80",
            "ctl00$MainContent$but_search": "Vyhledat",
        }
        r2 = self.s.post(SEARCH_URL, data=payload, timeout=30)
        r2.raise_for_status()
        m = TOTAL_RE.search(r2.text)
        total = int(m.group(1)) if m else 0
        return total, r2.text

    def search_recent(self, days):
        """Prirustkovy rezim - 'Jen prirustky za N dni' (dle data zpristupneni)."""
        r = self.s.get(SEARCH_URL, timeout=30)
        r.raise_for_status()
        hidden = self._get_hidden_fields(r.text)
        payload = {
            **hidden,
            "__EVENTTARGET": "",
            "__EVENTARGUMENT": "",
            "ctl00$MainContent$nalezy": "on",
            "ctl00$MainContent$usneseni": "on",
            "ctl00$MainContent$stanoviska_plena": "on",
            "ctl00$MainContent$dle_data_zpristupneni": "on",
            "ctl00$MainContent$zpristupneno_pred": str(days),
            "ctl00$MainContent$razeni": "3",
            "ctl00$MainContent$resultsPageSize": "80",
            "ctl00$MainContent$but_search": "Vyhledat",
        }
        r2 = self.s.post(SEARCH_URL, data=payload, timeout=30)
        r2.raise_for_status()
        m = TOTAL_RE.search(r2.text)
        total = int(m.group(1)) if m else 0
        return total, r2.text

    def get_page(self, page_num):
        r = self.s.get(RESULTS_URL, params={"page": page_num}, timeout=30)
        r.raise_for_status()
        return r.text

    def iter_ids(self, total, first_page_html, page_size=80):
        seen = set()
        for m in DETAIL_ID_RE.finditer(first_page_html):
            i = int(m.group(1))
            if i not in seen:
                seen.add(i)
                yield i
        pages = (total + page_size - 1) // page_size
        for page in range(2, pages + 1):
            html = self.get_page(page)
            for m in DETAIL_ID_RE.finditer(html):
                i = int(m.group(1))
                if i not in seen:
                    seen.add(i)
                    yield i
            time.sleep(0.5)


FIELD_PATTERNS = {
    "ecli": r'Identifikator evropske judikatury\s*([^\n]*)',
    "sp_zn": r'Spisova znacka\s*([^\n]*)',
    "datum_rozhodnuti": r'Datum rozhodnuti\s*([^\n]*)',
    "datum_vyhlaseni": r'Datum vyhlaseni\s*([^\n]*)',
    "datum_podani": r'Datum podani\s*([^\n]*)',
    "datum_zpristupneni": r'Datum zpristupneni\s*([^\n]*)',
    "forma_rozhodnuti": r'Forma rozhodnuti\s*([^\n]*)',
    "typ_rizeni": r'Typ rizeni\s*([^\n]*)',
    "vyznam": r'Vyznam\s*([^\n]*)',
    "navrhovatel": r'Navrhovatel\s*([^\n]*)',
    "soudce_zpravodaj": r'Soudce zpravodaj\s*([^\n]*)',
    "predmet_rizeni": r'Predmet rizeni\s*([^\n]*)',
    "vecny_rejstrik": r'Vecny rejstrik\s*([^\n]*)',
}


def fetch_detail(session, doc_id):
    """Stahne ResultDetail.aspx?id=X, vrati dict s metadaty + plnym textem, nebo None."""
    r = session.s.get(DETAIL_URL, params={"id": doc_id}, timeout=30)
    r.raise_for_status()
    html = r.text
    text_marker = "Text dokumentu"
    idx = html.find(text_marker)
    if idx == -1:
        return None
    tail = html[idx + len(text_marker):]
    end_idx = tail.find("Ustavni soud, Jostova 8")
    body = tail[:end_idx] if end_idx != -1 else tail
    body_text = re.sub(r'<[^>]+>', ' ', body)
    body_text = re.sub(r'\s+\n', '\n', body_text)
    body_text = re.sub(r'[ \t]+', ' ', body_text).strip()
    if not body_text or "Vytah z dokumentu neni k dispozici" in body_text:
        return None

    head = html[:idx]
    head_text = re.sub(r'<[^>]+>', '\n', head)
    fields = {}
    for key, pattern in FIELD_PATTERNS.items():
        m = re.search(pattern, head_text)
        fields[key] = m.group(1).strip() if m and m.group(1).strip() else None

    sp_zn = fields.get("sp_zn")
    return {
        "external_id": f"us:{doc_id}",
        "sp_zn": sp_zn,
        "ecli": fields.get("ecli"),
        "title": sp_zn,
        "forma_rozhodnuti": fields.get("forma_rozhodnuti"),
        "typ_rizeni": fields.get("typ_rizeni"),
        "vyznam": fields.get("vyznam"),
        "navrhovatel": fields.get("navrhovatel"),
        "soudce_zpravodaj": fields.get("soudce_zpravodaj"),
        "datum_rozhodnuti": parse_cz_date(fields.get("datum_rozhodnuti")),
        "datum_vyhlaseni": parse_cz_date(fields.get("datum_vyhlaseni")),
        "datum_podani": parse_cz_date(fields.get("datum_podani")),
        "datum_zpristupneni": parse_cz_date(fields.get("datum_zpristupneni")),
        "predmet_rizeni": fields.get("predmet_rizeni"),
        "vecny_rejstrik": fields.get("vecny_rejstrik"),
        "url": f"{DETAIL_URL}?id={doc_id}",
        "content": body_text,
    }


def upsert_document(conn, doc):
    content_hash = hashlib.sha256(doc["content"].encode("utf-8")).hexdigest()
    with conn.cursor() as cur:
        cur.execute("SELECT id, content_hash FROM documents WHERE external_id = %s", (doc["external_id"],))
        row = cur.fetchone()
        if row and row[1] == content_hash:
            return None, False
        if row:
            doc_id = row[0]
            cur.execute(
                """UPDATE documents SET sp_zn=%s, ecli=%s, title=%s, forma_rozhodnuti=%s,
                   typ_rizeni=%s, vyznam=%s, navrhovatel=%s, soudce_zpravodaj=%s,
                   datum_rozhodnuti=%s, datum_vyhlaseni=%s, datum_podani=%s,
                   datum_zpristupneni=%s, predmet_rizeni=%s, vecny_rejstrik=%s,
                   url=%s, content=%s, content_hash=%s, is_embedded=FALSE,
                   updated_at=now() WHERE id=%s""",
                (doc["sp_zn"], doc["ecli"], doc["title"], doc["forma_rozhodnuti"],
                 doc["typ_rizeni"], doc["vyznam"], doc["navrhovatel"], doc["soudce_zpravodaj"],
                 doc["datum_rozhodnuti"], doc["datum_vyhlaseni"], doc["datum_podani"],
                 doc["datum_zpristupneni"], doc["predmet_rizeni"], doc["vecny_rejstrik"],
                 doc["url"], doc["content"], content_hash, doc_id),
            )
            cur.execute("DELETE FROM chunks WHERE document_id = %s", (doc_id,))
        else:
            cur.execute(
                """INSERT INTO documents (doc_type, external_id, sp_zn, ecli, title,
                   forma_rozhodnuti, typ_rizeni, vyznam, navrhovatel, soudce_zpravodaj,
                   datum_rozhodnuti, datum_vyhlaseni, datum_podani, datum_zpristupneni,
                   predmet_rizeni, vecny_rejstrik, url, content, content_hash)
                   VALUES ('judikat_us', %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                   RETURNING id""",
                (doc["external_id"], doc["sp_zn"], doc["ecli"], doc["title"],
                 doc["forma_rozhodnuti"], doc["typ_rizeni"], doc["vyznam"], doc["navrhovatel"],
                 doc["soudce_zpravodaj"], doc["datum_rozhodnuti"], doc["datum_vyhlaseni"],
                 doc["datum_podani"], doc["datum_zpristupneni"], doc["predmet_rizeni"],
                 doc["vecny_rejstrik"], doc["url"], doc["content"], content_hash),
            )
            doc_id = cur.fetchone()[0]
    conn.commit()
    return doc_id, True


def embed_pending(conn, api_key, limit=200):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, content FROM documents WHERE is_embedded = FALSE AND content IS NOT NULL LIMIT %s",
            (limit,),
        )
        rows = cur.fetchall()
    for doc_id, content in rows:
        chunks = chunk_text(content)
        if not chunks:
            with conn.cursor() as cur:
                cur.execute("UPDATE documents SET is_embedded = TRUE WHERE id = %s", (doc_id,))
            conn.commit()
            continue
        vectors = embed_texts(chunks, api_key)
        with conn.cursor() as cur:
            for idx, (c, v) in enumerate(zip(chunks, vectors)):
                cur.execute(
                    """INSERT INTO chunks (document_id, chunk_index, content, embedding)
                       VALUES (%s,%s,%s,%s)
                       ON CONFLICT (document_id, chunk_index) DO UPDATE SET
                       content=EXCLUDED.content, embedding=EXCLUDED.embedding""",
                    (doc_id, idx, c, v),
                )
            cur.execute("UPDATE documents SET is_embedded = TRUE WHERE id = %s", (doc_id,))
        conn.commit()
        log(f"embedded doc_id={doc_id} ({len(chunks)} chunku)")


def run_backfill(year_from, year_to):
    conn = db_connect()
    api_key = get_admin_gemini_key()
    total_new = 0
    for year in range(year_from, year_to + 1):
        session = NalusSession()
        decided_from = f"1.1.{year}"
        decided_to = f"31.12.{year}"
        log(f"rok {year}: POST search {decided_from} - {decided_to}")
        try:
            total, first_html = session.search_by_date_range(decided_from, decided_to)
        except Exception as e:
            log(f"rok {year}: search selhal: {e}")
            continue
        log(f"rok {year}: celkem {total} zaznamu")
        if total == 0:
            continue
        count_year = 0
        for doc_id in session.iter_ids(total, first_html):
            try:
                doc = fetch_detail(session, doc_id)
            except Exception as e:
                log(f"id={doc_id}: fetch_detail selhal: {e}")
                continue
            if not doc:
                continue
            try:
                _, changed = upsert_document(conn, doc)
            except Exception as e:
                log(f"id={doc_id}: upsert selhal: {e}")
                conn.rollback()
                continue
            if changed:
                count_year += 1
                total_new += 1
            time.sleep(0.3)
        log(f"rok {year}: {count_year} novych/zmenenych dokumentu")
        embed_pending(conn, api_key)
    log(f"backfill hotovo, celkem {total_new} novych/zmenenych dokumentu")
    conn.close()


def run_incremental(days):
    conn = db_connect()
    api_key = get_admin_gemini_key()
    session = NalusSession()
    total, first_html = session.search_recent(days)
    log(f"prirustky za {days} dni: {total} zaznamu")
    count = 0
    if total > 0:
        for doc_id in session.iter_ids(total, first_html):
            try:
                doc = fetch_detail(session, doc_id)
            except Exception as e:
                log(f"id={doc_id}: fetch_detail selhal: {e}")
                continue
            if not doc:
                continue
            try:
                _, changed = upsert_document(conn, doc)
            except Exception as e:
                log(f"id={doc_id}: upsert selhal: {e}")
                conn.rollback()
                continue
            if changed:
                count += 1
            time.sleep(0.3)
    embed_pending(conn, api_key)
    log(f"incremental hotovo, {count} novych/zmenenych dokumentu")
    conn.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["backfill", "incremental"], default="incremental")
    parser.add_argument("--year-from", type=int, default=1993)
    parser.add_argument("--year-to", type=int, default=date.today().year)
    parser.add_argument("--days", type=int, default=3)
    args = parser.parse_args()

    if args.mode == "backfill":
        run_backfill(args.year_from, args.year_to)
    else:
        run_incremental(args.days)
