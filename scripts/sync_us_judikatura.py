"""
Fetcher nalezu Ustavniho soudu CR (NALUS) -> databaze judikatury (Supabase
judikatura, tabulky sources/documents/chunks), doc_type 'judikat_us'.

Embedding se NEDELA tady - chunks se ukladaji s embedding NULL a dopocita je
sync_sbirka_ns_embed.py (pridanim ID zdroje do EMBED_SOURCE_IDS).

Rozsah (Radek 2026-11-23): pouze NALEZY (ne usneseni, ne stanoviska plena),
od roku 2010. Zdroj: https://nalus.usoud.cz (ASP.NET WebForms, postback).

Rezimy:
  --mode backfill --year-from 2010 --year-to 2026   (po rocich)
  --mode incremental --days 3                       (prirustky dle data zpristupneni)
"""
import argparse
import hashlib
import os
import re
import sys
import time
from datetime import date

import psycopg2
import requests

DB_URL = os.environ["SUPABASE_JUDIKATURA_DB_URL"]
BASE = "https://nalus.usoud.cz"
SEARCH_URL = BASE + "/Search/Search.aspx"
RESULTS_URL = BASE + "/Search/Results.aspx"
DETAIL_URL = BASE + "/Search/ResultDetail.aspx"
SOURCE_CODE = "nalus_us"
SOURCE_NAME = "Nálezy Ústavního soudu (NALUS)"
DOC_TYPE = "judikat_us"
CHUNK_SIZE = 3000
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; ParagrAlfBot/1.0; +https://paragralf.cz)"}

VIEWSTATE_RE = re.compile(r'id="__VIEWSTATE"[^>]*value="([^"]*)"')
VIEWSTATEGEN_RE = re.compile(r'id="__VIEWSTATEGENERATOR"[^>]*value="([^"]*)"')
EVENTVALIDATION_RE = re.compile(r'id="__EVENTVALIDATION"[^>]*value="([^"]*)"')
TOTAL_RE = re.compile(r"celkem\s+(\d+)", re.IGNORECASE)
DETAIL_ID_RE = re.compile(r"ResultDetail\.aspx\?id=(\d+)")

LABELS = [
    "Identifikátor evropské judikatury", "Název soudu", "Spisová značka",
    "Paralelní citace (Sbírka zákonů)", "Paralelní citace (Sbírka nálezů a usnesení)",
    "Populární název", "Datum rozhodnutí", "Datum vyhlášení", "Datum podání",
    "Datum zpřístupnění", "Forma rozhodnutí", "Typ řízení", "Význam", "Navrhovatel",
    "Dotčený orgán", "Soudce zpravodaj", "Napadený akt", "Typ výroku",
    "Dotčené ústavní zákony a mezinárodní smlouvy", "Dotčené zákony", "Odlišné stanovisko",
    "Předmět řízení", "Věcný rejstřík", "Poznámka", "Text dokumentu",
]


def log(msg):
    print(f"[sync_us_judikatura] {msg}", flush=True)


def parse_cz_date(s):
    m = re.search(r"(\d{1,2})\.\s*(\d{1,2})\.\s*(\d{4})", s or "")
    if not m:
        return None
    try:
        return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
    except ValueError:
        return None


def db_connect():
    last = None
    for i in range(4):
        try:
            return psycopg2.connect(DB_URL, connect_timeout=15, keepalives=1,
                                    keepalives_idle=30, keepalives_interval=10, keepalives_count=3)
        except Exception as e:
            last = e
            log(f"db_connect selhalo ({i + 1}/4): {e}")
            time.sleep(3)
    raise last


def get_source_id(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM sources WHERE code = %s", (SOURCE_CODE,))
        row = cur.fetchone()
        if row:
            return row[0]
        cur.execute("INSERT INTO sources (code, name, base_url) VALUES (%s,%s,%s) RETURNING id",
                    (SOURCE_CODE, SOURCE_NAME, BASE))
        sid = cur.fetchone()[0]
    conn.commit()
    return sid


class Nalus:
    def __init__(self):
        self.s = requests.Session()
        self.s.headers.update(HEADERS)

    def _hidden(self, html):
        g = lambda rx: (rx.search(html).group(1) if rx.search(html) else "")
        return {"__VIEWSTATE": g(VIEWSTATE_RE), "__VIEWSTATEGENERATOR": g(VIEWSTATEGEN_RE),
                "__EVENTVALIDATION": g(EVENTVALIDATION_RE)}

    def _post(self, extra):
        r = self.s.get(SEARCH_URL, timeout=30)
        r.raise_for_status()
        payload = {**self._hidden(r.text), "__EVENTTARGET": "", "__EVENTARGUMENT": "",
                   "ctl00$MainContent$nalezy": "on",  # POUZE nalezy
                   "ctl00$MainContent$razeni": "3",
                   "ctl00$MainContent$resultsPageSize": "80",
                   "ctl00$MainContent$but_search": "Vyhledat", **extra}
        r2 = self.s.post(SEARCH_URL, data=payload, timeout=30)
        r2.raise_for_status()
        m = TOTAL_RE.search(r2.text)
        return (int(m.group(1)) if m else 0), r2.text

    def by_year(self, year):
        return self._post({"ctl00$MainContent$decidedFrom": f"1.1.{year}",
                           "ctl00$MainContent$decidedTo": f"31.12.{year}"})

    def recent(self, days):
        return self._post({"ctl00$MainContent$dle_data_zpristupneni": "on",
                           "ctl00$MainContent$zpristupneno_pred": str(days)})

    def iter_ids(self, total, first_html, page_size=80):
        seen = set()
        for m in DETAIL_ID_RE.finditer(first_html):
            i = int(m.group(1))
            if i not in seen:
                seen.add(i)
                yield i
        for page in range(2, (total + page_size - 1) // page_size + 1):
            r = self.s.get(RESULTS_URL, params={"page": page}, timeout=30)
            r.raise_for_status()
            for m in DETAIL_ID_RE.finditer(r.text):
                i = int(m.group(1))
                if i not in seen:
                    seen.add(i)
                    yield i
            time.sleep(0.5)

    def detail(self, doc_id):
        r = self.s.get(DETAIL_URL, params={"id": doc_id, "pos": 1, "cnt": 1, "typ": "result"}, timeout=30)
        r.raise_for_status()
        return parse_detail(r.text, doc_id)


def html_to_lines(html):
    html = re.sub(r"(?is)<(script|style).*?</\1>", " ", html)
    html = re.sub(r"(?i)<br\s*/?>|</(p|div|tr|td|th|li|h\d)>", "\n", html)
    txt = re.sub(r"<[^>]+>", " ", html)
    txt = (txt.replace("&nbsp;", " ").replace("&amp;", "&").replace("&lt;", "<")
              .replace("&gt;", ">").replace("&quot;", '"').replace("&#39;", "'"))
    return [re.sub(r"[ \t ]+", " ", l).strip() for l in txt.split("\n")]


def parse_detail(html, doc_id):
    lines = html_to_lines(html)
    try:
        start = next(i for i, l in enumerate(lines) if l.startswith("Karta záznamu"))
    except StopIteration:
        return None
    try:
        tstart = next(i for i in range(start, len(lines)) if lines[i].startswith("Text dokumentu"))
    except StopIteration:
        return None
    fields, i = {}, start + 1
    while i < tstart:
        l = lines[i]
        lab = next((x for x in LABELS if l.startswith(x)), None)
        if lab:
            val = l[len(lab):].strip()
            j = i + 1
            while not val and j < tstart and not any(lines[j].startswith(x) for x in LABELS):
                val = lines[j]
                j += 1
            fields[lab] = val
        i += 1
    body_lines = [l for l in lines[tstart + 1:] if l]
    cut = next((k for k, l in enumerate(body_lines) if l.startswith("Ústavní soud, Joštova")), len(body_lines))
    body = "\n".join(body_lines[:cut]).strip()
    if not body or "Výtah z dokumentu není k dispozici" in body:
        return None
    forma = fields.get("Forma rozhodnutí", "")
    if not forma.lower().startswith("nález"):
        return None  # jen nalezy
    sp = fields.get("Spisová značka") or None
    ecli = fields.get("Identifikátor evropské judikatury") or None
    d = parse_cz_date(fields.get("Datum rozhodnutí"))
    ds = f"{d.day}. {d.month}. {d.year}" if d else ""
    title = f"Nález Ústavního soudu ze dne {ds}, sp. zn. {sp}" + (f", {ecli}" if ecli else "")
    meta = [f"Spisová značka: {sp}" if sp else None, f"ECLI: {ecli}" if ecli else None,
            f"Datum rozhodnutí: {ds}" if ds else None,
            f"Soudce zpravodaj: {fields['Soudce zpravodaj']}" if fields.get("Soudce zpravodaj") else None,
            f"Typ řízení: {fields['Typ řízení']}" if fields.get("Typ řízení") else None,
            f"Typ výroku: {fields['Typ výroku']}" if fields.get("Typ výroku") else None,
            f"Populární název: {fields['Populární název']}" if fields.get("Populární název") else None,
            f"Předmět řízení: {fields['Předmět řízení']}" if fields.get("Předmět řízení") else None]
    content = "\n".join(m for m in meta if m) + "\n\n" + body
    return {"external_id": f"us:{doc_id}", "title": title, "decision_date": d,
            "url": f"{DETAIL_URL}?id={doc_id}", "content": content}


def chunk_text(text, size=CHUNK_SIZE):
    paras, chunks, cur = [p for p in text.split("\n") if p.strip()], [], ""
    for p in paras:
        while len(p) > size:  # velmi dlouhy odstavec
            if cur:
                chunks.append(cur)
                cur = ""
            chunks.append(p[:size])
            p = p[size:]
        if len(cur) + len(p) + 1 > size and cur:
            chunks.append(cur)
            cur = p
        else:
            cur = (cur + "\n" + p) if cur else p
    if cur:
        chunks.append(cur)
    return chunks


def upsert(conn, source_id, doc):
    h = hashlib.sha256(doc["content"].encode("utf-8")).hexdigest()
    with conn.cursor() as cur:
        cur.execute("SELECT id, content_hash FROM documents WHERE source_id=%s AND external_id=%s",
                    (source_id, doc["external_id"]))
        row = cur.fetchone()
        if row and row[1] == h:
            return False
        if row:
            did = row[0]
            cur.execute("""UPDATE documents SET title=%s, decision_date=%s, url=%s, content_hash=%s,
                           updated_at=now(), fetched_at=now() WHERE id=%s""",
                        (doc["title"], doc["decision_date"], doc["url"], h, did))
            cur.execute("DELETE FROM chunks WHERE document_id=%s", (did,))
        else:
            cur.execute("""INSERT INTO documents (source_id, external_id, doc_type, title, issuer,
                           decision_date, url, status, content_hash, fetched_at)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,'platny',%s,now()) RETURNING id""",
                        (source_id, doc["external_id"], DOC_TYPE, doc["title"],
                         "Ústavní soud České republiky", doc["decision_date"], doc["url"], h))
            did = cur.fetchone()[0]
        for idx, c in enumerate(chunk_text(doc["content"])):
            cur.execute("INSERT INTO chunks (document_id, chunk_index, content) VALUES (%s,%s,%s)",
                        (did, idx, c))
    conn.commit()
    return True


def known_ids(conn, source_id):
    with conn.cursor() as cur:
        cur.execute("SELECT external_id FROM documents WHERE source_id=%s", (source_id,))
        return {r[0] for r in cur.fetchall()}


def process(conn, source_id, session, total, first_html, known, label):
    new = 0
    for doc_id in session.iter_ids(total, first_html):
        if f"us:{doc_id}" in known:
            continue  # nalezy se nemeni; preskocit uz ulozene
        try:
            doc = session.detail(doc_id)
        except Exception as e:
            log(f"id={doc_id}: detail selhal: {e}")
            continue
        if not doc:
            continue
        try:
            if upsert(conn, source_id, doc):
                new += 1
        except Exception as e:
            log(f"id={doc_id}: upsert selhal: {e}")
            conn.rollback()
        time.sleep(0.3)
    log(f"{label}: {new} novych nalezu")
    return new


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["backfill", "incremental"], default="incremental")
    ap.add_argument("--year-from", type=int, default=2010)
    ap.add_argument("--year-to", type=int, default=date.today().year)
    ap.add_argument("--days", type=int, default=3)
    a = ap.parse_args()
    conn = db_connect()
    sid = get_source_id(conn)
    log(f"source_id={sid}")
    known = known_ids(conn, sid)
    if a.mode == "incremental":
        s = Nalus()
        total, html = s.recent(a.days)
        log(f"prirustky za {a.days} dni: {total} zaznamu")
        if total:
            process(conn, sid, s, total, html, known, "incremental")
    else:
        for y in range(a.year_from, a.year_to + 1):
            s = Nalus()
            try:
                total, html = s.by_year(y)
            except Exception as e:
                log(f"rok {y}: search selhal: {e}")
                continue
            log(f"rok {y}: {total} nalezu")
            if total:
                process(conn, sid, s, total, html, known, f"rok {y}")
    conn.close()


if __name__ == "__main__":
    sys.exit(main())
