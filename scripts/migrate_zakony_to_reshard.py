"""
Kopie pravnich predpisu z hlavni Supabase DB (jen pruchozi "staging" po
stazeni z psp.cz) RADOVE do reshardu - nahrazuje migrate_zakony_to_neon.py,
ktery psal do 4 puvodnich rocnikovych shardu (Radek 2026-10-08).

Co dela:
- zakony/vyhlasky/narizeni/... (ZAKONY_DOC_TYPES) z Supabase, ktere jeste nemaji
  marker content_hash='__migrated_to_neon__', zkopiruje do prvniho reshardu
  s volnym mistem (stejna pravidla rozpoctu jako reshard_and_clean_zakony.py),
  VCETNE ocisteni HTML a predpocitaneho search_tsv (embedding zustane NULL a
  doplni ho embed_zakony_neon.py).
- duvodove zpravy zapise take do prvniho reshardu s volnym mistem (NEMUSI byt
  ve stejnem reshardu jako zakon - aplikace sloupec explains_document_id nepouziva).
  Odkaz explains_document_id se nastavi jen, kdyz zakon nahodou lezi ve stejnem reshardu.
- po zkopirovani oznaci dokument v Supabase markerem (maze az
  verify_and_cleanup_zakony_supabase.py po overeni obsahu).
- dokument, ktery uz v nejakem reshardu je, se jen oznaci (nekopiruje se znovu).
"""

import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import requests

from reshard_and_clean_zakony import (
    TARGET_SHARDS,
    connect_target,
    ensure_target_conn,
    get_shard_size_bytes,
    effective_migration_budget_bytes,
    write_document_and_chunks_with_retry,
)

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SERVICE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
TIME_BUDGET_SECONDS = int(os.environ.get("TIME_BUDGET_SECONDS", "3000"))
MARK_BATCH_SIZE = 25
STORAGE_FETCH_WORKERS = 8
START_TIME = time.time()
SESSION = requests.Session()

ZAKONY_DOC_TYPES = ["zakon", "vyhlaska", "narizeni", "opatreni", "dekret", "jiny_predpis"]
DOC_COLUMNS = (
    "id,source_id,external_id,doc_type,title,issuer,decision_date,effective_date,"
    "url,status,content_hash,fetched_at,created_at,updated_at,skip_embedding,"
    "embed_priority,version_iri,valid_from,valid_until,superseded_by,is_current,"
    "explains_document_id,predpis_cislo,predpis_rok"
)
# POZOR: samotne "neq" by vyradilo i radky s content_hash IS NULL.
NOT_MIGRATED = "(content_hash.is.null,content_hash.neq.__migrated_to_neon__)"


def log(*a):
    print(*a)
    sys.stdout.flush()


def time_left():
    return TIME_BUDGET_SECONDS - (time.time() - START_TIME)


def sb_headers():
    return {"apikey": SERVICE_KEY, "Authorization": "Bearer " + SERVICE_KEY}


def sb_get(path, params):
    last = None
    for attempt in range(6):
        try:
            r = SESSION.get(SUPABASE_URL + "/rest/v1/" + path, headers=sb_headers(), params=params, timeout=60)
            if r.status_code >= 500:
                raise RuntimeError("HTTP " + str(r.status_code))
            r.raise_for_status()
            return r.json()
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout, RuntimeError) as e:
            last = e
            wait = 10 * (attempt + 1)
            log("  sb_get " + path + " chyba (" + str(e) + "), cekam " + str(wait) + "s...")
            time.sleep(wait)
    raise RuntimeError("sb_get(" + path + ") selhalo: " + str(last))


def fetch_storage_content(chunk_id):
    for attempt in range(5):
        r = SESSION.get(
            SUPABASE_URL + "/storage/v1/object/chunk-content/" + chunk_id + ".txt",
            headers=sb_headers(),
            timeout=30,
        )
        if r.status_code >= 500:
            time.sleep(5 * (attempt + 1))
            continue
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.text
    raise RuntimeError("fetch_storage_content(" + chunk_id + ") selhalo")


def fetch_chunks(doc_id):
    """Vsechny chunky dokumentu (strankovane); obsah vyvedeny do Storage se
    dotahne. Pokud nektery obsah chybi, vyhodi vyjimku (dokument se
    preskoci - nikdy nezapisujeme chunk s prazdnym obsahem)."""
    chunks = []
    offset = 0
    page = 1000
    while True:
        batch = sb_get(
            "chunks",
            {
                "select": "id,document_id,chunk_index,heading,content,content_migrated",
                "document_id": "eq." + doc_id,
                "order": "chunk_index.asc",
                "limit": str(page),
                "offset": str(offset),
            },
        )
        if not batch:
            break
        chunks.extend(batch)
        offset += page
        if len(batch) < page:
            break
    missing = [c["id"] for c in chunks if c.get("content") is None and c.get("content_migrated")]
    if missing:
        with ThreadPoolExecutor(max_workers=STORAGE_FETCH_WORKERS) as ex:
            res = dict(zip(missing, ex.map(fetch_storage_content, missing)))
        for c in chunks:
            if c["id"] in res:
                if res[c["id"]] is None:
                    raise RuntimeError("chybi obsah chunku " + c["id"] + " ve Storage")
                c["content"] = res[c["id"]]
    return chunks


def fetch_candidates(extra):
    params = {"select": DOC_COLUMNS, "or": NOT_MIGRATED, "order": "id.asc"}
    params.update(extra)
    rows = []
    offset = 0
    page = 500
    while True:
        p = dict(params)
        p["limit"] = str(page)
        p["offset"] = str(offset)
        batch = sb_get("documents", p)
        rows.extend(batch)
        offset += page
        if len(batch) < page:
            break
    return rows


def mark_migrated(doc_ids):
    if not doc_ids:
        return
    for attempt in range(5):
        r = SESSION.patch(
            SUPABASE_URL + "/rest/v1/documents",
            headers={**sb_headers(), "Content-Type": "application/json", "Prefer": "return=minimal"},
            params={"id": "in.(" + ",".join(doc_ids) + ")"},
            json={"content_hash": "__migrated_to_neon__", "skip_embedding": True},
            timeout=30,
        )
        if r.status_code >= 500:
            time.sleep(5 * (attempt + 1))
            continue
        if r.status_code >= 300:
            log("  mark_migrated(" + str(len(doc_ids)) + ") neuspesne: " + str(r.status_code) + " " + r.text[:200])
        return
    log("  mark_migrated selhalo po 5 pokusech, pokracuji bez oznaceni")


class Shards:
    def __init__(self):
        self.env = {}
        self.conn = {}
        self.doc_shard = {}
        for name, env_key in TARGET_SHARDS:
            if not os.environ.get(env_key):
                continue
            conn = connect_target(env_key, retries=8)
            with conn.cursor() as cur:
                cur.execute("select id from documents")
                for (i,) in cur.fetchall():
                    self.doc_shard[str(i)] = name
            conn.commit()
            self.env[name] = env_key
            self.conn[name] = conn
        log("Reshardy nacteny: " + str(len(self.conn)) + ", znamych dokumentu: " + str(len(self.doc_shard)))

    def active_conn(self, name):
        self.conn[name] = ensure_target_conn(self.conn[name], self.env[name])
        return self.conn[name]

    def pick_active(self):
        for name in self.conn:
            conn = self.active_conn(name)
            if get_shard_size_bytes(conn) < effective_migration_budget_bytes(conn):
                return name
        return None

    def write(self, name, doc, chunks):
        self.conn[name] = write_document_and_chunks_with_retry(
            self.active_conn(name), self.env[name], doc, chunks
        )
        self.doc_shard[doc["id"]] = name

    def link_explains(self, name, doc_id, law_id):
        conn = self.active_conn(name)
        with conn.cursor() as cur:
            cur.execute("update documents set explains_document_id = %s where id = %s", (law_id, doc_id))
        conn.commit()

    def close(self):
        for c in self.conn.values():
            try:
                c.close()
            except Exception:
                pass


def main():
    log("Migrace predpisu ze Supabase do reshardu - start.")
    laws = fetch_candidates({"doc_type": "in.(" + ",".join(ZAKONY_DOC_TYPES) + ")"})
    dzs = fetch_candidates({"doc_type": "eq.duvodova_zprava"})
    log("Kandidatu: predpisy " + str(len(laws)) + ", duvodove zpravy " + str(len(dzs)))
    if not laws and not dzs:
        log("Nic k migraci, konec.")
        return

    shards = Shards()
    pending = []
    copied = 0
    already = 0
    failed = 0
    try:
        for doc in laws:
            if time_left() <= 60:
                log("Casovy rozpocet vycerpan.")
                break
            try:
                if doc["id"] in shards.doc_shard:
                    already += 1
                else:
                    name = shards.pick_active()
                    if name is None:
                        log("STOP: vsechny reshardy jsou plne - je treba zalozit dalsi reshard.")
                        break
                    shards.write(name, doc, fetch_chunks(doc["id"]))
                    copied += 1
                pending.append(doc["id"])
            except Exception as e:
                failed += 1
                log("CHYBA u dokumentu " + doc["id"] + ": " + str(e))
                continue
            if len(pending) >= MARK_BATCH_SIZE:
                mark_migrated(pending)
                pending = []
        mark_migrated(pending)
        pending = []

        for dz in dzs:
            if time_left() <= 30:
                break
            law_id = dz.get("explains_document_id")
            try:
                if dz["id"] in shards.doc_shard:
                    already += 1
                    name = shards.doc_shard[dz["id"]]
                else:
                    name = shards.pick_active()
                    if name is None:
                        log("STOP: vsechny reshardy jsou plne - je treba zalozit dalsi reshard.")
                        break
                    shards.write(name, dz, fetch_chunks(dz["id"]))
                    copied += 1
                # Odkaz na zakon je jen informativni (ai-query ho nepouziva); FK
                # funguje jen v ramci jednoho reshardu, proto ho nastavime jen tam.
                if law_id and shards.doc_shard.get(law_id) == name:
                    shards.link_explains(name, dz["id"], law_id)
                pending.append(dz["id"])
            except Exception as e:
                failed += 1
                log("CHYBA u duvodove zpravy " + dz["id"] + ": " + str(e))
        mark_migrated(pending)
        log("Hotovo. Zkopirovano " + str(copied) + ", jiz v reshardu " + str(already)
            + ", chyb " + str(failed))
    finally:
        shards.close()


if __name__ == "__main__":
    main()
