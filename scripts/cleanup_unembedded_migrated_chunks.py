"""
Bezpecnostne mensi bratr scripts/verify_and_cleanup_zakony_supabase.py.

Ten puvodni skript maze CELE dokumenty ze stare Supabase, ale az kdyz maji
v Neonu VSECHNY chunky zembedovane (has_pending_chunks = false) - a Neon
embedding zakonu bezi pomalu (viz task #85), takze tenhle bezpecnostni pas
zbytecne drzi v Supabase i chunky, ktere uz stejne NEJSOU pouzitelne pro
vyhledavani (nemaji embedding ani v Supabase), jen zabiraji misto.

Tenhle skript maze jen TAKOVE CHUNKY (ne cele dokumenty!), ktere:
- patri dokumentu, ktery uz je v Supabase oznacen jako migrovany
  (documents.content_hash = '__migrated_to_neon__'),
- maji v prislusnem Neon shardu PRESNE odpovidajici pocet chunku (integritni
  kontrola - dukaz, ze Neon ma kompletni kopii tohoto dokumentu, i kdyz jeste
  ne vsechny embedovane),
- A SOUCASNE nemaji embedding ani v teto stare Supabase DB (embedding is
  null) - tzn. nejsou prave ted vubec pouzitelne pro semanticke vyhledavani,
  takze jejich smazani NEZHORSI aktualni kvalitu vysledku.

Chunky, ktere v Supabase JESTE embedovane JSOU, se NECHAVAJI na miste (i
kdyz uz jsou taky migrovane) - ty se smazou az puvodnim skriptem, jakmile
Neon dohoni embedding cele dokumenty. Dokument (radek v documents) se
NIKDY nemaze - jen jeho nezembedovane chunky, takze contents zustavaji
konzistentni a nic nehrozi zadnym FK vazbam (explains_document_id,
superseded_by).
"""

import os
import sys

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
import requests

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SERVICE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
MAX_DOCS_PER_RUN = int(os.environ.get("MAX_DOCS_PER_RUN", "2000"))

NEON_URLS = {
    "do1997": os.environ["NEON_ZAKONY_DO1997_DB_URL"],
    "1998_2007": os.environ["NEON_ZAKONY_1998_2007_DB_URL"],
    "2008_2020": os.environ["NEON_ZAKONY_2008_2020_DB_URL"],
    "2021_dosud": os.environ["NEON_ZAKONY_2021_DOSUD_DB_URL"],
}

SESSION = requests.Session()


def log(*a):
    print(*a)
    sys.stdout.flush()


def sb_headers():
    return {
        "apikey": SERVICE_KEY,
        "Authorization": f"Bearer {SERVICE_KEY}",
        "Content-Type": "application/json",
    }


def sb_get(path, params):
    r = SESSION.get(f"{SUPABASE_URL}/rest/v1/{path}", headers=sb_headers(), params=params, timeout=60)
    r.raise_for_status()
    return r.json()


def get_migrated_candidates():
    """Vsechny zakony a duvodove zpravy v Supabase, ktere migracni skript uz
    oznacil jako presunute do Neonu (bez ohledu na to, jak daleko je tam
    embedding)."""
    all_rows = {}
    page = 1000
    offset = 0
    while True:
        rows = sb_get(
            "documents",
            {
                "select": "id,external_id,title",
                "doc_type": "in.(zakon,duvodova_zprava)",
                "content_hash": "eq.__migrated_to_neon__",
                "order": "id.asc",
                "limit": str(page),
                "offset": str(offset),
            },
        )
        if not rows:
            break
        for r in rows:
            all_rows[r["id"]] = r
        offset += page
        if len(rows) < page:
            break
    return all_rows


def get_supabase_chunk_counts(doc_ids):
    """Aktualni pocet VSECH chunku (embedovanych i ne) v Supabase pro dane
    dokumenty - pro integritni porovnani s Neonem."""
    counts = {}
    if not doc_ids:
        return counts
    doc_batch = 20
    ids = list(doc_ids)
    for i in range(0, len(ids), doc_batch):
        batch = ids[i : i + doc_batch]
        page = 1000
        offset = 0
        while True:
            rows = sb_get(
                "chunks",
                {
                    "select": "document_id",
                    "document_id": f"in.({','.join(batch)})",
                    "order": "id.asc",
                    "limit": str(page),
                    "offset": str(offset),
                },
            )
            if not rows:
                break
            for r in rows:
                counts[r["document_id"]] = counts.get(r["document_id"], 0) + 1
            offset += page
            if len(rows) < page:
                break
    return counts


def get_neon_all_chunk_counts(shard_key):
    """Pro dany shard vrati {doc_id: pocet_chunku} pro VSECHNY dokumenty
    (bez ohledu na embedding) - dukaz, ze Neon uz ma kompletni kopii."""
    conn = db_connect(NEON_URLS[shard_key])
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                select d.id, count(c.id)
                from documents d
                join chunks c on c.document_id = d.id
                group by d.id
                """
            )
            rows = cur.fetchall()
        return {str(doc_id): cnt for doc_id, cnt in rows}
    finally:
        conn.close()


def delete_unembedded_chunks_batch(doc_ids):
    """Smaze jen chunky (embedding is null) patrici danym dokumentum -
    dokument samotny (radek v documents) zustava netknuty."""
    if not doc_ids:
        return
    ids_expr = f"in.({','.join(doc_ids)})"
    r = SESSION.delete(
        f"{SUPABASE_URL}/rest/v1/chunks",
        headers={**sb_headers(), "Prefer": "return=minimal"},
        params={"document_id": ids_expr, "embedding": "is.null"},
        timeout=60,
    )
    if r.status_code >= 300:
        raise RuntimeError(f"Mazani nezembedovanych chunku selhalo: {r.status_code} {r.text[:300]}")


def main():
    log("Overuji migrovane zakony a mazu jejich JESTE NEZEMBEDOVANE chunky ze Supabase...")

    candidates = get_migrated_candidates()
    log(f"Kandidatu (oznaceno jako migrovano v Supabase): {len(candidates)}")
    if not candidates:
        log("Zadni kandidati, konec.")
        return

    neon_totals = {}
    for shard_key in NEON_URLS:
        try:
            shard_map = get_neon_all_chunk_counts(shard_key)
            log(f"  [{shard_key}] dokumentu v Neonu (s aspon 1 chunkem): {len(shard_map)}")
            neon_totals.update(shard_map)
        except Exception as e:
            log(f"  [{shard_key}] CHYBA pri cteni z Neonu, tento shard preskakuji: {e}")

    present_in_neon = [doc_id for doc_id in candidates if doc_id in neon_totals]
    log(f"Kandidatu s existujici kopii v Neonu: {len(present_in_neon)}")
    if not present_in_neon:
        log("Zadny dokument zatim nema kopii v Neonu, konec.")
        return

    to_check = present_in_neon[:MAX_DOCS_PER_RUN]
    if len(present_in_neon) > MAX_DOCS_PER_RUN:
        log(f"Omezuji integritni kontrolu na MAX_DOCS_PER_RUN={MAX_DOCS_PER_RUN}, zbytek doplni dalsi beh.")

    sb_counts = get_supabase_chunk_counts(to_check)

    safe_doc_ids = []
    skipped_mismatch = 0
    for doc_id in to_check:
        neon_count = neon_totals.get(doc_id, 0)
        sb_count = sb_counts.get(doc_id, 0)
        if neon_count != sb_count or neon_count == 0:
            skipped_mismatch += 1
            continue
        safe_doc_ids.append(doc_id)

    log(
        f"Integritne overenych (pocet chunku v Neonu presne sedi s aktualnim poctem v Supabase): "
        f"{len(safe_doc_ids)} | preskoceno kvuli nesouladu: {skipped_mismatch}"
    )
    if not safe_doc_ids:
        log("Po integritni kontrole nezbyl zadny bezpecny kandidat, konec bez mazani.")
        return

    batch_size = 50
    processed = 0
    for i in range(0, len(safe_doc_ids), batch_size):
        batch = safe_doc_ids[i : i + batch_size]
        try:
            delete_unembedded_chunks_batch(batch)
            processed += len(batch)
            log(f"Zpracovano {processed}/{len(safe_doc_ids)} dokumentu (mazani nezembedovanych chunku)...")
        except Exception as e:
            log(f"CHYBA behem mazani davky, preskakuji ji: {e}")

    log(f"Hotovo. Zpracovano {processed} dokumentu - jejich nezembedovane chunky smazany ze stare Supabase.")


if __name__ == "__main__":
    main()
