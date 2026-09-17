"""
Uklidi OSIRELE objekty v Supabase Storage bucketu "chunk-content".

KONTEXT: kdyz migrate_chunks_to_storage.py presunul obsah chunku z tabulky
chunks.content do tohoto bucketu (kazdy chunk jako soubor "{chunk_id}.txt"),
pocitalo se s tim, ze chunk radek v Postgresu (s content_migrated=true) dal
zustane a jen ukazuje na soubor ve Storage. Jenze pozdejsi
verify_and_cleanup_zakony_supabase.py (uklid po overene migraci do Neonu)
mimo jine mimo jine maze i cele radky z tabulky chunks (kaskadove pres
documents) - a NEMAZAL pritom odpovidajici soubor ve Storage. Vysledek:
soubor ve "chunk-content" zustal navzdy viset, i kdyz uz na nej nic
neukazuje - a to i pro 266 000+ objektu (~484 MB), viz pamet asistenta
chunk_content_orphan_cleanup_2026-09-17.

Tento skript porovna, ktere objekty v bucketu jeste maji odpovidajici radek
v public.chunks (ty NECHAT - pouziva je aktivne ai-query i review-document
pres fetchStorageContent, kdyz je chunks.content prazdny) a ktere uz radek
nemaji (ty jsou osirele a bezpecne smazatelne).

Rezim DRY_RUN=true (vychozi): jen spocita a vypise, nic nemaze.
Rezim DRY_RUN=false: opravdu mazat po davkach (Storage bulk delete API).

Idempotentni a bezpecne opakovatelny - kazdy beh znovu overi aktualni stav
tabulky chunks pred smazanim, takze i kdyz mezitim pribyl novy migrovany
chunk, jeho soubor se nesmaze.
"""

import os
import time

import requests

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SERVICE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

BUCKET = "chunk-content"
LIST_PAGE_SIZE = 1000
DELETE_BATCH_SIZE = 1000
DRY_RUN = os.environ.get("DRY_RUN", "true").strip().lower() != "false"
MAX_RUNTIME_SECONDS = int(os.environ.get("MAX_RUNTIME_SECONDS", "3000"))

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "ai-semanticke-zakony/1.0 (chunk-storage-orphan-cleanup)"})


def log(*a):
    print(*a, flush=True)


def sb_headers(content_type="application/json"):
    return {
        "apikey": SERVICE_KEY,
        "Authorization": f"Bearer {SERVICE_KEY}",
        "Content-Type": content_type,
    }


def fetch_existing_chunk_ids() -> set:
    """Nacte VSECHNY aktualni chunks.id (dnes jen ~stovky radku, i kdyby
    jich bylo vic, strankuje po 10000)."""
    ids = set()
    offset = 0
    page = 10000
    while True:
        r = SESSION.get(
            f"{SUPABASE_URL}/rest/v1/chunks",
            headers=sb_headers(),
            params={"select": "id", "limit": page, "offset": offset},
            timeout=30,
        )
        r.raise_for_status()
        rows = r.json()
        if not rows:
            break
        ids.update(row["id"] for row in rows)
        if len(rows) < page:
            break
        offset += page
    return ids


def list_all_objects() -> list:
    """Vypise vsechny objekty v bucketu (nazev = "{chunk_id}.txt")."""
    names = []
    offset = 0
    while True:
        r = SESSION.post(
            f"{SUPABASE_URL}/storage/v1/object/list/{BUCKET}",
            headers=sb_headers(),
            json={"limit": LIST_PAGE_SIZE, "offset": offset, "sortBy": {"column": "name", "order": "asc"}},
            timeout=30,
        )
        r.raise_for_status()
        rows = r.json()
        if not rows:
            break
        names.extend(row["name"] for row in rows)
        if len(rows) < LIST_PAGE_SIZE:
            break
        offset += LIST_PAGE_SIZE
    return names


def delete_batch(paths: list) -> int:
    for attempt in range(5):
        r = SESSION.request(
            "DELETE",
            f"{SUPABASE_URL}/storage/v1/object/{BUCKET}",
            headers=sb_headers(),
            json={"prefixes": paths},
            timeout=60,
        )
        if r.status_code >= 500:
            wait = 10 * (attempt + 1)
            log(f"   delete_batch chyba {r.status_code}, cekam {wait}s a zkusim znovu...")
            time.sleep(wait)
            continue
        if not r.ok:
            log(f"   delete_batch selhalo: {r.status_code} {r.text[:300]}")
            return 0
        return len(r.json()) if isinstance(r.json(), list) else len(paths)
    log("   delete_batch selhalo po 5 pokusech, zbytek davky preskocen (dalsi beh to zkusi znovu).")
    return 0


def main():
    log("=== Uklid osirelych objektu v chunk-content: start ===")
    log(f"DRY_RUN={DRY_RUN}")
    start = time.monotonic()

    log("Nacitam aktualni chunks.id...")
    existing_ids = fetch_existing_chunk_ids()
    log(f"  aktualnich chunku v Postgresu: {len(existing_ids)}")

    log("Vypisuji vsechny objekty v bucketu (muze chvili trvat, ~267 stranek)...")
    all_names = list_all_objects()
    log(f"  celkem objektu v bucketu: {len(all_names)}")

    orphaned = [n for n in all_names if n[:-4] not in existing_ids and n.endswith(".txt")]
    kept = len(all_names) - len(orphaned)
    log(f"  osirelych (bez odpovidajiciho chunk radku): {len(orphaned)}")
    log(f"  ponechano (aktivne pouzivanych): {kept}")

    if DRY_RUN:
        log("DRY_RUN=true - nic se nemaze. Pro skutecne smazani spustte znovu s DRY_RUN=false.")
        return

    log(f"Mazu {len(orphaned)} osirelych objektu po davkach po {DELETE_BATCH_SIZE}...")
    deleted_total = 0
    for i in range(0, len(orphaned), DELETE_BATCH_SIZE):
        if time.monotonic() - start > MAX_RUNTIME_SECONDS:
            log("Blizi se timeout GitHub Actions, koncim - zbytek dobehne v pristim behu (skript je idempotentni).")
            break
        batch = orphaned[i : i + DELETE_BATCH_SIZE]
        deleted = delete_batch(batch)
        deleted_total += deleted
        log(f"  davka {i // DELETE_BATCH_SIZE + 1}: smazano {deleted}/{len(batch)} (celkem zatim {deleted_total})")

    log(f"=== Hotovo. Smazano celkem {deleted_total} osirelych objektu. ===")


if __name__ == "__main__":
    main()
