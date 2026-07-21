"""
Presun textoveho obsahu useku (chunks.content) z Postgres do Supabase
Storage (bucket "chunk-content"), aby se uvolnilo misto v 500MB DB limitu
free tieru (viz pamet asistenta: storage_architecture_findings).

Duvod: chunks.content byl k 2026-07-21 zodpovedny za 236MB z 376MB celkove
velikosti DB (91% tabulky chunks), a embeddingy porostou o dalsich ~190MB,
jak se dokoncuje embedovaci fronta - bez tohoto presunu by DB brzy presahla
500MB limit jeste pred dokoncenim embedovani cele databaze.

Postup pro kazdy chunk (content is not null AND content_migrated = false):
1. Nahrajeme surovy text jako objekt "{chunk_id}.txt" do privatniho
   Storage bucketu "chunk-content" (pristupny jen pres service_role klic -
   viz upravene edge funkce ai-query / review-document, ktere ho pri
   sestavovani odpovedi cetou).
2. Po uspesnem nahrani zavolame RPC finalize_chunk_storage_migration_batch,
   ktera v JEDNOM atomickem SQL prikazu spocita content_tsv (plnotextovy
   index pro zalozni vyhledavani klicovych slov - viz keywordFallbackSearch
   v edge funkcich), vynuluje content a nastavi content_migrated=true.
   Diky tomu nikdy nenastane stav "content=null, ale tsv nespocitany".

Skript je idempotentni a bezpecne opakovatelny - vyber "content_migrated =
false" zajisti, ze uz migrovane radky se znovu nezpracovavaji, i kdyz beh
skoncil predcasne (timeout GH Actions) nebo byl spusten vicekrat rucne.

MAX_RUNTIME_SECONDS je pojistka proti 60min timeoutu GitHub Actions -
skript se sam ukonci nekolik minut pred limitem a necha zbytek na dalsi
beh (cron nebo rucni "Run workflow").
"""

import os
import time
import concurrent.futures

import requests

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SERVICE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

BUCKET = "chunk-content"
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "200"))
MAX_ITEMS = int(os.environ.get("MAX_ITEMS", "200000"))
CONCURRENCY = int(os.environ.get("CONCURRENCY", "15"))
MAX_RUNTIME_SECONDS = int(os.environ.get("MAX_RUNTIME_SECONDS", "3000"))

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "ai-semanticke-zakony/1.0 (chunk-storage-migration)"})


def log(*a):
    print(*a, flush=True)


def sb_headers(content_type="application/json"):
    return {
        "apikey": SERVICE_KEY,
        "Authorization": f"Bearer {SERVICE_KEY}",
        "Content-Type": content_type,
    }


def fetch_batch():
    r = SESSION.get(
        f"{SUPABASE_URL}/rest/v1/chunks",
        headers=sb_headers(),
        params={
            "select": "id,content",
            "content_migrated": "eq.false",
            "content": "not.is.null",
            "order": "id.asc",
            "limit": str(BATCH_SIZE),
        },
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def upload_one(chunk):
    chunk_id = chunk["id"]
    content = chunk.get("content") or ""
    try:
        r = SESSION.post(
            f"{SUPABASE_URL}/storage/v1/object/{BUCKET}/{chunk_id}.txt",
            headers={
                "apikey": SERVICE_KEY,
                "Authorization": f"Bearer {SERVICE_KEY}",
                "Content-Type": "text/plain; charset=utf-8",
                "x-upsert": "true",
            },
            data=content.encode("utf-8"),
            timeout=30,
        )
        if r.ok:
            return chunk_id
        log(f"  upload selhal pro {chunk_id}: {r.status_code} {r.text[:200]}")
        return None
    except requests.exceptions.RequestException as e:
        log(f"  upload chyba pro {chunk_id}: {e}")
        return None


def finalize_batch(chunk_ids):
    if not chunk_ids:
        return 0
    r = SESSION.post(
        f"{SUPABASE_URL}/rest/v1/rpc/finalize_chunk_storage_migration_batch",
        headers=sb_headers(),
        json={"p_chunk_ids": chunk_ids},
        timeout=60,
    )
    if not r.ok:
        log(f"  finalize_chunk_storage_migration_batch selhalo: {r.status_code} {r.text[:300]}")
        return 0
    return r.json()


def main():
    log("=== Presun chunks.content do Supabase Storage: start ===")
    log(f"MAX_ITEMS={MAX_ITEMS} BATCH_SIZE={BATCH_SIZE} CONCURRENCY={CONCURRENCY}")
    start = time.monotonic()
    total_migrated = 0
    total_upload_failed = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENCY) as pool:
        while total_migrated < MAX_ITEMS:
            if time.monotonic() - start > MAX_RUNTIME_SECONDS:
                log("Blizi se timeout GitHub Actions, koncim - zbytek dobehne v pristim behu.")
                break

            batch = fetch_batch()
            if not batch:
                log("Zadne dalsi nemigrovane chunky - hotovo.")
                break

            uploaded_ids = list(pool.map(upload_one, batch))
            successful_ids = [cid for cid in uploaded_ids if cid]
            failed_count = len(batch) - len(successful_ids)
            total_upload_failed += failed_count

            finalized = finalize_batch(successful_ids)
            total_migrated += finalized

            log(f"  davka: {len(batch)} chunku, nahrano {len(successful_ids)}, financalizovano {finalized}, selhalo {failed_count} (celkem migrovano {total_migrated})")

            if len(batch) < BATCH_SIZE:
                log("Posledni (neuplna) davka zpracovana - hotovo.")
                break

    elapsed = time.monotonic() - start
    log(f"=== Hotovo: migrovano {total_migrated} chunku, selhalych nahrani {total_upload_failed}, cas {elapsed:.0f}s ===")


if __name__ == "__main__":
    main()
