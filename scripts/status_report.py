"""
Rychly prehled aktualniho stavu vsech Neon shardu zakonu - "kolik je
hotovo" na jedno kliknuti (tlacitko 8 v NAS_Sprava.ps1), bez nutnosti
rucne prochazet logy jednotlivych behu reshardovani/embeddingu/cisteni.

Pro kazdy novy (reshard) shard vypise:
  - kolik dokumentu/chunku celkem
  - kolik chunku jeste ceka na embedding
  - kolik dokumentu je UPLNE hotovych (vsechny chunky zaembedovane)

Pro kazdy stary (zdrojovy) shard vypise:
  - kolik dokumentu celkem jeste ceka na reshardovani (has_pending_chunks)
  - kolik dokumentu uz ma hotovou (plne zaembedovanou) kopii v nekterem
    novem shardu, tedy je bezpecne ke smazani (viz cleanup_old_zakony_shards.py)

Pouze cte data (zadne UPDATE/DELETE) - bezpecne spoustet kdykoliv,
opakovane, bez rizika.
"""

import os
import sys
import time
import psycopg2

SOURCE_SHARDS = [
    ("do1997", "NEON_ZAKONY_DO1997_DB_URL"),
    ("1998_2007", "NEON_ZAKONY_1998_2007_DB_URL"),
    ("2008_2020", "NEON_ZAKONY_2008_2020_DB_URL"),
    ("2021_dosud", "NEON_ZAKONY_2021_DOSUD_DB_URL"),
]

TARGET_SHARDS = [
    ("reshard-01", "NEON_RESHARD_01_DB_URL"),
    ("reshard-02", "NEON_RESHARD_02_DB_URL"),
    ("reshard-03", "NEON_RESHARD_03_DB_URL"),
    ("reshard-04", "NEON_RESHARD_04_DB_URL"),
    ("reshard-05", "NEON_RESHARD_05_DB_URL"),
    ("reshard-06", "NEON_RESHARD_06_DB_URL"),
]


def log(*a):
    print(*a)
    sys.stdout.flush()


def db_connect(url, timeout=15):
    last_err = None
    for attempt in range(8):
        try:
            return psycopg2.connect(url, connect_timeout=timeout, keepalives=1, keepalives_idle=30, keepalives_interval=10, keepalives_count=3)
        except Exception as e:
            last_err = e
            log(f"db_connect selhalo (pokus {attempt + 1}/8): {e}")
            time.sleep(min(10 * (attempt + 1), 45))
    raise last_err


def get_fully_embedded_doc_ids(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            select document_id
            from chunks
            group by document_id
            having count(*) > 0 and count(*) filter (where embedding is null) = 0
            """
        )
        return {row[0] for row in cur.fetchall()}


def main():
    log("=== POKROK - stav vsech shardu k tomuto okamziku ===")

    fully_embedded_ids = set()
    migrated_ids = set()
    for name, env_key in TARGET_SHARDS:
        if env_key not in os.environ:
            log(f"   [{name}] preskoceno - env var {env_key} neni nastavena.")
            continue
        conn = db_connect(os.environ[env_key])
        try:
            with conn.cursor() as cur:
                cur.execute("select count(*) from documents")
                doc_total = cur.fetchone()[0]
                cur.execute("select count(*) from chunks")
                chunk_total = cur.fetchone()[0]
                cur.execute("select count(*) from chunks where embedding is null")
                chunk_pending = cur.fetchone()[0]
            ids = get_fully_embedded_doc_ids(conn)
            fully_embedded_ids |= ids
            with conn.cursor() as cur:
                cur.execute("select id from documents")
                migrated_ids |= {r[0] for r in cur.fetchall()}
            pct = (100.0 * (chunk_total - chunk_pending) / chunk_total) if chunk_total else 100.0
            log(
                f"   [{name}] dokumentu celkem: {doc_total}, chunku celkem: {chunk_total}, "
                f"chunku ceka na embedding: {chunk_pending} ({pct:.1f}% hotovo), "
                f"plne hotovych dokumentu: {len(ids)}"
            )
        finally:
            conn.close()

    log(f"Celkem napric vsemi novymi shardy: {len(fully_embedded_ids)} unikatnich plne zaembedovanych dokumentu.")

    for name, env_key in SOURCE_SHARDS:
        if env_key not in os.environ:
            log(f"   [{name}] preskoceno - env var {env_key} neni nastavena.")
            continue
        conn = db_connect(os.environ[env_key])
        try:
            with conn.cursor() as cur:
                cur.execute("select count(*) from documents")
                doc_total = cur.fetchone()[0]
                cur.execute("select count(*), count(*) filter (where embedding is not null) from chunks")
                _ct, _ce = cur.fetchone()
                log(f"   [{name}] STARY SHARD chunku celkem: {_ct}, z toho s embeddingem: {_ce}")
                if migrated_ids:
                    cur.execute(
                        "select count(*) from documents where id = any(%s::uuid[])",
                        (list(migrated_ids),),
                    )
                    doc_migrated = cur.fetchone()[0]
                else:
                    doc_migrated = 0
                doc_pending_reshard = doc_total - doc_migrated
                if fully_embedded_ids:
                    cur.execute(
                        "select count(*) from documents where id = any(%s::uuid[])",
                        (list(fully_embedded_ids),),
                    )
                    doc_ready_cleanup = cur.fetchone()[0]
                else:
                    doc_ready_cleanup = 0
            log(
                f"   [{name}] dokumentu celkem: {doc_total}, jeste NEzmigrovano: {doc_pending_reshard} (zmigrovano {doc_migrated}), "
                f"pripraveno ke smazani (hotova kopie jinde): {doc_ready_cleanup}"
            )
        finally:
            conn.close()

    log("=== KONEC POKROKU ===")


if __name__ == "__main__":
    main()
