"""
Uklid starych 4 Neon shardu zakonu (do1997 / 1998_2007 / 2008_2020 / 2021_dosud)
- FAZE 2 (Radek 2026-09-27): dokument se ze stareho shardu smaze POUZE pokud:
  1) uz existuje (byl migrovan, stejne document.id) v nekterem z novych
     reshard-* shardu (viz reshard_and_clean_zakony.py), A ZAROVEN
  2) v tom novem shardu uz ma VSECHNY sve chunky realne zaembedovane
     (embedding is not null) - tedy je uplne hotovy, ne jen zkopirovany.

BEZPECNOSTNI POJISTKA (dulezite): stare shardy jsou soucasne ZDROJEM, ze
ktereho reshard_and_clean_zakony.py teprve migruje dokumenty, ktere jeste
nezpracoval. Tenhle skript proto NIKDY nemaze na zaklade "needembedovanosti"
ve starem shardu samotnem - jen na zaklade toho, ze prokazatelne uz existuje
HOTOVA (zaembedovana) kopie v novem shardu. Bez teto podminky by hrozila
nenavratna ztrata jeste nemigrovanych dat.

Rezim behu (VZDY explicitne, zadne tiche vychozi chovani k horsimu):
  - DRY_RUN=true (VYCHOZI): jen spocita a vypise, kolik dokumentu/chunku by
    se ve kterem starem shardu smazalo, NIC nesmaze.
  - Pro skutecne smazani je potreba SOUCASNE DRY_RUN=false A
    CONFIRM_DELETE=yes - jen jeden z techto prepinacu nestaci.
"""

import os
import sys
import time
import psycopg2

DRY_RUN = os.environ.get("DRY_RUN", "true").strip().lower() != "false"
CONFIRM_DELETE = os.environ.get("CONFIRM_DELETE", "no").strip().lower() == "yes"
REALLY_DELETE = (not DRY_RUN) and CONFIRM_DELETE

# Stare (zdrojove) shardy - stejne jako SOURCE_SHARDS v reshard_and_clean_zakony.py.
SOURCE_SHARDS = [
    ("do1997", "NEON_ZAKONY_DO1997_DB_URL"),
    ("1998_2007", "NEON_ZAKONY_1998_2007_DB_URL"),
    ("2008_2020", "NEON_ZAKONY_2008_2020_DB_URL"),
    ("2021_dosud", "NEON_ZAKONY_2021_DOSUD_DB_URL"),
]

# Nove (cistene) reshard shardy - stejne jako TARGET_SHARDS v
# reshard_and_clean_zakony.py. Volitelne (jen pokud je secret nastaveny).
TARGET_SHARD_CANDIDATES = [
    ("reshard-01", "NEON_RESHARD_01_DB_URL"),
    ("reshard-02", "NEON_RESHARD_02_DB_URL"),
    ("reshard-03", "NEON_RESHARD_03_DB_URL"),
    ("reshard-04", "NEON_RESHARD_04_DB_URL"),
    ("reshard-05", "NEON_RESHARD_05_DB_URL"),
    ("reshard-06", "NEON_RESHARD_06_DB_URL"),
    ("reshard-07", "NEON_RESHARD_07_DB_URL"),
    ("reshard-08", "NEON_RESHARD_08_DB_URL"),
    ("reshard-09", "NEON_RESHARD_09_DB_URL"),
]


def log(*a):
    print(*a)
    sys.stdout.flush()


def db_connect(url, timeout=15):
    last_err = None
    for attempt in range(15):
        try:
            return psycopg2.connect(url, connect_timeout=timeout, keepalives=1, keepalives_idle=30, keepalives_interval=10, keepalives_count=3)
        except Exception as e:
            last_err = e
            log(f"db_connect selhalo (pokus {attempt + 1}/15): {e}")
            time.sleep(15)
    raise last_err


def get_fully_embedded_doc_ids(conn):
    """Vraci mnozinu document_id, ktere v tomto (novem) shardu maji VSECHNY
    sve chunky zaembedovane (embedding is not null) a alespon 1 chunk."""
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


def get_doc_stats(conn, ids=None):
    """{document_id: (pocet_chunku, pocet_zaembedovanych)}"""
    with conn.cursor() as cur:
        if ids is None:
            cur.execute("select document_id, count(*), count(*) filter (where embedding is not null) from chunks group by document_id")
        else:
            cur.execute("select document_id, count(*), count(*) filter (where embedding is not null) from chunks where document_id = any(%s::uuid[]) group by document_id", (list(ids),))
        return {r[0]: (r[1], r[2]) for r in cur.fetchall()}


def delete_documents(conn, doc_ids, really_delete):
    """Smaze dane dokumenty (a jejich chunky) ze stareho shardu. Vraci
    (pocet_dokumentu, pocet_chunku) - realne smazanych, nebo (jen pro info)
    kolik by se smazalo v DRY_RUN rezimu.

    BEZPECNOSTNI POJISTKA (Radek/oprava 2026-09-30): pokud je dokument
    stale odkazovan jako superseded_by z jineho dokumentu, ktery SAM jeste
    smazan neni (typicky proto, ze jeste neni migrovan/zaembedovan), NESMI
    se smazat - jinak by to spadlo na cizim klici, nebo (kdyby omezeni
    nebylo) by se ztratila historicka vazba retezce novelizaci. Takove
    dokumenty se z davky proste vynechaji a pockaji na pristi beh."""
    if not doc_ids:
        return 0, 0
    doc_ids = list(doc_ids)
    with conn.cursor() as cur:
        cur.execute(
            "select distinct superseded_by from documents "
            "where superseded_by = any(%s::uuid[]) and not (id = any(%s::uuid[]))",
            (doc_ids, doc_ids),
        )
        still_referenced = {row[0] for row in cur.fetchall()}
        cur.execute(
            "select distinct explains_document_id from documents "
            "where explains_document_id = any(%s::uuid[]) and not (id = any(%s::uuid[]))",
            (doc_ids, doc_ids),
        )
        still_referenced |= {row[0] for row in cur.fetchall()}
    if still_referenced:
        doc_ids = [i for i in doc_ids if i not in still_referenced]
    if not doc_ids:
        return 0, 0
    with conn.cursor() as cur:
        cur.execute(
            "select count(*) from documents where id = any(%s::uuid[])",
            (doc_ids,),
        )
        doc_count = cur.fetchone()[0]
        cur.execute(
            "select count(*) from chunks where document_id = any(%s::uuid[])",
            (doc_ids,),
        )
        chunk_count = cur.fetchone()[0]

    if not really_delete:
        return doc_count, chunk_count

    with conn.cursor() as cur:
        cur.execute("delete from chunks where document_id = any(%s::uuid[])", (doc_ids,))
        cur.execute("delete from documents where id = any(%s::uuid[])", (doc_ids,))
    conn.commit()
    return doc_count, chunk_count


def main():
    log(f"Uklid starych zakony shardu - REZIM: {'DRY RUN (nic se nesmaze)' if not REALLY_DELETE else 'OSTRE MAZANI'}")
    if not REALLY_DELETE and CONFIRM_DELETE and DRY_RUN:
        log("   POZOR: CONFIRM_DELETE=yes je nastaveno, ale DRY_RUN neni 'false' - stale jen DRY RUN.")

    # 1) Posbirat plne zaembedovane document_id napric VSEMI konfigurovanymi
    #    novymi shardy (union - dokument je "bezpecny ke smazani ze stareho
    #    shardu", pokud je hotovy alespon v jednom novem shardu).
    fully_embedded_ids = set()
    new_stats = {}  # kopie v reshardech: id -> (chunku, zaembedovanych)
    for name, env_key in TARGET_SHARD_CANDIDATES:
        if not os.environ.get(env_key):
            log(f"   [{name}] preskoceno - env var {env_key} neni nastavena.")
            continue
        conn = db_connect(os.environ[env_key])
        try:
            ids = get_fully_embedded_doc_ids(conn)
            log(f"   [{name}] {len(ids)} plne zaembedovanych dokumentu.")
            fully_embedded_ids |= ids
            new_stats.update(get_doc_stats(conn))
        finally:
            conn.close()

    log(f"Celkem napric vsemi novymi shardy: {len(fully_embedded_ids)} unikatnich plne zaembedovanych dokumentu.")

    if not fully_embedded_ids:
        log("Zadne plne zaembedovane dokumenty zatim nikde nejsou - konci bez akce.")
        return

    # 2) Pro kazdy stary (zdrojovy) shard smazat prusecik s touto mnozinou.
    grand_docs = 0
    grand_chunks = 0
    for name, env_key in SOURCE_SHARDS:
        if not os.environ.get(env_key):
            log(f"   [{name}] preskoceno - env var {env_key} neni nastavena.")
            continue
        conn = db_connect(os.environ[env_key])
        try:
            with conn.cursor() as cur:
                allids = list(fully_embedded_ids | set(new_stats.keys()))
                cand = []
                for i in range(0, len(allids), 300):
                    cur.execute("select id from documents where id = any(%s::uuid[])", (allids[i:i + 300],))
                    cand += [row[0] for row in cur.fetchall()]
            # smazat, pokud je reshard kopie kompletni: stejny/vyssi pocet chunku A zadny embedding se neztrati
            old_stats = {}
            for i in range(0, len(cand), 300):
                old_stats.update(get_doc_stats(conn, cand[i:i + 300]))
            matched_ids = []
            for d in cand:
                o = old_stats.get(d, (10**9, 0))
                n = new_stats.get(d, (0, 0))
                if d in fully_embedded_ids or (n[0] >= o[0] and n[1] >= o[1]):
                    matched_ids.append(d)
            log(f"   [{name}] kandidatu s kopii v reshardech: {len(cand)}, bezpecne ke smazani: {len(matched_ids)}")
            doc_count, chunk_count = 0, 0
            for i in range(0, len(matched_ids), 2000):
                dc, cc = delete_documents(conn, matched_ids[i:i + 2000], REALLY_DELETE)
                doc_count += dc
                chunk_count += cc
            verb = "smazano" if REALLY_DELETE else "by se smazalo (dry run)"
            log(f"   [{name}] {verb}: {doc_count} dokumentu, {chunk_count} chunku.")
            grand_docs += doc_count
            grand_chunks += chunk_count
        finally:
            conn.close()

    verb = "Smazano" if REALLY_DELETE else "By se smazalo (DRY RUN - nic se nesmazalo)"
    log(f"{verb} celkem: {grand_docs} dokumentu, {grand_chunks} chunku napric vsemi starymi shardy.")


if __name__ == "__main__":
    for _attempt in range(8):
        try:
            main()
            break
        except psycopg2.OperationalError as e:
            log(f"Pokus {_attempt + 1}/8 selhal ({str(e)[:80]}) - opakuji za 20 s (mazani je idempotentni)")
            time.sleep(20)
    else:
        sys.exit(1)
