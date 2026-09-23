"""
Jednorazovy backfill: doplni chunky pro jiz existujici "historicke" zaznamy
zakonu (is_current=false, status='historicky'), ktere maji 0 chunku kvuli
stejne priciny jako F-01/Cl.-bug u aktualnich zneni (viz sync_esbirka_text.py,
Radek 2026-09-22 - regex pro fragment-citace puvodne poznaval jen "SS N"
paragrafy, ne novelizacni "Cl. N" clanky).

U aktualnich zneni se to opravilo samo dnesnim planovanym behem
sync_esbirka_text_neon.py (po resetu version_iri=null u postizenych
zaznamu). Historicke zaznamy uz ale byly archivovany v minulosti - jejich
version_iri se od te doby nezmeni, takze normalni denni sync uz o ne
nezavadi. Tento skript pro kazdy takovy zaznam pouzije jeho VLASTNI
version_iri (sloupec documents.version_iri, ulozeny pri archivaci) a
znovu stahne/naparsuje jen 003+004 (ne 006/002 - "aktualni" verzi tu
nehledame, uz ji zname).

Bezi na GitHub-hosted runneru (ubuntu-latest) - NAS self-hosted runner ma
nespolehlive pripojeni k opendata.eselpoint.gov.cz pro takhle velke soubory.
"""

import os
import sys
import time
import uuid

import psycopg2
import psycopg2.extras

sys.path.insert(0, os.path.dirname(__file__))
import sync_esbirka_text as fetcher

NEON_URLS = {
    "do1997": os.environ["NEON_ZAKONY_DO1997_DB_URL"],
    "1998_2007": os.environ["NEON_ZAKONY_1998_2007_DB_URL"],
    "2008_2020": os.environ["NEON_ZAKONY_2008_2020_DB_URL"],
    "2021_dosud": os.environ["NEON_ZAKONY_2021_DOSUD_DB_URL"],
}

MAX_SECTIONS_PER_ACT = int(os.environ.get("MAX_SECTIONS_PER_ACT", "3000"))


def log(*a):
    print(*a, flush=True)


def db_connect(url, timeout=15):
    last_err = None
    for attempt in range(4):
        try:
            return psycopg2.connect(url, connect_timeout=timeout)
        except Exception as e:
            last_err = e
            log(f"db_connect selhalo (pokus {attempt + 1}/4): {e}")
            time.sleep(3)
    raise last_err


def get_empty_historical(conn):
    """id -> version_iri pro historicke zaznamy s 0 chunky a znamym version_iri."""
    with conn.cursor() as cur:
        cur.execute(
            """
            select d.id, d.version_iri
            from documents d
            where d.is_current = false and d.status = 'historicky'
              and d.version_iri is not null
              and not exists (select 1 from chunks c where c.document_id = d.id)
            """
        )
        return {str(doc_id): version_iri for doc_id, version_iri in cur.fetchall()}


def backfill_shard(shard_key, url):
    # Radek 2026-09-23: pripojeni k Neonu se NEOTVIRA hned na zacatku a
    # nedrzi se cely cas - stahovani+skenovani 003/004 muze trvat i pres
    # 7 minut a drzena nevyuzita DB session mezitim spadne na "SSL
    # connection has been closed unexpectedly" (presne tohle se stalo v
    # prvnim behu). Kratke pripojeni se otevre zvlast na precteni seznamu
    # a zvlast az tesne pred zapisem (a znovu-otevre pri vypadku behem
    # zapisu).
    conn = db_connect(url)
    try:
        empty = get_empty_historical(conn)
    finally:
        conn.close()

    log(f"[{shard_key}] {len(empty)} historickych zaznamu bez chunku")
    if not empty:
        return 0, 0

    version_iris = sorted(set(empty.values()))
    log(f"[{shard_key}] -> scan_version_fragments pro {len(version_iris)} unikatnich verzi")
    section_nodes_by_version, all_fragments_by_version = fetcher._with_retry(
        lambda: fetcher.scan_version_fragments(version_iris),
        label=f"[{shard_key}] Skenovani fragmentu (003)",
    )

    for v in section_nodes_by_version:
        total = len(section_nodes_by_version[v])
        if total > MAX_SECTIONS_PER_ACT:
            log(f"  ! verze {v}: {total} useku presahuje pojistku, oriznuto")
            section_nodes_by_version[v] = section_nodes_by_version[v][:MAX_SECTIONS_PER_ACT]

    by_section_by_version = {}
    all_needed_fragment_ids = set()
    for v in version_iris:
        by_section = fetcher.group_descendants(section_nodes_by_version[v], all_fragments_by_version[v])
        by_section_by_version[v] = by_section
        for ids in by_section.values():
            all_needed_fragment_ids.update(ids)

    texts_by_id = fetcher._with_retry(
        lambda: fetcher.fetch_fragment_texts(all_needed_fragment_ids),
        label=f"[{shard_key}] Nacteni textu fragmentu (004)",
    )

    # Az TED, tesne pred zapisem, se otevre cerstve pripojeni pro zapis.
    conn = db_connect(url)
    try:
        filled = 0
        still_empty = 0
        processed = 0
        for doc_id, version_iri in empty.items():
            processed += 1
            section_nodes = section_nodes_by_version.get(version_iri, [])
            by_section = by_section_by_version.get(version_iri, {})
            chunk_rows = []
            for idx, node in enumerate(section_nodes):
                frag_ids = by_section.get(node["iri"], [])
                parts = [texts_by_id[fid] for fid in frag_ids if fid in texts_by_id]
                content = " ".join(parts).strip()
                if not content:
                    continue
                chunk_rows.append((str(uuid.uuid4()), doc_id, idx, node["citace"], content, None))

            if not chunk_rows:
                still_empty += 1
                continue

            for attempt in range(3):
                try:
                    with conn.cursor() as cur:
                        psycopg2.extras.execute_values(
                            cur,
                            "insert into chunks (id, document_id, chunk_index, heading, content, embedding) values %s",
                            chunk_rows,
                        )
                    conn.commit()
                    break
                except psycopg2.OperationalError as e:
                    log(f"[{shard_key}] DB zapis selhal (pokus {attempt + 1}/3), obnovuji spojeni: {e}")
                    try:
                        conn.close()
                    except Exception:
                        pass
                    time.sleep(3)
                    conn = db_connect(url)
            else:
                log(f"[{shard_key}] VZDAVAM se zapisu chunku pro dokument {doc_id} po 3 pokusech")
                still_empty += 1
                continue

            filled += 1
            if processed % 200 == 0:
                log(f"[{shard_key}] ...zpracovano {processed}/{len(empty)} (doplneno {filled})")

        log(f"[{shard_key}] Hotovo: doplneno {filled}, stale bez textu {still_empty}")
        return filled, still_empty
    finally:
        conn.close()


def main():
    log("=== Backfill historickych zneni bez textu: start ===")
    total_filled = 0
    total_still_empty = 0
    for shard_key, url in NEON_URLS.items():
        filled, still_empty = backfill_shard(shard_key, url)
        total_filled += filled
        total_still_empty += still_empty
    log(f"=== Hotovo. Doplneno celkem {total_filled}, stale bez textu {total_still_empty} ===")


if __name__ == "__main__":
    main()
