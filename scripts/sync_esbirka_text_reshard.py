"""
Sync e-Sbirka TEXT -> PRIMO do RESHARDU (Radek 2026-10-08). Nahrazuje
sync_esbirka_text_neon.py, ktery psal do 4 puvodnich rocnikovych shardu.

Rozdily oproti puvodnimu skriptu:
- cilem jsou reshardy (NEON_RESHARD_0N_DB_URL); novy predpis jde do prvniho
  reshardu s volnym mistem, zmeneny predpis se prepise v reshardu, kde uz lezi;
- obsah chunku se cisti od HTML (clean_html) a plni se search_tsv (cz_search_text),
  stejne jako to dela reshard_and_clean_zakony.py;
- historicka verze se archivuje jen kdyz je u stareho dokumentu znama
  version_iri (jinak by se u dokumentu bez version_iri zbytecne duplikoval obsah);
- stahovaci/parsovaci cast se importuje z sync_esbirka_text.py (beze zmeny).

Embedding se nepocita - chunky maji embedding NULL, doplni je embed_zakony_neon.py.
Embeddingy nezmenenych useku (stejny nadpis + text) se prenasi ze stare verze.
"""

import os
import re
import sys
import time
import uuid

import psycopg2
import psycopg2.extras

import sync_esbirka_text as fetcher
from reshard_and_clean_zakony import (
    TARGET_SHARDS,
    PRIORITY_PREDPISY,
    PRIORITY_EMBED_PRIORITY,
    SUPER_EMBED_PRIORITY,
    connect_target,
    ensure_target_conn,
    get_shard_size_bytes,
    effective_migration_budget_bytes,
    clean_html,
    cz_search_text,
)

SOURCE_ID = "5804ffaa-c5c6-4f35-b5c7-48da040ed457"

TIME_BUDGET_SECONDS = int(os.environ.get("TIME_BUDGET_SECONDS", "18000"))
START_TIME = time.time()


def log(*a):
    print(*a, flush=True)
    sys.stdout.flush()


def time_left():
    return TIME_BUDGET_SECONDS - (time.time() - START_TIME)


def parse_predpis(external_id):
    m = re.match(r"^(\d+)/(\d{4})", external_id)
    if not m:
        return None, None
    return int(m.group(1)), int(m.group(2))


class Shards:
    def __init__(self):
        self.env = {}
        self.conn = {}
        for name, env_key in TARGET_SHARDS:
            if not os.environ.get(env_key):
                continue
            self.conn[name] = connect_target(env_key, retries=8)
            self.env[name] = env_key
        log("Reshardy pripojeny: " + str(len(self.conn)))

    def live(self, name):
        self.conn[name] = ensure_target_conn(self.conn[name], self.env[name])
        return self.conn[name]

    def pick_active(self):
        for name in self.conn:
            conn = self.live(name)
            if get_shard_size_bytes(conn) < effective_migration_budget_bytes(conn):
                return name
        return None

    def get_existing_current(self):
        """external_id -> (id, version_iri, shard) pro is_current dokumenty e-Sbirky."""
        result = {}
        for name in self.conn:
            conn = self.live(name)
            with conn.cursor() as cur:
                cur.execute(
                    "select id, external_id, version_iri from documents "
                    "where source_id = %s and is_current = true",
                    (SOURCE_ID,),
                )
                for doc_id, external_id, version_iri in cur.fetchall():
                    result[external_id] = (str(doc_id), version_iri, name)
            conn.commit()
        return result

    def close(self):
        for c in self.conn.values():
            try:
                c.close()
            except Exception:
                pass


def fetch_doc_row(conn, doc_id):
    with conn.cursor() as cur:
        cur.execute(
            "select doc_type, title, issuer, url, version_iri from documents where id = %s",
            (doc_id,),
        )
        row = cur.fetchone()
    if not row:
        return None
    return {"doc_type": row[0], "title": row[1], "issuer": row[2], "url": row[3], "version_iri": row[4]}


def fetch_doc_chunks(conn, doc_id):
    with conn.cursor() as cur:
        cur.execute(
            "select heading, content, embedding, search_tsv::text from chunks "
            "where document_id = %s order by chunk_index",
            (doc_id,),
        )
        return cur.fetchall()


def upsert_law(conn, existing_entry, citace, meta, version_iri, doc_url):
    """existing_entry: None (novy predpis) nebo (doc_id, old_version_iri, shard).
    Vraci (document_id, reuse_map) - reuse_map {(heading, content): embedding}."""
    predpis_cislo, predpis_rok = parse_predpis(citace)
    reuse_map = {}

    if existing_entry is not None:
        document_id = existing_entry[0]
        old_doc = fetch_doc_row(conn, document_id)

        if old_doc is not None:
            old_chunks = fetch_doc_chunks(conn, document_id)
            for heading, content, embedding, _tsv in old_chunks:
                if embedding is not None:
                    reuse_map[(heading, content)] = embedding

            # Historickou verzi archivujeme jen kdyz zname jeji version_iri
            # (bez ni nevime, zda jde opravdu o jinou verzi nez ta nova).
            if old_chunks and old_doc.get("version_iri"):
                hist_id = str(uuid.uuid4())
                version_suffix = old_doc["version_iri"][-40:]
                archived_external_id = citace + "#hist-" + version_suffix
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        insert into documents (
                            id, source_id, external_id, doc_type, title,
                            issuer, url, status, version_iri, is_current,
                            valid_until, superseded_by, predpis_cislo,
                            predpis_rok
                        ) values (
                            %(id)s, %(source_id)s, %(external_id)s,
                            %(doc_type)s, %(title)s, %(issuer)s, %(url)s,
                            'historicky', %(version_iri)s, false,
                            current_date, %(superseded_by)s,
                            %(predpis_cislo)s, %(predpis_rok)s
                        )
                        on conflict (source_id, external_id) do nothing
                        """,
                        {
                            "id": hist_id,
                            "source_id": SOURCE_ID,
                            "external_id": archived_external_id,
                            "doc_type": old_doc["doc_type"],
                            "title": old_doc["title"],
                            "issuer": old_doc.get("issuer"),
                            "url": old_doc.get("url"),
                            "version_iri": old_doc.get("version_iri"),
                            "superseded_by": document_id,
                            "predpis_cislo": predpis_cislo,
                            "predpis_rok": predpis_rok,
                        },
                    )
                    hist_rows = [
                        (str(uuid.uuid4()), hist_id, idx, heading, content, embedding, tsv)
                        for idx, (heading, content, embedding, tsv) in enumerate(old_chunks)
                    ]
                    psycopg2.extras.execute_values(
                        cur,
                        "insert into chunks (id, document_id, chunk_index, heading, content, embedding, search_tsv) values %s",
                        hist_rows,
                        template="(%s,%s,%s,%s,%s,%s,%s::tsvector)",
                    )

            with conn.cursor() as cur:
                cur.execute("delete from chunks where document_id = %s", (document_id,))
                cur.execute(
                    """
                    update documents set
                        title = %(title)s, issuer = %(issuer)s, url = %(url)s,
                        status = 'platny', version_iri = %(version_iri)s,
                        is_current = true, valid_from = null, valid_until = null,
                        updated_at = now()
                    where id = %(id)s
                    """,
                    {
                        "id": document_id,
                        "title": meta["title"],
                        "issuer": "Sbírka zákonů",
                        "url": doc_url,
                        "version_iri": version_iri,
                    },
                )
        else:
            existing_entry = None

    if existing_entry is None:
        document_id = str(uuid.uuid4())
        key = (predpis_cislo, predpis_rok)
        embed_priority = SUPER_EMBED_PRIORITY if key in PRIORITY_PREDPISY else 0
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into documents (
                    id, source_id, external_id, doc_type, title, issuer,
                    url, status, version_iri, is_current, predpis_cislo,
                    predpis_rok, embed_priority, content_hash
                ) values (
                    %(id)s, %(source_id)s, %(external_id)s, %(doc_type)s,
                    %(title)s, %(issuer)s, %(url)s, 'platny', %(version_iri)s,
                    true, %(predpis_cislo)s, %(predpis_rok)s, %(embed_priority)s,
                    '__resharded_cleaned_v1__'
                )
                """,
                {
                    "id": document_id,
                    "source_id": SOURCE_ID,
                    "external_id": citace,
                    "doc_type": meta["doc_type"],
                    "title": meta["title"],
                    "issuer": "Sbírka zákonů",
                    "url": doc_url,
                    "version_iri": version_iri,
                    "predpis_cislo": predpis_cislo,
                    "predpis_rok": predpis_rok,
                    "embed_priority": embed_priority,
                },
            )

    return document_id, reuse_map


def main():
    log("=== Sync e-Sbirka TEXT -> PRIMO do reshardu: start ===")

    shards = Shards()

    valid_citace = fetcher._with_retry(fetcher.find_valid_citace, label="Nacteni metadat (006)")
    if not valid_citace:
        log("Nic k zpracovani, konec.")
        return
    acts = fetcher._with_retry(lambda: fetcher.find_acts(valid_citace), label="Nacteni katalogu aktu (002)")
    if not acts:
        log("Nic k zpracovani, konec.")
        return

    version_iri_by_citace = {}
    for citace, act in acts.items():
        vi = fetcher.current_version_iri(act)
        if vi:
            version_iri_by_citace[citace] = vi
        else:
            log("! " + citace + ": nenalezena aktualni verze, preskakuji")

    existing = shards.get_existing_current()
    unchanged = 0
    for citace in list(version_iri_by_citace.keys()):
        prev = existing.get(citace)
        if prev is not None and prev[1] == version_iri_by_citace[citace]:
            del version_iri_by_citace[citace]
            unchanged += 1
    log("Beze zmeny od minuleho behu (preskakuji): " + str(unchanged) + ", ke zpracovani: " + str(len(version_iri_by_citace)))
    if not version_iri_by_citace:
        log("Nic se od minuleho behu nezmenilo, konec.")
        shards.close()
        return

    version_iris = list(version_iri_by_citace.values())
    log("-> jeden prochod 003 pro " + str(len(version_iris)) + " verzi soucasne")
    section_nodes_by_version, all_fragments_by_version = fetcher._with_retry(
        lambda: fetcher.scan_version_fragments(version_iris), label="Skenovani fragmentu verzi (003)"
    )

    for v in section_nodes_by_version:
        total = len(section_nodes_by_version[v])
        if total > fetcher.MAX_SECTIONS_PER_ACT:
            log(" ! verze " + v + ": " + str(total) + " paragrafu presahuje pojistku, oriznuto")
            section_nodes_by_version[v] = section_nodes_by_version[v][: fetcher.MAX_SECTIONS_PER_ACT]

    by_section_by_version = {}
    all_needed_fragment_ids = set()
    for v in version_iris:
        by_section = fetcher.group_descendants(section_nodes_by_version[v], all_fragments_by_version[v])
        by_section_by_version[v] = by_section
        for ids in by_section.values():
            all_needed_fragment_ids.update(ids)

    texts_by_id = fetcher._with_retry(
        lambda: fetcher.fetch_fragment_texts(all_needed_fragment_ids), label="Nacteni textu fragmentu (004)"
    )

    done = 0
    updated = 0
    failed = 0
    per_shard = {}

    for citace, version_iri in version_iri_by_citace.items():
        if time_left() <= 120:
            log("Casovy rozpocet vycerpan, koncim (dalsi beh bude pokracovat od zbyvajicich zmen).")
            break

        act = acts[citace]
        meta = valid_citace[citace]
        doc_url = (
            "https://e-sbirka.cz" + version_iri.split("esel-esb:eli/cz")[-1]
            if "eli/cz" in version_iri
            else None
        )

        prev = existing.get(citace)
        if prev is not None:
            target_shard = prev[2]
        else:
            target_shard = shards.pick_active()
            if target_shard is None:
                log("STOP: vsechny reshardy jsou plne - je treba zalozit dalsi reshard.")
                break

        conn = shards.live(target_shard)

        try:
            document_id, reuse_map = upsert_law(conn, prev, citace, meta, version_iri, doc_url)
            if prev is not None:
                updated += 1

            section_nodes = section_nodes_by_version[version_iri]
            by_section = by_section_by_version[version_iri]

            chunk_rows = []
            for idx, node in enumerate(section_nodes):
                frag_ids = by_section.get(node["iri"], [])
                parts = [texts_by_id[fid] for fid in frag_ids if fid in texts_by_id]
                content = clean_html(" ".join(parts).strip())
                if not content:
                    continue
                chunk_rows.append(
                    (
                        str(uuid.uuid4()),
                        document_id,
                        idx,
                        node["citace"],
                        content,
                        reuse_map.get((node["citace"], content)),
                        cz_search_text(content),
                    )
                )

            if chunk_rows:
                with conn.cursor() as cur:
                    psycopg2.extras.execute_values(
                        cur,
                        "insert into chunks (id, document_id, chunk_index, heading, content, embedding, search_tsv) values %s",
                        chunk_rows,
                        template="(%s,%s,%s,%s,%s,%s,to_tsvector('simple', %s))",
                    )
            conn.commit()

            done += 1
            per_shard[target_shard] = per_shard.get(target_shard, 0) + 1
            if done % 100 == 0:
                log(" ...zpracovano " + str(done) + "/" + str(len(version_iri_by_citace)) + " predpisu")

        except Exception as e:
            failed += 1
            try:
                conn.rollback()
            except Exception:
                pass
            log("CHYBA u " + citace + ": " + str(e))
            continue

    log(
        "=== Hotovo, zpracovano " + str(done) + " predpisu (z toho aktualizace existujicich: " + str(updated)
        + "), chyb " + str(failed) + ", po reshardech: "
        + ", ".join(k + "=" + str(v) for k, v in per_shard.items()) + " ==="
    )
    shards.close()


if __name__ == "__main__":
    main()
