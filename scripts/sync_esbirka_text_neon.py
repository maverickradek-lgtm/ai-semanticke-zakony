"""
FAZE A (PRIMY zapis do Neonu): hromadny import SYROVEHO TEXTU vsech
aktualne platnych ceskych pravnich predpisu (zakony, ustavni zakony,
vyhlasky, narizeni vlady, opatreni, historicke dekrety prezidenta
republiky) z otevrenych dat e-Sbirky PRIMO do prislusneho Neon shardu
(podle roku predpisu) - narozdil od puvodniho scripts/sync_esbirka_text.py,
ktery pise do hlavni Supabase a data se pak teprve dodatecne kopiruji do
Neonu pres migrate_zakony_to_neon.py + verify_and_cleanup_zakony_supabase.py.
Tento skript ten mezikrok obchazi - nova/zmenena data uz vubec neprochazi
pres Supabase.

DULEZITE - cteci/parsovaci cast (stahovani a rozparsovani e-Sbirka gzip
dumpu - 006/002/003/004) se SCHVALNE NEKOPIRUJE, ale importuje se PRIMO ze
scripts/sync_esbirka_text.py (find_valid_citace, find_acts,
current_version_iri, scan_version_fragments, group_descendants,
fetch_fragment_texts). Byl to zamerny navrh - rucni prepis by riskoval
poskozeni Ceskych nazvu poli z e-Sbirkv JSON schematu (diakritika v
retezcich jako "položky.item" nebo "cis-esb-typ-právní-akt-položka"
se pri kopirovani pres nekolik encode/decode kroku snadno rozbije a
ijson by pak tise nenasel nic, aniz by to skript ohlasil jako chybu).
Diky primemu importu je tahle cast 1:1 stejna jako v jiz overenem a
bezici puvodnim skriptu.

Zapisovaci cast pouziva stejne sdilene funkce jako
scripts/migrate_zakony_to_neon.py (ensure_schema, bucket_for_year,
ensure_conn, NEON_URLS), aby zustalo garantovane, ze stejny predpis vzdy
skonci ve stejnem shardu bez ohledu na to, ktery skript ho zrovna zapisuje.

DULEZITE: tato faze NEPOCITA embeddingy - nove/zmenene chunky se ukladaji
s embedding = NULL. Doplni je JIZ EXISTUJICI scripts/embed_zakony_neon.py
(bezi round-robin pres vsechny 4 shardy a hleda pending chunky genericky
pres get_pending_chunks_prioritized() - zadna zmena tam neni potreba).

Import modulu sync_esbirka_text.py a migrate_zakony_to_neon.py na urovni
souboru vyzaduje, aby byly nastavene SUPABASE_URL/SUPABASE_SERVICE_ROLE_KEY
(i kdyz tento skript uz samotnou Supabase pro zapis vubec nepouziva) -
oba secrety je proto potreba predavat i tomuto workflow, jinak import
selze na chybejici env. promenne.

Zdroj: https://opendata.eselpoint.gov.cz/datove-sady-esbirka/
Zadna registrace/API klic neni potreba - jde o volne dostupna otevrena data.
"""

import os
import re
import sys
import time
import uuid

import psycopg2
import psycopg2.extras

import migrate_zakony_to_neon as neonlib
import sync_esbirka_text as fetcher

# Stejne id jako radek v hlavni Supabase tabulce "sources" (code='esbirka',
# viz `select id from sources where code='esbirka'`). Neon nema vlastni
# "sources" tabulku - source_id se jen prenasi jako stabilni konstanta,
# aby zustala kompatibilita s radky jiz drive migrovanymi ze Supabase
# (unique(source_id, external_id)).
SOURCE_ID = "5804ffaa-c5c6-4f35-b5c7-48da040ed457"

TIME_BUDGET_SECONDS = int(os.environ.get("TIME_BUDGET_SECONDS", "18000"))
START_TIME = time.time()


def log(*a):
    print(*a, flush=True)
    sys.stdout.flush()


def time_left():
    return TIME_BUDGET_SECONDS - (time.time() - START_TIME)


def parse_predpis(external_id):
    """Replika GENERATED sloupcu documents.predpis_cislo/predpis_rok z
    hlavni Supabase (viz information_schema.columns - regex
    '^\\d+/\\d{4}' na citaci) - Neon ma tyto sloupce jako obycejne (ne
    generated) kvuli migrate_zakony_to_neon.py, proto se tu pocitaji
    rucne stejnym vzorcem, aby vysledek byl bit-identicky."""
    m = re.match(r"^(\d+)/(\d{4})", external_id)
    if not m:
        return None, None
    return int(m.group(1)), int(m.group(2))


def get_existing_current(neon_conns):
    """external_id -> (id, version_iri, shard_key) pro VSECHNY
    is_current=true dokumenty tohoto zdroje napric vsemi 4 shardy -
    obdoba get_existing_version_iris() z puvodniho sync_esbirka_text.py,
    jen se navic pamatuje i to, ve kterem shardu dany zaznam uz je."""
    result = {}
    for key, conn in neon_conns.items():
        with conn.cursor() as cur:
            cur.execute(
                "select id, external_id, version_iri from documents "
                "where source_id = %s and is_current = true",
                (SOURCE_ID,),
            )
            for doc_id, external_id, version_iri in cur.fetchall():
                result[external_id] = (str(doc_id), version_iri, key)
    return result


def fetch_doc_row(conn, doc_id):
    with conn.cursor() as cur:
        cur.execute(
            "select doc_type, title, issuer, url, version_iri "
            "from documents where id = %s",
            (doc_id,),
        )
        row = cur.fetchone()
    if not row:
        return None
    return {
        "doc_type": row[0],
        "title": row[1],
        "issuer": row[2],
        "url": row[3],
        "version_iri": row[4],
    }


def fetch_doc_chunks(conn, doc_id):
    with conn.cursor() as cur:
        cur.execute(
            "select heading, content, embedding "
            "from chunks where document_id = %s order by chunk_index",
            (doc_id,),
        )
        return cur.fetchall()


def upsert_law(conn, existing_entry, citace, meta, version_iri, doc_url):
    """existing_entry: None (novy predpis) nebo (doc_id, old_version_iri,
    shard_key) z get_existing_current(). Vraci (document_id, reuse_map),
    kde reuse_map je {(heading, content): embedding} pro useky, jejichz
    text se mezi starou a novou verzi nezmenil (embedding se tak nemusi
    pocitat znovu)."""
    predpis_cislo, predpis_rok = parse_predpis(citace)
    reuse_map = {}

    if existing_entry is not None:
        document_id, _old_version_iri, _shard_key = existing_entry
        old_doc = fetch_doc_row(conn, document_id)

        if old_doc is not None:
            old_chunks = fetch_doc_chunks(conn, document_id)
            if old_chunks:
                hist_id = str(uuid.uuid4())
                version_suffix = (old_doc.get("version_iri") or "")[-40:]
                archived_external_id = f"{citace}#hist-{version_suffix}"

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

                    if cur.rowcount == 0:
                        # archivni zaznam se stejnym external_id uz existoval (napr.
                        # zbytek z puvodni migrace ze Supabase) - ON CONFLICT DO
                        # NOTHING nic nevlozil, takze nove vygenerovany hist_id
                        # neexistuje v documents. Dohledej skutecne id existujiciho
                        # radku, jinak by insert chunku nize spadl na FK constraint
                        # (presne tohle zpusobilo CHYBA u 86/2011 Sb. apod. v run #2).
                        cur.execute(
                            "select id from documents where source_id = %s and external_id = %s",
                            (SOURCE_ID, archived_external_id),
                        )
                        existing_hist_row = cur.fetchone()
                        if existing_hist_row:
                            hist_id = str(existing_hist_row[0])
                            # smaz pripadne stare chunky pod timhle hist_id, aby se
                            # pri opakovanem konfliktu nehromadily duplicity
                            cur.execute("delete from chunks where document_id = %s", (hist_id,))

                    hist_rows = []
                    for idx, (heading, content, embedding) in enumerate(old_chunks):
                        hist_rows.append(
                            (str(uuid.uuid4()), hist_id, idx, heading, content, embedding)
                        )
                        if embedding is not None:
                            reuse_map[(heading, content)] = embedding

                    if hist_rows:
                        psycopg2.extras.execute_values(
                            cur,
                            "insert into chunks (id, document_id, chunk_index, heading, content, embedding) values %s",
                            hist_rows,
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
            # Zaznam v existing map byl, ale radek uz nejde dohledat
            # (nemelo by nastat) - pojìstka: zapiseme jako novy.
            existing_entry = None

    if existing_entry is None:
        document_id = str(uuid.uuid4())
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into documents (
                    id, source_id, external_id, doc_type, title, issuer,
                    url, status, version_iri, is_current, predpis_cislo,
                    predpis_rok
                ) values (
                    %(id)s, %(source_id)s, %(external_id)s, %(doc_type)s,
                    %(title)s, %(issuer)s, %(url)s, 'platny', %(version_iri)s,
                    true, %(predpis_cislo)s, %(predpis_rok)s
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
                },
            )

    return document_id, reuse_map


def main():
    log("=== Sync e-Sbirka TEXT -> PRIMO do Neonu: start ===")

    neon_conns = {}
    for key in neonlib.NEON_URLS:
        conn = psycopg2.connect(neonlib.NEON_URLS[key], connect_timeout=15)
        neonlib.ensure_schema(conn)
        neon_conns[key] = conn
        log(f"Schema pripraveno v Neon shardu: {key}")

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
            log(f"! {citace}: nenalezena aktualni verze, preskakuji")

    existing = get_existing_current(neon_conns)
    unchanged = 0
    for citace in list(version_iri_by_citace.keys()):
        prev = existing.get(citace)
        if prev is not None and prev[1] == version_iri_by_citace[citace]:
            del version_iri_by_citace[citace]
            unchanged += 1
    log(f"Beze zmeny od minuleho behu (preskakuji): {unchanged}, ke zpracovani: {len(version_iri_by_citace)}")
    if not version_iri_by_citace:
        log("Nic se od minuleho behu nezmenilo, konec.")
        for conn in neon_conns.values():
            conn.close()
        return

    version_iris = list(version_iri_by_citace.values())
    log(f"-> jeden prochod 003 pro {len(version_iris)} verzi soucasne")
    section_nodes_by_version, all_fragments_by_version = fetcher._with_retry(
        lambda: fetcher.scan_version_fragments(version_iris), label="Skenovani fragmentu verzi (003)"
    )

    for v in section_nodes_by_version:
        total = len(section_nodes_by_version[v])
        if total > fetcher.MAX_SECTIONS_PER_ACT:
            log(f" ! verze {v}: {total} paragrafu presahuje pojistku, oriznuto")
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
    per_shard = {k: 0 for k in neonlib.NEON_URLS}

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
        predpis_cislo, predpis_rok = parse_predpis(citace)
        target_shard = neonlib.bucket_for_year(predpis_rok)
        if prev is not None and prev[2] != target_shard:
            # Cislo/rok predpisu (citace) se v case nemeni - pokud uz
            # existujici zaznam sedi v jinem shardu, nez by ted vysel z
            # roku, jde o nesrovnalost (nemelo by nastat). Radeji zustat
            # v puvodnim shardu, at nevznikne duplicitni zaznam ve dvou
            # shardech soucasne.
            log(f"! POZOR {citace}: existujici zaznam je v shardu {prev[2]}, rok {predpis_rok} by vysel na {target_shard} - ponechavam v {prev[2]}")
            target_shard = prev[2]

        if prev is None and predpis_rok is not None and 1945 <= predpis_rok <= 1950:
            # Radek (2026-09-02): agregovane poválečné vyhlášky/oznámení z let
            # 1945-1950 (hlavně série "Úřední list", ne "Sbírka zákonů") byly po
            # pečlivé rucni/AI kontrole zamerne vyrazeny z databaze jako
            # obsoletni balast (viz projektova pamet
            # aggregate_vyhlasky_1945_1950_investigation) - nove zaznamy z
            # tohoto obdobi se uz znovu nepridavaji. Jiz existujici zaznamy
            # (prev is not None) se ale i nadale normalne aktualizuji, pokud
            # se jejich verze zmeni.
            continue

        conn = neonlib.ensure_conn(neon_conns, target_shard)

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
                content = " ".join(parts).strip()
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
                    )
                )

            if chunk_rows:
                with conn.cursor() as cur:
                    psycopg2.extras.execute_values(
                        cur,
                        "insert into chunks (id, document_id, chunk_index, heading, content, embedding) values %s",
                        chunk_rows,
                    )
            conn.commit()

            done += 1
            per_shard[target_shard] += 1
            if done % 100 == 0:
                log(f" ...zpracovano {done}/{len(version_iri_by_citace)} predpisu")

        except Exception as e:
            conn.rollback()
            log(f"CHYBA u {citace}: {e}")
            continue

    log(
        f"=== Hotovo, zpracovano {done} predpisu (z toho aktualizace existujicich: {updated}) "
        f"po shardech: " + ", ".join(f"{k}={v}" for k, v in per_shard.items()) + " ==="
    )

    for conn in neon_conns.values():
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
