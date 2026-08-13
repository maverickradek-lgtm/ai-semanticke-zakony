"""
Overuje, ktere jiz do Neonu migrovane zakony (documents.content_hash =
'__migrated_to_neon__' v hlavni Supabase DB) maji v cilovem Neon shardu
KOMPLETNI embedding (has_pending_chunks = false) a presne sedici pocet
chunku, a takove bezpecne maze ze zdrojove Supabase DB, aby se uvolnilo
misto - viz pamet storage_scaling_plan a zakony_neon_sharding_plan.

Bezpecnostni zasady (zamerne konzervativni, protoze mazani je nevratne):
- NIC se nesmaze, dokud v prislusnem Neon shardu dany dokument nema
  has_pending_chunks = false (tj. VSECHNY jeho chunky maji embedding).
- NIC se nesmaze, pokud pocet chunku v Neonu neodpovida presne poctu
  chunku aktualne v Supabase (ochrana proti necekanym rozdilum, napr.
  soubeznemu behu neceho jineho).
- Pokud cokoliv selze neocekavane (chyba pripojeni k shardu apod.), dany
  shard/dokument se proste PRESKOCI (nesmaze se), ne "smaze se at to stoji
  za to".
- Mazani je omezene na MAX_DELETE_PER_RUN dokumentu za jeden beh, aby
  pripadny nevidenym problem zpusobil jen omezenou, rychle zjistitelnou
  skodu.
- Dokument, na ktery jeste odkazuje jiny dokument (documents.
  explains_document_id, napr. duvodova zprava, nebo documents.
  superseded_by), se NIKDY nemaze - smazani by porusilo FK constraint a
  (protoze DELETE ... WHERE id IN (...) je atomicke) shodilo by mazani
  CELE davky, ve ktere se takovy dokument nahodou ocitl (viz incident
  2026-08-13).

Toto NENI zivy funkcni test aplikace (nevola se ai-query edge funkce) -
"dostupnost v aplikaci" je zde definovana jako "kompletni a integritne
overena data v Neonu", protoze napojeni Neon shardu do ai-query bylo jiz
drive tento projekt zivotne overeno (viz zakony_neon_embed_and_search_wiring).
"""

import os
import sys
import psycopg2
import requests

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SERVICE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
MAX_DELETE_PER_RUN = int(os.environ.get("MAX_DELETE_PER_RUN", "500"))

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
    """Vsechny zakony A duvodove zpravy v Supabase, ktere migracni skript uz
    oznacil jako prekopirovane do Neonu (content_hash marker).

    Duvodove zpravy se sem zapocitavaji od 2026-08-13 spolu se zakony -
    migrate_zakony_to_neon.py je ted take migruje (do stejneho shardu jako
    zakon, ktery vysvetluji), prave aby se casem uvolnil i jejich FK odkaz
    (explains_document_id) na svuj zakon a ten zakon se pak mohl bezpecne
    smazat - viz get_referenced_document_ids() a pamet
    explains_document_id_fk_bug_2026-08-13.

    Musi se strankovat (offset/limit v cyklu) - jeden pozadavek s
    limit=10000 se ticha orizne na Supabase/PostgREST vychozi max-rows
    (obvykle 1000), takze bez strankovani by skript videl jen nahodnou
    prvni tisicovku migrovanych dokumentu a zbytek (klidne tisice
    skutecne bezpecnych kandidatu) by proste nikdy neposoudil - ne ze by
    je zamitl, jen by o nich nevedel."""
    all_rows = {}
    page = 1000
    offset = 0
    while True:
        rows = sb_get(
            "documents",
            {
                "select": "id,external_id,title,predpis_rok",
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
    """Aktualni pocet chunku v Supabase pro dane dokumenty (pro integritni
    porovnani s Neonem tesne pred smazanim).

    Stejny duvod jako u get_migrated_candidates(): jeden pozadavek se
    "vysokym limitem" se muze ticha orizout na Supabase/PostgREST vychozi
    max-rows. Zde je to o to zakeznejsi, ze by to VZDY vypadalo bezpecne
    (nizsi napocitany pocet chunku => dokument se jen presko ci jako
    "nesedi pocet", nikdy se omylem nesmaze) - ale zbytecne by to
    blokovalo skutecne bezpecne kandidaty. Proto se i tady stranku uje
    pres offset v cyklu, dokud dana davka doku mentu neni cela nactena."""
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


def get_referenced_document_ids():
    """Id dokumentu, na ktere jeste nekdo jiny odkazuje pres
    documents.explains_document_id (napr. duvodova zprava odkazujici na
    svuj zakon) nebo documents.superseded_by (retezec nahrazenych verzi).

    Tyto NESMIME smazat, dokud odkaz existuje - jinak FK constraint
    (documents_explains_document_id_fkey / documents_superseded_by_fkey)
    porusi cele davkove mazani, a to tak, ze to NEPOMUZE jen preskocit ten
    jeden spatny radek: jedno DELETE ... WHERE id IN (...) je v Postgresu
    atomicke, takze jeden odkazovany dokument shodi mazani VSECH dokumentu
    v cele davce (viz incident 2026-08-13: davka 28 dokumentu prisla o
    chunky, ale mazani samotnych zaznamu dokumentu selhalo kvuli 1
    odkazovanemu zakonu a vsech 28 zustalo v napulmazanem stavu)."""
    referenced = set()
    for col in ("explains_document_id", "superseded_by"):
        page = 1000
        offset = 0
        while True:
            rows = sb_get(
                "documents",
                {
                    "select": col,
                    col: "not.is.null",
                    "order": "id.asc",
                    "limit": str(page),
                    "offset": str(offset),
                },
            )
            if not rows:
                break
            for r in rows:
                v = r.get(col)
                if v:
                    referenced.add(v)
            offset += page
            if len(rows) < page:
                break
    return referenced


def get_neon_fully_embedded(shard_key):
    """Pro dany shard vrati {doc_id: pocet_chunku} pro dokumenty, ktere maji
    has_pending_chunks = false (vsechny chunky zembedovane) a alespon 1
    chunk (prazdny/rozbity dokument se nepocita jako hotovy)."""
    conn = psycopg2.connect(NEON_URLS[shard_key], connect_timeout=15)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                select d.id, count(c.id)
                from documents d
                join chunks c on c.document_id = d.id
                where d.has_pending_chunks = false
                group by d.id
                """
            )
            rows = cur.fetchall()
        return {str(doc_id): cnt for doc_id, cnt in rows}
    finally:
        conn.close()


def delete_batch(doc_ids):
    if not doc_ids:
        return
    ids_expr = f"in.({','.join(doc_ids)})"
    r1 = SESSION.delete(
        f"{SUPABASE_URL}/rest/v1/chunks",
        headers={**sb_headers(), "Prefer": "return=minimal"},
        params={"document_id": ids_expr},
        timeout=60,
    )
    if r1.status_code >= 300:
        raise RuntimeError(f"Mazani chunku selhalo: {r1.status_code} {r1.text[:300]}")
    r2 = SESSION.delete(
        f"{SUPABASE_URL}/rest/v1/documents",
        headers={**sb_headers(), "Prefer": "return=minimal"},
        params={"id": ids_expr},
        timeout=60,
    )
    if r2.status_code >= 300:
        raise RuntimeError(f"Mazani dokumentu selhalo: {r2.status_code} {r2.text[:300]}")


def main():
    log("Overuji migrovane+zembedovane zakony a pripravuji bezpecny uklid Supabase...")

    candidates = get_migrated_candidates()
    log(f"Kandidatu (oznaceno jako migrovano v Supabase): {len(candidates)}")
    if not candidates:
        log("Zadni kandidati, konec.")
        return

    referenced_ids = get_referenced_document_ids()
    log(f"Dokumentu odkazovanych odjinud (explains_document_id/superseded_by), tyto se nikdy nemazou: {len(referenced_ids)}")

    neon_embedded = {}
    for shard_key in NEON_URLS:
        try:
            shard_map = get_neon_fully_embedded(shard_key)
            log(f"  [{shard_key}] plne zembedovanych dokumentu v Neonu: {len(shard_map)}")
            neon_embedded.update(shard_map)
        except Exception as e:
            log(f"  [{shard_key}] CHYBA pri cteni z Neonu, tento shard preskakuji (nic z nej se nesmaze): {e}")

    fully_ready_ids = [doc_id for doc_id in candidates if doc_id in neon_embedded]
    log(f"Kandidatu s kompletnim embeddingem v prislusnem Neon shardu: {len(fully_ready_ids)}")
    if not fully_ready_ids:
        log("Zadny dokument zatim neni pripraven ke smazani, konec.")
        return

    skipped_referenced = [d for d in fully_ready_ids if d in referenced_ids]
    fully_ready_ids = [d for d in fully_ready_ids if d not in referenced_ids]
    if skipped_referenced:
        log(f"PRESKAKUJI {len(skipped_referenced)} dokumentu, protoze na ne jeste nekdo odkazuje (napr. duvodova zprava) - nelze smazat bez poruseni vazby")
    if not fully_ready_ids:
        log("Po vyrazeni odkazovanych dokumentu nezbyl zadny kandidat, konec.")
        return

    sb_chunk_counts = get_supabase_chunk_counts(fully_ready_ids)

    safe_to_delete = []
    for doc_id in fully_ready_ids:
        neon_count = neon_embedded[doc_id]
        sb_count = sb_chunk_counts.get(doc_id, 0)
        title = candidates[doc_id].get("title", "")[:60]
        if sb_count == 0:
            # V teto fazi uz vime, ze dokument ma v Neonu >=1 kompletne
            # zembedovany chunk (jinak by nebyl v neon_embedded/
            # fully_ready_ids - get_neon_fully_embedded pouziva INNER JOIN
            # na chunks). 0 chunku v Supabase tedy uz NEMUZE znamenat
            # "nejasny, historicky prazdny dokument" - muze uz jen
            # znamenat, ze predchozi beh chunky uz smazal, ale smazani
            # samotneho zaznamu dokumentu tehdy selhalo (viz incident
            # 2026-08-13). Bezpecne tedy jen DOKONCIME drive preruseny
            # uklid - chunky uz beztak nejsou co mazat.
            log(f"  DOKONCUJI drive preruseny uklid {doc_id} ({title}): chunky uz v Supabase nejsou, mazu jen zbyvajici zaznam dokumentu")
            safe_to_delete.append(doc_id)
            continue
        if neon_count != sb_count:
            log(f"  PRESKAKUJI {doc_id} ({title}): pocet chunku nesedi (Neon={neon_count}, Supabase={sb_count})")
            continue
        safe_to_delete.append(doc_id)

    log(f"Integritne overenych (pocet chunku presne sedi) kandidatu: {len(safe_to_delete)}")
    if not safe_to_delete:
        log("Po integritni kontrole nezbyl zadny bezpecny kandidat, konec bez mazani.")
        return

    to_delete = safe_to_delete[:MAX_DELETE_PER_RUN]
    log(f"Mazu {len(to_delete)} dokumentu ze Supabase (z {len(safe_to_delete)} bezpecnych kandidatu, limit {MAX_DELETE_PER_RUN}/beh)...")

    page = 50
    deleted = 0
    for i in range(0, len(to_delete), page):
        batch = to_delete[i : i + page]
        try:
            delete_batch(batch)
            deleted += len(batch)
        except Exception as e:
            # Jedno DELETE ... WHERE id IN (...) je atomicke - kdyby v
            # davce byl i pres predchozi filtry nejaky necekany konflikt
            # (napr. novy odkaz vznikly mezitim), nechceme kvuli 1 spatnemu
            # dokumentu preskocit cely zbytek davky. Zkusime tedy davku po
            # jednom dokumentu, aby se problem omezil jen na ten konkretni
            # zaznam (viz incident 2026-08-13).
            log(f"  CHYBA pri mazani davky ({len(batch)} dok.), zkousim po jednom: {e}")
            for doc_id in batch:
                try:
                    delete_batch([doc_id])
                    deleted += 1
                except Exception as e2:
                    log(f"    PRESKAKUJI {doc_id}: {e2}")

    log(f"Hotovo. Smazano {deleted} dokumentu (+ jejich chunky) ze Supabase.")


if __name__ == "__main__":
    main()
