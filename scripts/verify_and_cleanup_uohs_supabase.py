"""
Overuje, ktere rozhodnuti UOHS uz existuji v Neon projektu (ai-semanticke-zakony-uohs)
s KOMPLETNIM embeddingem a presne stejnym poctem chunku jako ve stare Supabase
judikatura DB (ejjkyrprdzetxfwlkboa, doc_type=rozhodnuti_uohs), a takove bezpecne
maze ze stare Supabase, aby se uvolnilo misto - viz vzor
scripts/verify_and_cleanup_zakony_supabase.py.

Bezpecnostni zasady (zamerne konzervativni, protoze mazani je nevratne):
- NIC se nesmaze, pokud v Neonu neexistuje dokument se stejnym external_id.
- NIC se nesmaze, pokud pocet chunku v Neonu neodpovida presne poctu chunku
  aktualne v Supabase (ochrana proti necekanym rozdilum).
- NIC se nesmaze, pokud v Neonu nema VSECHNY chunky embedovane
  (embedding is not null u kazdeho jednoho chunku dokumentu).
- Dokumenty s nulovym poctem chunku se preskakuji (nedaji se bezpecne overit).
- Pokud cokoliv selze neocekavane (chyba pripojeni apod.), dany dokument se
  proste PRESKOCI (nesmaze se).
- Mazani je omezene na MAX_DELETE_PER_RUN dokumentu za jeden beh.
"""

import os
import sys
import time

import psycopg2
import requests

SUPABASE_URL = os.environ["SUPABASE_URL"]
SERVICE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
NEON_UOHS_DB_URL = os.environ["NEON_UOHS_DB_URL"]
MAX_DELETE_PER_RUN = int(os.environ.get("MAX_DELETE_PER_RUN", "500"))

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


def get_supabase_uohs_docs():
    """Vsechna rozhodnuti UOHS ve stare Supabase, kazde s poctem chunku
    (pres PostgREST embedded resource chunks(id))."""
    out = []
    page = 500
    offset = 0
    while True:
        rows = sb_get(
            "documents",
            {
                "select": "id,external_id,chunks(id)",
                "doc_type": "eq.rozhodnuti_uohs",
                "order": "id.asc",
                "limit": str(page),
                "offset": str(offset),
            },
        )
        if not rows:
            break
        for r in rows:
            out.append(
                {
                    "id": r["id"],
                    "external_id": r.get("external_id"),
                    "chunk_count": len(r.get("chunks") or []),
                }
            )
        offset += page
        if len(rows) < page:
            break
    return out


def get_neon_fully_embedded():
    """{external_id: pocet_chunku} pro dokumenty v Neonu, kde VSECHNY chunky
    maji embedding a je jich alespon 1."""
    conn = psycopg2.connect(NEON_UOHS_DB_URL, connect_timeout=15)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                select d.external_id, count(c.id) as total, count(c.id) filter (where c.embedding is not null) as embedded
                from documents d
                join chunks c on c.document_id = d.id
                where d.external_id is not null
                group by d.external_id
                """
            )
            rows = cur.fetchall()
        out = {}
        for external_id, total, embedded in rows:
            if total > 0 and total == embedded:
                out[external_id] = total
        return out
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
    log("Nacitam rozhodnuti UOHS ze stare Supabase...")
    supabase_docs = get_supabase_uohs_docs()
    log(f"Supabase: {len(supabase_docs)} dokumentu rozhodnuti_uohs.")

    log("Nacitam plne zembedovane dokumenty z Neonu...")
    try:
        neon_ready = get_neon_fully_embedded()
    except Exception as e:
        log(f"CHYBA pripojeni k Neonu, koncim bez mazani: {e}")
        return
    log(f"Neon: {len(neon_ready)} dokumentu plne zembedovano.")

    safe_to_delete = []
    skipped_no_match = 0
    skipped_mismatch = 0
    skipped_zero_chunks = 0

    for d in supabase_docs:
        if not d["external_id"] or d["chunk_count"] == 0:
            skipped_zero_chunks += 1
            continue
        neon_count = neon_ready.get(d["external_id"])
        if neon_count is None:
            skipped_no_match += 1
            continue
        if neon_count != d["chunk_count"]:
            skipped_mismatch += 1
            continue
        safe_to_delete.append(d["id"])

    log(
        f"Bezpecne k smazani: {len(safe_to_delete)} | "
        f"bez shody v Neonu: {skipped_no_match} | "
        f"neshoda poctu chunku: {skipped_mismatch} | "
        f"nulovy pocet chunku: {skipped_zero_chunks}"
    )

    to_delete = safe_to_delete[:MAX_DELETE_PER_RUN]
    if len(safe_to_delete) > MAX_DELETE_PER_RUN:
        log(f"Omezuji na MAX_DELETE_PER_RUN={MAX_DELETE_PER_RUN}, zbytek doplni dalsi beh.")

    batch_size = 200
    deleted = 0
    for i in range(0, len(to_delete), batch_size):
        batch = to_delete[i : i + batch_size]
        try:
            delete_batch(batch)
            deleted += len(batch)
            log(f"Smazano {deleted}/{len(to_delete)}...")
        except Exception as e:
            log(f"CHYBA behem mazani davky, preskakuji ji: {e}")

    log(f"Hotovo. Smazano celkem {deleted} dokumentu (+ jejich chunky) ze stare Supabase.")


if __name__ == "__main__":
    main()
