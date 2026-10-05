"""
Presun dokumentu mezi dvema reshard shardy (vyvazeni zaplneni).

Duvod (Radek 2026-10-05): reshard-07 se naplnil nad efektivni rozpocet (velke
davky historickych znenich prioritnich zakonu) - po zaembedovani vsech chunku
by prekrocil 512 MB limit Neonu. Tenhle skript presune cast nejtezsich
HISTORICKYCH (is_current=false) dokumentu do jineho (prazdnejsiho) shardu.

BEZPECNOST: kazdy dokument se nejdriv ZKOPIRUJE (dokument + vsechny chunky vcetne
embeddingu a search_tsv) do ciloveho shardu, PREKONTROLUJE se pocet chunku a
teprve pak se smaze ze zdroje - po dokumentech, kazdy v samostatne transakci,
takze se nikdy neztrati data a beh se da kdykoliv prerusit a znovu spustit.
DRY_RUN=true (vychozi) jen vypise plan a nic nemeni.

Env: SRC_SHARD (napr. reshard-07), DST_SHARD (reshard-08), MOVE_CHUNKS (cil,
kolik chunku presunout), DRY_RUN, + NEON_RESHARD_NN_DB_URL.
"""

import os
import sys
import time

import psycopg2
import psycopg2.extras

SRC_SHARD = os.environ["SRC_SHARD"]
DST_SHARD = os.environ["DST_SHARD"]
MOVE_CHUNKS = int(os.environ.get("MOVE_CHUNKS", "12000"))
DRY_RUN = os.environ.get("DRY_RUN", "true").strip().lower() != "false"
TIME_BUDGET_SECONDS = int(os.environ.get("TIME_BUDGET_SECONDS", "3000"))
START = time.time()


def log(*a):
    print(*a)
    sys.stdout.flush()


def env_key(shard):
    return "NEON_" + shard.upper().replace("-", "_") + "_DB_URL"


def connect(shard):
    url = os.environ.get(env_key(shard), "").strip()
    if not url:
        raise SystemExit(f"Chybi secret {env_key(shard)}")
    last = None
    for attempt in range(8):
        try:
            return psycopg2.connect(url, connect_timeout=15, keepalives=1, keepalives_idle=30,
                                    keepalives_interval=10, keepalives_count=3)
        except Exception as e:  # noqa: BLE001
            last = e
            log(f"   connect {shard} selhalo (pokus {attempt + 1}/8): {type(e).__name__}")
            time.sleep(min(10 * (attempt + 1), 45))
    raise last


def columns(conn, table):
    with conn.cursor() as cur:
        cur.execute(
            "select column_name from information_schema.columns "
            "where table_schema='public' and table_name=%s and is_generated='NEVER' order by ordinal_position",
            (table,),
        )
        return [r[0] for r in cur.fetchall()]


def size_mb(conn):
    with conn.cursor() as cur:
        cur.execute("select pg_database_size(current_database())")
        return cur.fetchone()[0] / 1024 / 1024


def pick_documents(src):
    """Historicke, NEPRIORITNI dokumenty, nejdriv ty bez embeddingu a nejvetsi."""
    with src.cursor() as cur:
        cur.execute(
            """
            select d.id, count(c.id) as n, count(c.id) filter (where c.embedding is not null) as emb
            from documents d join chunks c on c.document_id = d.id
            where d.is_current = false and d.embed_priority < 1000
            group by d.id
            order by emb asc, n desc
            """
        )
        rows = cur.fetchall()
    picked, total = [], 0
    for doc_id, n, emb in rows:
        if total >= MOVE_CHUNKS:
            break
        picked.append((doc_id, n, emb))
        total += n
    return picked, total


def move_one(src, dst, doc_id, dcols, ccols):
    with src.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("select * from documents where id = %s", (doc_id,))
        doc = cur.fetchone()
        sel = ", ".join(("embedding::text as embedding" if c == "embedding" else
                         "search_tsv::text as search_tsv" if c == "search_tsv" else c) for c in ccols)
        cur.execute(f"select {sel} from chunks where document_id = %s order by chunk_index", (doc_id,))
        chunks = cur.fetchall()
    if doc is None:
        return 0
    n = len(chunks)
    with dst.cursor() as cur:
        cols_sql = ", ".join(dcols)
        ph = ", ".join(["%s"] * len(dcols))
        doc_vals = [doc[c] for c in dcols]
        # superseded_by / explains_document_id v reshardech nepouzivame (FK) - vynulovat
        for i, c in enumerate(dcols):
            if c in ("superseded_by", "explains_document_id"):
                doc_vals[i] = None
        cur.execute(f"insert into documents ({cols_sql}) values ({ph}) on conflict (id) do nothing", doc_vals)
        ccols_sql = ", ".join(ccols)
        tmpl = "(" + ", ".join(
            "%s::vector" if c == "embedding" else "%s::tsvector" if c == "search_tsv" else "%s" for c in ccols
        ) + ")"
        rows = [tuple(ch[c] for c in ccols) for ch in chunks]
        if rows:
            psycopg2.extras.execute_values(
                cur,
                f"insert into chunks ({ccols_sql}) values %s on conflict (id) do nothing",
                rows,
                template=tmpl,
                page_size=200,
            )
        cur.execute("select count(*) from chunks where document_id = %s", (doc_id,))
        got = cur.fetchone()[0]
        if got != n:
            dst.rollback()
            raise RuntimeError(f"kontrola poctu chunku selhala u {doc_id}: {got} != {n}")
    dst.commit()
    with src.cursor() as cur:
        cur.execute("delete from chunks where document_id = %s", (doc_id,))
        cur.execute("delete from documents where id = %s", (doc_id,))
    src.commit()
    return n


def main():
    log(f"Presun dokumentu {SRC_SHARD} -> {DST_SHARD}, cil ~{MOVE_CHUNKS} chunku, "
        f"{'DRY RUN (nic se nemeni)' if DRY_RUN else 'OSTRE'}")
    src, dst = connect(SRC_SHARD), connect(DST_SHARD)
    log(f"   velikost pred: {SRC_SHARD}={size_mb(src):.0f} MB, {DST_SHARD}={size_mb(dst):.0f} MB")
    dcols = [c for c in columns(src, "documents") if c in set(columns(dst, "documents"))]
    ccols = [c for c in columns(src, "chunks") if c in set(columns(dst, "chunks"))]
    picked, total = pick_documents(src)
    emb_total = sum(e for _, _, e in picked)
    log(f"   vybrano {len(picked)} dokumentu, {total} chunku (z toho {emb_total} s embeddingem)")
    if DRY_RUN:
        log("=== DRY RUN hotovo - nic se nepresunulo ===")
        return
    moved_docs, moved_chunks = 0, 0
    for doc_id, n, _emb in picked:
        if time.time() - START > TIME_BUDGET_SECONDS - 60:
            log("Casovy rozpocet vycerpan - koncim (dalsi beh naváže).")
            break
        for attempt in range(5):
            try:
                moved_chunks += move_one(src, dst, doc_id, dcols, ccols)
                moved_docs += 1
                break
            except psycopg2.OperationalError as e:
                log(f"   WARN {doc_id} spojeni padlo ({type(e).__name__}), obnovuji (pokus {attempt + 1}/5)")
                for c in (src, dst):
                    try:
                        c.close()
                    except Exception:  # noqa: BLE001
                        pass
                time.sleep(min(5 * (2 ** attempt), 60))
                src, dst = connect(SRC_SHARD), connect(DST_SHARD)
        if moved_docs % 20 == 0 and moved_docs:
            log(f"   ...presunuto {moved_docs} dok. / {moved_chunks} chunku")
    log(f"=== Hotovo: presunuto {moved_docs} dokumentu, {moved_chunks} chunku ===")
    log(f"   velikost po: {SRC_SHARD}={size_mb(src):.0f} MB, {DST_SHARD}={size_mb(dst):.0f} MB")
    src.close()
    dst.close()


if __name__ == "__main__":
    main()
