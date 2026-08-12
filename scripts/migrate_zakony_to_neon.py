"""
Migrace zakonu (doc_type='zakon') z hlavni Supabase DB do 4 Neon shardu podle
roku predpisu - viz pamet zakony_neon_sharding_plan.
Hranice: do 1997 / 1998-2007 / 2008-2020 / 2021-dosud.

Tento skript je POUZE KOPIROVACI - necte se z hlavni Supabase (funguje i behem
read-only vypadku, viz pg_repack_space_requirement_incident), zapisuje do
prislusneho Neonu. NEMAZE nic ze zdroje - smazani ze zdroje je samostatny,
zamerny krok az po overeni, ze data v Neonu sedi.
"""

import os
import sys
import time
import psycopg2
import psycopg2.extras
import requests

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SERVICE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
BATCH_LIMIT = int(os.environ.get("BATCH_LIMIT", "300"))
TIME_BUDGET_SECONDS = int(os.environ.get("TIME_BUDGET_SECONDS", "3000"))

NEON_URLS = {
    "do1997": os.environ["NEON_ZAKONY_DO1997_DB_URL"],
    "1998_2007": os.environ["NEON_ZAKONY_1998_2007_DB_URL"],
    "2008_2020": os.environ["NEON_ZAKONY_2008_2020_DB_URL"],
    "2021_dosud": os.environ["NEON_ZAKONY_2021_DOSUD_DB_URL"],
}

START_TIME = time.time()
SESSION = requests.Session()


def log(*a):
    print(*a)
    sys.stdout.flush()


def time_left():
    return TIME_BUDGET_SECONDS - (time.time() - START_TIME)


def bucket_for_year(rok):
    if rok is None:
        return "2021_dosud"
    if rok <= 1997:
        return "do1997"
    if rok <= 2007:
        return "1998_2007"
    if rok <= 2020:
        return "2008_2020"
    return "2021_dosud"


def parse_embedding(raw):
    if raw is None:
        return None
    if isinstance(raw, list):
        return raw
    s = str(raw).strip()
    if s.startswith("[") and s.endswith("]"):
        s = s[1:-1]
    if not s:
        return None
    return [float(x) for x in s.split(",")]


def sb_get(path, params):
    for attempt in range(5):
        r = SESSION.get(
            f"{SUPABASE_URL}/rest/v1/{path}",
            headers={
                "apikey": SERVICE_KEY,
                "Authorization": f"Bearer {SERVICE_KEY}",
            },
            params=params,
            timeout=60,
        )
        if r.status_code >= 500:
            wait = 10 * (attempt + 1)
            log(f"   sb_get {path} chyba {r.status_code}, cekam {wait}s a zkusim znovu...")
            time.sleep(wait)
            continue
        r.raise_for_status()
        return r.json()
    raise RuntimeError(f"sb_get({path}) selhalo po 5 pokusech")


def fetch_storage_content(chunk_id):
    for attempt in range(5):
        r = SESSION.get(
            f"{SUPABASE_URL}/storage/v1/object/chunk-content/{chunk_id}.txt",
            headers={
                "apikey": SERVICE_KEY,
                "Authorization": f"Bearer {SERVICE_KEY}",
            },
            timeout=30,
        )
        if r.status_code >= 500:
            wait = 5 * (attempt + 1)
            time.sleep(wait)
            continue
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.text
    raise RuntimeError(f"fetch_storage_content({chunk_id}) selhalo po 5 pokusech")


def get_migrated_marker_table_name():
    return "zakon_neon_migration_marker"


def get_already_migrated_ids():
    rows = sb_get(
        "documents",
        {
            "select": "id",
            "doc_type": "eq.zakon",
            "content_hash": "eq.__migrated_to_neon__",
        },
    )
    return set(r["id"] for r in rows)


def ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            create extension if not exists vector;
            create extension if not exists pgcrypto;

            create table if not exists documents (
              id uuid primary key,
              source_id uuid not null,
              external_id text not null,
              doc_type text not null,
              title text not null,
              issuer text,
              decision_date date,
              effective_date date,
              url text,
              status text,
              content_hash text,
              fetched_at timestamptz not null default now(),
              created_at timestamptz not null default now(),
              updated_at timestamptz not null default now(),
              skip_embedding boolean not null default false,
              embed_priority integer not null default 0,
              version_iri text,
              valid_from date,
              valid_until date,
              superseded_by uuid references documents(id),
              is_current boolean not null default true,
              explains_document_id uuid references documents(id),
              predpis_cislo integer,
              predpis_rok integer,
              has_pending_chunks boolean not null default true,
              unique (source_id, external_id)
            );

            create table if not exists chunks (
              id uuid primary key,
              document_id uuid not null references documents(id) on delete cascade,
              chunk_index integer not null,
              heading text,
              content text,
              embedding vector(256),
              created_at timestamptz not null default now(),
              unique (document_id, chunk_index)
            );

            create index if not exists documents_doc_type_idx on documents(doc_type);
            create index if not exists documents_predpis_sort_idx on documents(predpis_rok, predpis_cislo);
            create index if not exists idx_documents_explains on documents(explains_document_id);
            create index if not exists documents_superseded_idx on documents(superseded_by);
            create index if not exists chunks_document_idx on chunks(document_id);
            create index if not exists chunks_embedding_idx on chunks using hnsw (embedding vector_cosine_ops);
            create index if not exists chunks_pending_embed_idx on chunks(document_id) where embedding is null;
            create index if not exists chunks_pending_embed_created_idx on chunks(created_at) where embedding is null;
            create index if not exists documents_pending_embed_priority_idx
              on documents(embed_priority desc nulls last, created_at)
              where skip_embedding = false and has_pending_chunks = true;

            create or replace function chunks_maintain_has_pending_chunks()
            returns trigger language plpgsql as $f$
            declare v_still_pending boolean;
            begin
              if tg_op = 'INSERT' then
                if new.embedding is null then
                  update documents set has_pending_chunks = true where id = new.document_id and has_pending_chunks = false;
                end if;
                return new;
              elsif tg_op = 'UPDATE' then
                if new.embedding is not null and old.embedding is null then
                  select exists(select 1 from chunks where document_id = new.document_id and embedding is null and id <> new.id) into v_still_pending;
                  if not v_still_pending then update documents set has_pending_chunks = false where id = new.document_id; end if;
                elsif new.embedding is null and old.embedding is not null then
                  update documents set has_pending_chunks = true where id = new.document_id and has_pending_chunks = false;
                end if;
                return new;
              elsif tg_op = 'DELETE' then
                if old.embedding is null then
                  select exists(select 1 from chunks where document_id = old.document_id and embedding is null) into v_still_pending;
                  if not v_still_pending then update documents set has_pending_chunks = false where id = old.document_id; end if;
                end if;
                return old;
              end if;
              return null;
            end;
            $f$;

            drop trigger if exists trg_chunks_maintain_has_pending on chunks;
            create trigger trg_chunks_maintain_has_pending
            after insert or update of embedding or delete on chunks
            for each row execute function chunks_maintain_has_pending_chunks();

            create or replace function get_pending_chunks_prioritized(p_limit integer, p_ascending boolean default false)
            returns table(id uuid, heading text, content text, document_id uuid)
            language plpgsql stable as $f$
            begin
              if p_ascending then
                return query select c.id, c.heading, c.content, c.document_id
                  from chunks c join documents d on d.id = c.document_id
                  where c.embedding is null and d.skip_embedding = false and d.has_pending_chunks = true
                  order by d.embed_priority asc nulls last, d.created_at desc limit p_limit;
              else
                return query select c.id, c.heading, c.content, c.document_id
                  from chunks c join documents d on d.id = c.document_id
                  where c.embedding is null and d.skip_embedding = false and d.has_pending_chunks = true
                  order by d.embed_priority desc nulls last, d.created_at asc limit p_limit;
              end if;
            end;
            $f$;

            create or replace function match_chunks(query_embedding vector, match_count integer default 8,
              min_similarity double precision default 0.55, p_as_of date default null)
            returns table(chunk_id uuid, document_id uuid, heading text, content text, similarity double precision,
              doc_title text, doc_url text, doc_type text)
            language plpgsql stable as $f$
            begin
              return query
              select c.id, c.document_id, c.heading, c.content,
                     (1 - (c.embedding <=> query_embedding))::float, d.title, d.url, d.doc_type
              from chunks c join documents d on d.id = c.document_id
              where case when p_as_of is null then d.is_current = true
                         else (d.valid_from is null or d.valid_from <= p_as_of)
                          and (d.valid_until is null or p_as_of <= d.valid_until) end
                and (1 - (c.embedding <=> query_embedding)) >= min_similarity
              order by c.embedding <=> query_embedding limit match_count;
            end;
            $f$;
            """
        )
    conn.commit()


def upsert_document(conn, doc):
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into documents (
              id, source_id, external_id, doc_type, title, issuer, decision_date,
              effective_date, url, status, content_hash, fetched_at, created_at,
              updated_at, skip_embedding, embed_priority, version_iri, valid_from,
              valid_until, superseded_by, is_current, explains_document_id,
              predpis_cislo, predpis_rok
            ) values (
              %(id)s, %(source_id)s, %(external_id)s, %(doc_type)s, %(title)s,
              %(issuer)s, %(decision_date)s, %(effective_date)s, %(url)s,
              %(status)s, %(content_hash)s, %(fetched_at)s, %(created_at)s,
              %(updated_at)s, %(skip_embedding)s, %(embed_priority)s,
              %(version_iri)s, %(valid_from)s, %(valid_until)s, %(superseded_by)s,
              %(is_current)s, %(explains_document_id)s, %(predpis_cislo)s,
              %(predpis_rok)s
            )
            on conflict (id) do update set
              title = excluded.title, issuer = excluded.issuer,
              decision_date = excluded.decision_date, effective_date = excluded.effective_date,
              url = excluded.url, status = excluded.status, content_hash = excluded.content_hash,
              updated_at = excluded.updated_at, skip_embedding = excluded.skip_embedding,
              embed_priority = excluded.embed_priority, version_iri = excluded.version_iri,
              valid_from = excluded.valid_from, valid_until = excluded.valid_until,
              superseded_by = excluded.superseded_by, is_current = excluded.is_current,
              explains_document_id = excluded.explains_document_id,
              predpis_cislo = excluded.predpis_cislo, predpis_rok = excluded.predpis_rok
            """,
            doc,
        )
    conn.commit()


def upsert_chunk(conn, chunk):
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into chunks (id, document_id, chunk_index, heading, content, embedding, created_at)
            values (%(id)s, %(document_id)s, %(chunk_index)s, %(heading)s, %(content)s, %(embedding)s, %(created_at)s)
            on conflict (id) do update set
              heading = excluded.heading, content = excluded.content, embedding = excluded.embedding
            """,
            chunk,
        )
    conn.commit()


def mark_migrated(doc_id):
    for attempt in range(5):
        r = SESSION.patch(
            f"{SUPABASE_URL}/rest/v1/documents",
            headers={
                "apikey": SERVICE_KEY,
                "Authorization": f"Bearer {SERVICE_KEY}",
                "Content-Type": "application/json",
                "Prefer": "return=minimal",
            },
            params={"id": f"eq.{doc_id}"},
            json={"content_hash": "__migrated_to_neon__", "skip_embedding": True},
            timeout=30,
        )
        if r.status_code >= 500:
            time.sleep(5 * (attempt + 1))
            continue
        if r.status_code == 400 or r.status_code >= 300:
            log(f"   mark_migrated({doc_id}) neuspesne: {r.status_code} {r.text[:200]} (DB je mozna read-only, pokracuji bez oznaceni)")
            return
        return
    log(f"   mark_migrated({doc_id}) selhalo po 5 pokusech, pokracuji bez oznaceni")


def main():
    log("Zacinam migraci zakonu do Neon shardu (kopie, bez mazani ze zdroje)...")

    already_migrated = set()
    try:
        already_migrated = get_already_migrated_ids()
        log(f"Uz drive oznaceno jako migrovano: {len(already_migrated)}")
    except Exception as e:
        log(f"Nepodarilo se nacist marker seznam ({e}), pokracuji bez neho (muze zpusobit duplicitni praci, ne chybu)")

    neon_conns = {}
    for key, url in NEON_URLS.items():
        conn = psycopg2.connect(url, connect_timeout=15)
        ensure_schema(conn)
        neon_conns[key] = conn
        log(f"Schema pripraveno v Neon shardu: {key}")

    offset = 0
    page = 100
    processed = 0
    migrated_docs = 0
    migrated_chunks = 0

    while time_left() > 60:
        docs = sb_get(
            "documents",
            {
                "select": "id,source_id,external_id,doc_type,title,issuer,decision_date,effective_date,url,status,content_hash,fetched_at,created_at,updated_at,skip_embedding,embed_priority,version_iri,valid_from,valid_until,superseded_by,is_current,explains_document_id,predpis_cislo,predpis_rok",
                "doc_type": "eq.zakon",
                "order": "id.asc",
                "limit": str(page),
                "offset": str(offset),
            },
        )
        if not docs:
            log("Vsechny zakony projity, konec.")
            break

        offset += page

        for doc in docs:
            if doc["id"] in already_migrated:
                continue
            if migrated_docs >= BATCH_LIMIT:
                break
            if time_left() <= 60:
                break

            processed += 1
            rok = doc.get("predpis_rok")
            target_key = bucket_for_year(rok)
            conn = neon_conns[target_key]

            try:
                upsert_document(conn, doc)

                chunks = sb_get(
                    "chunks",
                    {
                        "select": "id,document_id,chunk_index,heading,content,embedding,created_at,content_migrated",
                        "document_id": f"eq.{doc['id']}",
                        "order": "chunk_index.asc",
                        "limit": "2000",
                    },
                )

                for ch in chunks:
                    content = ch.get("content")
                    if content is None and ch.get("content_migrated"):
                        content = fetch_storage_content(ch["id"])
                    upsert_chunk(
                        conn,
                        {
                            "id": ch["id"],
                            "document_id": ch["document_id"],
                            "chunk_index": ch["chunk_index"],
                            "heading": ch.get("heading"),
                            "content": content,
                            "embedding": parse_embedding(ch.get("embedding")),
                            "created_at": ch.get("created_at"),
                        },
                    )
                    migrated_chunks += 1

                mark_migrated(doc["id"])
                migrated_docs += 1

                if migrated_docs % 25 == 0:
                    log(f"...{migrated_docs} dokumentu / {migrated_chunks} chunku migrovano (shard: {target_key})")

            except Exception as e:
                log(f"CHYBA u dokumentu {doc['id']} ({doc.get('title', '')[:60]}): {e}")
                for c in neon_conns.values():
                    try:
                        c.rollback()
                    except Exception:
                        pass
                continue

        if migrated_docs >= BATCH_LIMIT:
            log(f"Dosazen BATCH_LIMIT={BATCH_LIMIT}, koncim tento beh (dalsi beh bude pokracovat).")
            break

    for conn in neon_conns.values():
        conn.close()

    log(f"Hotovo. Zpracovano={processed}, migrovano dokumentu={migrated_docs}, chunku={migrated_chunks}")


if __name__ == "__main__":
    main()
