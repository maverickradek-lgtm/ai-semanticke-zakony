# Schema snapshot — judikatura Supabase project (`ejjkyrprdzetxfwlkboa`)

Manual snapshot generated 2026-09-18 via Supabase MCP (`list_tables`, `execute_sql` against `pg_policies`, `pg_class`, `pg_proc`, `information_schema.triggers`). No Edge Functions live in this project. Not a restorable `pg_dump` — a readable reference to diff against over time. See `../README.md` for context (project audit item K-02).

## Tables (schema `public`)

### `sources`
RLS enabled (not forced).
| column | type | notes |
|---|---|---|
| id | uuid | PK, default `gen_random_uuid()` |
| code | text | unique |
| name | text | |
| base_url | text | nullable |
| last_synced_at | timestamptz | nullable |
| created_at | timestamptz | default `now()` |

FK referenced by: `documents.source_id -> sources.id`

### `documents`
RLS enabled (not forced).
| column | type | notes |
|---|---|---|
| id | uuid | PK, default `gen_random_uuid()` |
| source_id | uuid | |
| external_id | text | |
| doc_type | text | CHECK: one of `judikat_ns, judikat_nss, judikat_us, rozhodnuti_uohs, judikat` |
| title | text | |
| issuer | text | nullable |
| decision_date | date | nullable |
| effective_date | date | nullable |
| url | text | nullable |
| status | text | nullable |
| content_hash | text | nullable |
| fetched_at | timestamptz | default `now()` |
| created_at | timestamptz | default `now()` |
| updated_at | timestamptz | default `now()` |
| skip_embedding | boolean | default `false` |
| embed_priority | integer | default `0` |

FK: `source_id -> sources.id`. Referenced by: `chunks.document_id`.

Note: this table has no `is_current`/`valid_from`/`valid_until`/`explains_document_id`/`predpis_*` columns — those only exist in the main project's `documents` (they relate to zákony/vyhlášky versioning and důvodové zprávy, which don't apply to judikatura).

### `chunks`
RLS enabled (not forced).
| column | type | notes |
|---|---|---|
| id | uuid | PK, default `gen_random_uuid()` |
| document_id | uuid | FK -> documents.id |
| chunk_index | integer | |
| heading | text | nullable |
| content | text | NOT NULL (unlike main project — no storage-migration split here) |
| embedding | vector | nullable |
| created_at | timestamptz | default `now()` |

Note: no `content_tsv` / `content_migrated` columns here (those are main-project-only additions from the full-text-search and storage-size work).

## RLS policies (`pg_policies`, schema `public`)

No rows returned — RLS is enabled on all 3 tables but **no policies are defined**. This means no `authenticated`/`anon` role can read/write these tables at all; only `service_role` (which bypasses RLS) can access them. Consistent with this project being a backend-only judikatura store accessed exclusively via edge functions / service-role scripts in the main project, not directly by end users.

## RLS enabled per table (`pg_class.relrowsecurity`)

All 3 public tables have RLS enabled, none have `FORCE ROW LEVEL SECURITY`: `chunks`, `documents`, `sources`.

## Functions (schema `public`)

```sql
CREATE OR REPLACE FUNCTION public.count_documents_by_type(p_doc_type text)
 RETURNS bigint
 LANGUAGE sql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  select count(*) from documents where doc_type = p_doc_type;
$function$;

CREATE OR REPLACE FUNCTION public.get_pending_chunks_by_doctype(p_doc_type text, p_limit integer)
 RETURNS TABLE(id uuid, heading text, content text, document_id uuid)
 LANGUAGE sql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  select c.id, c.heading, c.content, c.document_id
  from chunks c join documents d on d.id = c.document_id
  where c.embedding is null and d.skip_embedding = false and d.doc_type = p_doc_type
  order by c.created_at asc
  limit p_limit;
$function$;

CREATE OR REPLACE FUNCTION public.list_documents_by_type(p_doc_type text, p_search text DEFAULT NULL::text, p_limit integer DEFAULT 30, p_offset integer DEFAULT 0)
 RETURNS jsonb
 LANGUAGE sql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  with filtered as (
    select id, title, doc_type, issuer, decision_date, url, external_id, embed_priority, created_at
    from documents
    where doc_type = p_doc_type
      and (p_search is null or p_search = '' or title ilike '%'||p_search||'%' or external_id ilike '%'||p_search||'%')
    order by created_at desc
    offset p_offset limit p_limit
  ),
  total as (
    select count(*) as c from documents
    where doc_type = p_doc_type
      and (p_search is null or p_search = '' or title ilike '%'||p_search||'%' or external_id ilike '%'||p_search||'%')
  )
  select jsonb_build_object(
    'data', coalesce((select jsonb_agg(to_jsonb(filtered.*)) from filtered), '[]'::jsonb),
    'count', (select c from total)
  );
$function$;

CREATE OR REPLACE FUNCTION public.match_chunks_judikat(query_embedding vector, match_count integer DEFAULT 8, min_similarity double precision DEFAULT 0.55)
 RETURNS TABLE(chunk_id uuid, document_id uuid, heading text, content text, similarity double precision, doc_title text, doc_url text, doc_type text)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions'
AS $function$
begin
  return query
  select c.id, c.document_id, c.heading, c.content,
         (1 - (c.embedding <=> query_embedding))::float as similarity,
         d.title, d.url, d.doc_type
  from public.chunks c
  join public.documents d on d.id = c.document_id
  where d.doc_type = 'judikat'
    and c.embedding is not null
    and (1 - (c.embedding <=> query_embedding)) >= min_similarity
  order by c.embedding <=> query_embedding
  limit match_count;
end;
$function$;

CREATE OR REPLACE FUNCTION public.match_chunks_ns(query_embedding vector, match_count integer DEFAULT 8, min_similarity double precision DEFAULT 0.55)
 RETURNS TABLE(chunk_id uuid, document_id uuid, heading text, content text, similarity double precision, doc_title text, doc_url text, doc_type text)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions'
AS $function$
begin
  return query
  select c.id, c.document_id, c.heading, c.content,
         (1 - (c.embedding <=> query_embedding))::float as similarity,
         d.title, d.url, d.doc_type
  from public.chunks c
  join public.documents d on d.id = c.document_id
  where (1 - (c.embedding <=> query_embedding)) >= min_similarity
  order by c.embedding <=> query_embedding
  limit match_count;
end;
$function$;

CREATE OR REPLACE FUNCTION public.match_chunks_uohs(query_embedding vector, match_count integer DEFAULT 8, min_similarity double precision DEFAULT 0.55)
 RETURNS TABLE(chunk_id uuid, document_id uuid, heading text, content text, similarity double precision, doc_title text, doc_url text, doc_type text)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions'
AS $function$
begin
  return query
  select c.id, c.document_id, c.heading, c.content,
         (1 - (c.embedding <=> query_embedding))::float as similarity,
         d.title, d.url, d.doc_type
  from public.chunks c
  join public.documents d on d.id = c.document_id
  where d.doc_type = 'rozhodnuti_uohs'
    and c.embedding is not null
    and (1 - (c.embedding <=> query_embedding)) >= min_similarity
  order by c.embedding <=> query_embedding
  limit match_count;
end;
$function$;
```

No secret-reading functions exist in this project (no Vault `get_*_key`/`get_*_db_url` helpers here — those all live in the main project, since Vault secrets are per-project and this project has no Edge Functions needing them).

## Table-level triggers (`information_schema.triggers`)

None found in schema `public`. No `has_pending_chunks`-style bookkeeping trigger exists here (that pattern is main-project-only).
