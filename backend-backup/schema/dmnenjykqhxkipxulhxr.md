# Schema snapshot — main Supabase project (`dmnenjykqhxkipxulhxr`)

Manual snapshot generated 2026-09-18 via Supabase MCP (`list_tables`, `execute_sql` against `pg_policies`, `pg_class`, `pg_proc`, `information_schema.triggers`). Not a restorable `pg_dump` — a readable reference to diff against over time. See `../README.md` for context (project audit item K-02).

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
| doc_type | text | CHECK: one of `zakon, judikat, narizeni, vyhlaska, opatreni, dekret, jiny_predpis, rozhodnuti_uohs, duvodova_zprava` |
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
| version_iri | text | nullable |
| valid_from | date | nullable |
| valid_until | date | nullable |
| superseded_by | uuid | nullable, FK -> documents.id |
| is_current | boolean | default `true` |
| explains_document_id | uuid | nullable, FK -> documents.id. Comment: "Pro doc_type=duvodova_zprava: odkaz na dokument (zakon) v tabulce documents, ke kteremu se duvodova zprava vztahuje." |
| predpis_cislo | integer | GENERATED from `external_id` via regex `^(\d+)/` |
| predpis_rok | integer | GENERATED from `external_id` via regex `^\d+/(\d{4})` |
| has_pending_chunks | boolean | default `true` |

FKs: `source_id -> sources.id`, `superseded_by -> documents.id`, `explains_document_id -> documents.id`
Referenced by: `chunks.document_id`, `psp_dz_check_log.document_id`

### `chunks`
RLS enabled (not forced).
| column | type | notes |
|---|---|---|
| id | uuid | PK, default `gen_random_uuid()` |
| document_id | uuid | FK -> documents.id |
| chunk_index | integer | |
| heading | text | nullable |
| content | text | nullable (nulled out once migrated to storage — see `content_migrated`) |
| embedding | vector | nullable |
| created_at | timestamptz | default `now()` |
| content_tsv | tsvector | nullable |
| content_migrated | boolean | default `false` |

### `profiles`
RLS enabled (not forced).
| column | type | notes |
|---|---|---|
| id | uuid | PK, FK -> auth.users.id |
| email | text | |
| display_name | text | nullable |
| status | text | default `'pending'`, CHECK: `pending, approved, rejected, revoked` |
| is_admin | boolean | default `false` |
| invite_note | text | nullable |
| approved_by | uuid | nullable, FK -> auth.users.id |
| approved_at | timestamptz | nullable |
| created_at | timestamptz | default `now()` |
| gemini_key_secret_id | uuid | nullable — points at a Vault secret, not the key itself |
| first_name | text | nullable |
| last_name | text | nullable |
| phone | text | nullable |
| tier | text | default `'free'`, CHECK: `free, plus, pro` |
| trial_queries_used | integer | default `0` |
| can_prioritize_embedding | boolean | default `false` |

### `usage_log`
RLS enabled (not forced).
| column | type | notes |
|---|---|---|
| id | bigint | PK, identity always |
| user_id | uuid | FK -> auth.users.id |
| occurred_at | timestamptz | default `now()` |
| request_type | text | CHECK: `search, chat, web_search, document_review` |
| model | text | nullable |
| input_tokens | integer | nullable |
| output_tokens | integer | nullable |
| query_preview | text | nullable |

### `predpis_requests`
RLS enabled (not forced).
| column | type | notes |
|---|---|---|
| id | uuid | PK, default `gen_random_uuid()` |
| user_id | uuid | FK -> auth.users.id |
| query_text | text | |
| status | text | default `'pending'`, CHECK: `pending, fetched, not_found, error` |
| note | text | nullable |
| requested_at | timestamptz | default `now()` |
| processed_at | timestamptz | nullable |

### `deleted_obsolete_predpisy_log`
RLS enabled (not forced). No primary key. Audit log of deleted obsolete předpisy (id, external_id, title, doc_type, is_current, deleted_at — all nullable).

### `psp_dz_check_log`
RLS enabled (not forced). Comment: "Persistent cache of psp.cz dukvodova zprava lookup attempts (sync_psp_tisky.py), so daily reruns skip laws recently checked without success instead of re-attempting the same stuck subset forever."
| column | type | notes |
|---|---|---|
| document_id | uuid | PK, FK -> documents.id |
| result | text | CHECK: `not_found, error, found` |
| checked_at | timestamptz | default `now()` |

## RLS policies (`pg_policies`, schema `public`)

| table | policy | cmd | roles | using / with_check |
|---|---|---|---|---|
| chunks | approved or trial users can read chunks | SELECT | authenticated | `EXISTS (SELECT 1 FROM profiles p WHERE p.id = auth.uid() AND p.status IN ('approved','pending'))` |
| documents | approved or trial users can read documents | SELECT | authenticated | same as above |
| predpis_requests | admins can view all predpis requests | SELECT | authenticated | `is_admin(auth.uid())` |
| predpis_requests | users insert own predpis requests | INSERT | authenticated | with_check: `auth.uid() = user_id` |
| predpis_requests | users view own predpis requests | SELECT | authenticated | `auth.uid() = user_id` |
| profiles | admins can update any profile | UPDATE | authenticated | `is_admin(auth.uid())` |
| profiles | users can update own profile basics | UPDATE | authenticated | using+with_check: `id = auth.uid()` |
| profiles | users can view own profile | SELECT | authenticated | `id = auth.uid() OR is_admin(auth.uid())` |
| sources | approved users can read sources | SELECT | authenticated | `EXISTS (SELECT 1 FROM profiles p WHERE p.id = auth.uid() AND p.status = 'approved')` |
| usage_log | users can view own usage | SELECT | authenticated | `user_id = auth.uid() OR EXISTS (SELECT 1 FROM profiles a WHERE a.id = auth.uid() AND a.is_admin)` |

Note: `deleted_obsolete_predpisy_log` and `psp_dz_check_log` have RLS enabled but no policies found — meaning no `authenticated`/`anon` role can access them at all (service_role bypasses RLS by default), which is presumably intentional for these internal/admin-only tables.

## RLS enabled per table (`pg_class.relrowsecurity`)

All 8 public tables have RLS enabled, none have `FORCE ROW LEVEL SECURITY`:
`chunks`, `deleted_obsolete_predpisy_log`, `documents`, `predpis_requests`, `profiles`, `psp_dz_check_log`, `sources`, `usage_log`.

## Functions (schema `public`)

Utility/extension functions from `pgstattuple` (`pg_relpages`, `pgstatginindex`, `pgstathashindex`, `pgstatindex`, `pgstattuple`, `pgstattuple_approx`) are omitted below as they're extension-provided, not app logic.

```sql
CREATE OR REPLACE FUNCTION public.admin_set_profile_status(p_user_id uuid, p_status text)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
begin
  if not is_admin(auth.uid()) then
    raise exception 'not_authorized' using errcode = '42501';
  end if;
  if p_status not in ('pending','approved','rejected','revoked') then
    raise exception 'invalid_status';
  end if;

  update public.profiles
  set
    status = p_status,
    approved_at = case when p_status = 'approved' then now() else approved_at end,
    approved_by = case when p_status = 'approved' then auth.uid() else approved_by end
  where id = p_user_id;
end;
$function$;

CREATE OR REPLACE FUNCTION public.chunks_maintain_has_pending_chunks()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
declare v_doc_id uuid;
declare v_still_pending boolean;
begin
  if tg_op = 'INSERT' then
    if new.embedding is null then
      update public.documents set has_pending_chunks = true
      where id = new.document_id and has_pending_chunks = false;
    end if;
    return new;
  elsif tg_op = 'UPDATE' then
    if new.embedding is not null and old.embedding is null then
      select exists(select 1 from public.chunks where document_id = new.document_id and embedding is null and id <> new.id)
      into v_still_pending;
      if not v_still_pending then
        update public.documents set has_pending_chunks = false where id = new.document_id;
      end if;
    elsif new.embedding is null and old.embedding is not null then
      update public.documents set has_pending_chunks = true
      where id = new.document_id and has_pending_chunks = false;
    end if;
    return new;
  elsif tg_op = 'DELETE' then
    if old.embedding is null then
      select exists(select 1 from public.chunks where document_id = old.document_id and embedding is null)
      into v_still_pending;
      if not v_still_pending then
        update public.documents set has_pending_chunks = false where id = old.document_id;
      end if;
    end if;
    return old;
  end if;
  return null;
end;
$function$;

CREATE OR REPLACE FUNCTION public.delete_user_gemini_key(p_user_id uuid)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'vault'
AS $function$
BEGIN
  DELETE FROM vault.secrets
  WHERE id = (SELECT gemini_key_secret_id FROM public.profiles WHERE id = p_user_id);
END;
$function$;

CREATE OR REPLACE FUNCTION public.finalize_chunk_storage_migration_batch(p_chunk_ids uuid[])
 RETURNS integer
 LANGUAGE sql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  with updated as (
    update public.chunks
    set content = null,
        content_migrated = true
    where id = any(p_chunk_ids)
      and content_migrated = false
      and content is not null
    returning id
  )
  select count(*)::integer from updated;
$function$;

CREATE OR REPLACE FUNCTION public.get_chunk_embedding_status(p_document_ids uuid[])
 RETURNS TABLE(document_id uuid, total integer, embedded integer)
 LANGUAGE sql
 STABLE
AS $function$
  select c.document_id, count(*)::int as total, count(*) filter (where c.embedding is not null)::int as embedded
  from chunks c
  where c.document_id = any(p_document_ids)
  group by c.document_id;
$function$;

CREATE OR REPLACE FUNCTION public.get_esbirka_api_key()
 RETURNS text
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'vault'
AS $function$
declare
  v_key text;
begin
  select decrypted_secret into v_key
  from vault.decrypted_secrets
  where name = 'esbirka_rest_api_key';
  return v_key;
end;
$function$;

-- get_neon_celni_db_url / get_neon_eru_db_url / get_neon_fs_db_url / get_neon_ifrs_db_url /
-- get_neon_mf_db_url / get_neon_mmr_db_url / get_neon_mv_db_url / get_neon_uohs_db_url /
-- get_neon_uoou_db_url / get_neon_zakony_db_url(p_shard) all follow the same shape:
-- SECURITY DEFINER, reads a named secret out of vault.decrypted_secrets and returns it.
-- Example (get_neon_zakony_db_url, the parameterized one):
CREATE OR REPLACE FUNCTION public.get_neon_zakony_db_url(p_shard text)
 RETURNS text
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'vault'
AS $function$
declare
  v_key text;
begin
  select decrypted_secret into v_key
  from vault.decrypted_secrets
  where name = 'neon_zakony_' || p_shard || '_db_url'
  limit 1;
  return v_key;
end;
$function$;

CREATE OR REPLACE FUNCTION public.get_pending_chunks_by_doctype(p_doc_type text, p_limit integer)
 RETURNS TABLE(id uuid, heading text, content text, document_id uuid)
 LANGUAGE sql
 STABLE
AS $function$
  select c.id, c.heading, c.content, c.document_id
  from chunks c join documents d on d.id = c.document_id
  where c.embedding is null and d.skip_embedding = false and d.doc_type = p_doc_type and d.has_pending_chunks = true
  order by c.created_at asc
  limit p_limit;
$function$;

CREATE OR REPLACE FUNCTION public.get_pending_chunks_prioritized(p_limit integer, p_ascending boolean DEFAULT false)
 RETURNS TABLE(id uuid, heading text, content text, document_id uuid)
 LANGUAGE plpgsql
 STABLE
AS $function$
begin
  if p_ascending then
    return query
      select c.id, c.heading, c.content, c.document_id
      from chunks c join documents d on d.id = c.document_id
      where c.embedding is null and d.skip_embedding = false and d.doc_type <> 'rozhodnuti_uohs' and d.has_pending_chunks = true
      order by d.embed_priority asc nulls last, d.created_at desc
      limit p_limit;
  else
    return query
      select c.id, c.heading, c.content, c.document_id
      from chunks c join documents d on d.id = c.document_id
      where c.embedding is null and d.skip_embedding = false and d.doc_type <> 'rozhodnuti_uohs' and d.has_pending_chunks = true
      order by d.embed_priority desc nulls last, d.created_at asc
      limit p_limit;
  end if;
end;
$function$;

CREATE OR REPLACE FUNCTION public.get_pending_storage_migration_chunks(p_limit integer DEFAULT 200)
 RETURNS TABLE(id uuid, content text)
 LANGUAGE sql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  select c.id, c.content
  from chunks c
  where c.content_migrated = false
    and c.content is not null
  order by c.id asc
  limit p_limit;
$function$;

CREATE OR REPLACE FUNCTION public.get_tavily_key()
 RETURNS text
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'vault'
AS $function$
declare
  v_key text;
begin
  select decrypted_secret into v_key
  from vault.decrypted_secrets
  where name = 'tavily_api_key'
  limit 1;
  return v_key;
end;
$function$;

CREATE OR REPLACE FUNCTION public.get_user_gemini_key(p_user_id uuid)
 RETURNS text
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'vault'
AS $function$
declare
  v_key text;
begin
  select ds.decrypted_secret into v_key
  from vault.decrypted_secrets ds
  join public.profiles p on p.gemini_key_secret_id = ds.id
  where p.id = p_user_id;
  return v_key;
end;
$function$;

CREATE OR REPLACE FUNCTION public.handle_new_user()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
begin
  insert into public.profiles (id, email)
  values (new.id, new.email);

  -- Auto-confirm email removed 17. 9. 2026: outbound e-mail (Resend SMTP)
  -- is now configured, so real Supabase e-mail confirmation is used again.
  -- Manual admin approval (profiles.status) still gates actual app access
  -- on top of this.

  return new;
end;
$function$;

CREATE OR REPLACE FUNCTION public.handle_user_email_update()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
begin
  if new.email is distinct from old.email then
    update public.profiles set email = new.email where id = new.id;
  end if;
  return new;
end;
$function$;

CREATE OR REPLACE FUNCTION public.has_gemini_key(p_user_id uuid)
 RETURNS boolean
 LANGUAGE sql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  select gemini_key_secret_id is not null from public.profiles where id = p_user_id;
$function$;

CREATE OR REPLACE FUNCTION public.is_admin(uid uuid)
 RETURNS boolean
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  select coalesce((select is_admin from public.profiles where id = uid), false);
$function$;

CREATE OR REPLACE FUNCTION public.match_chunks(query_embedding vector, match_count integer DEFAULT 8, min_similarity double precision DEFAULT 0.55, p_as_of date DEFAULT NULL::date, p_doc_types text[] DEFAULT NULL::text[])
 RETURNS TABLE(chunk_id uuid, document_id uuid, heading text, content text, similarity double precision, doc_title text, doc_url text, doc_type text)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions'
AS $function$
begin
  if not exists (select 1 from public.profiles where id = auth.uid() and status in ('approved','pending')) then
    raise exception 'not approved';
  end if;
  return query
  select c.id, c.document_id, c.heading, c.content,
         (1 - (c.embedding <=> query_embedding))::float as similarity,
         d.title, d.url, d.doc_type
  from public.chunks c
  join public.documents d on d.id = c.document_id
  where
    case
      when p_as_of is null then d.is_current = true
      else (d.valid_from is null or d.valid_from <= p_as_of)
       and (d.valid_until is null or p_as_of <= d.valid_until)
    end
    and (1 - (c.embedding <=> query_embedding)) >= min_similarity
    and (p_doc_types is null or d.doc_type = any(p_doc_types))
  order by c.embedding <=> query_embedding
  limit match_count;
end;
$function$;

CREATE OR REPLACE FUNCTION public.prevent_profile_privilege_escalation()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
begin
  if auth.uid() is null or is_admin(auth.uid()) then
    return new;
  end if;

  new.status := old.status;
  new.is_admin := old.is_admin;
  new.tier := old.tier;
  new.approved_by := old.approved_by;
  new.approved_at := old.approved_at;
  new.gemini_key_secret_id := old.gemini_key_secret_id;
  new.email := old.email;
  new.trial_queries_used := old.trial_queries_used;
  return new;
end;
$function$;

CREATE OR REPLACE FUNCTION public.rls_auto_enable()
 RETURNS event_trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog'
AS $function$
DECLARE
  cmd record;
BEGIN
  FOR cmd IN
    SELECT *
    FROM pg_event_trigger_ddl_commands()
    WHERE command_tag IN ('CREATE TABLE', 'CREATE TABLE AS', 'SELECT INTO')
      AND object_type IN ('table','partitioned table')
  LOOP
     IF cmd.schema_name IS NOT NULL AND cmd.schema_name IN ('public') AND cmd.schema_name NOT IN ('pg_catalog','information_schema') AND cmd.schema_name NOT LIKE 'pg_toast%' AND cmd.schema_name NOT LIKE 'pg_temp%' THEN
      BEGIN
        EXECUTE format('alter table if exists %s enable row level security', cmd.object_identity);
        RAISE LOG 'rls_auto_enable: enabled RLS on %', cmd.object_identity;
      EXCEPTION
        WHEN OTHERS THEN
          RAISE LOG 'rls_auto_enable: failed to enable RLS on %', cmd.object_identity;
      END;
     ELSE
        RAISE LOG 'rls_auto_enable: skip % (either system schema or not in enforced list: %.)', cmd.object_identity, cmd.schema_name;
     END IF;
  END LOOP;
END;
$function$;
-- (this is an event trigger's handler function; see "Event triggers" note below)

CREATE OR REPLACE FUNCTION public.set_embed_priority(p_document_id uuid, p_priority boolean)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_allowed boolean;
begin
  select coalesce(is_admin(auth.uid()), false)
      or coalesce((select can_prioritize_embedding from public.profiles where id = auth.uid()), false)
  into v_allowed;

  if not v_allowed then
    raise exception 'not authorized to set embed priority';
  end if;

  update public.documents
  set embed_priority = case when p_priority then 200 else 0 end
  where id = p_document_id;
end;
$function$;

CREATE OR REPLACE FUNCTION public.set_embed_priority_top(p_document_id uuid, p_top boolean)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_is_admin boolean;
begin
  select coalesce(is_admin(auth.uid()), false) into v_is_admin;

  if not v_is_admin then
    raise exception 'not authorized to set top embed priority';
  end if;

  update public.documents
  set embed_priority = case when p_top then 300 else 200 end
  where id = p_document_id;
end;
$function$;

CREATE OR REPLACE FUNCTION public.set_user_gemini_key(p_user_id uuid, p_key text)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'vault'
AS $function$
declare
  v_secret_id uuid;
begin
  select gemini_key_secret_id into v_secret_id from public.profiles where id = p_user_id;
  if v_secret_id is not null then
    perform vault.update_secret(v_secret_id, p_key);
  else
    v_secret_id := vault.create_secret(p_key, 'gemini_key_' || p_user_id::text, 'Gemini API key for user ' || p_user_id::text);
    update public.profiles set gemini_key_secret_id = v_secret_id where id = p_user_id;
  end if;
end;
$function$;
```

Note: none of these function bodies contain a literal secret value — the `get_neon_*_db_url` / `get_*_key` functions all read from `vault.decrypted_secrets` by name; the actual secret values live only in Supabase Vault, not in this dump.

Note on `rls_auto_enable`: this is the handler for an event trigger (not visible via `information_schema.triggers`, which only covers table-level triggers) that auto-enables RLS on any newly created table in `public`. Not independently verified via `pg_event_trigger` catalog in this pass — flagged here for awareness.

## Table-level triggers (`information_schema.triggers`)

| table | trigger | timing | event | action |
|---|---|---|---|---|
| chunks | trg_chunks_maintain_has_pending | AFTER | INSERT | `chunks_maintain_has_pending_chunks()` |
| chunks | trg_chunks_maintain_has_pending | AFTER | DELETE | `chunks_maintain_has_pending_chunks()` |
| chunks | trg_chunks_maintain_has_pending | AFTER | UPDATE | `chunks_maintain_has_pending_chunks()` |
| profiles | trg_prevent_profile_privilege_escalation | BEFORE | UPDATE | `prevent_profile_privilege_escalation()` |

Note: `handle_new_user` and `handle_user_email_update` are almost certainly wired as triggers on `auth.users` (not `public`), so they don't show up in the `public`-schema trigger query above — this is expected, not a gap.
