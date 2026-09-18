import { createClient } from "jsr:@supabase/supabase-js@2";
import postgres from "npm:postgres";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
};

let neonSqlClient: ReturnType<typeof postgres> | null = null;

async function getNeonSql(adminClient: ReturnType<typeof createClient>): Promise<ReturnType<typeof postgres> | null> {
  try {
    if (neonSqlClient) return neonSqlClient;
    const { data: url, error } = await adminClient.rpc("get_neon_uohs_db_url");
    if (error || !url || typeof url !== "string") return null;
    neonSqlClient = postgres(url, { max: 2, idle_timeout: 20, connect_timeout: 8, prepare: false });
    return neonSqlClient;
  } catch {
    return null;
  }
}

function jsonResponse(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { ...corsHeaders, "Content-Type": "application/json" },
  });
}

function docTypeForSource(source: string): string {
  return source === "soudni_prezkum" ? "soudni_prezkum_uohs" : "rozhodnuti_uohs";
}

Deno.serve(async (req: Request) => {
  if (req.method === "OPTIONS") {
    return new Response("ok", { headers: corsHeaders });
  }
  if (req.method !== "POST") {
    return jsonResponse({ error: "method_not_allowed" }, 405);
  }

  try {
    const supabaseUrl = Deno.env.get("SUPABASE_URL")!;
    const serviceKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
    const adminClient = createClient(supabaseUrl, serviceKey);

    const sql = await getNeonSql(adminClient);
    if (!sql) {
      return jsonResponse({ error: "neon_unavailable" }, 502);
    }

    const body = await req.json().catch(() => ({}));
    const action = body?.action;
    const source: string = typeof body?.source === "string" && body.source.trim() ? body.source.trim() : "uohs";

    if (action === "stats") {
      const rows = await sql`select count(*)::int as count from documents where source = ${source}`;
      const chunkRows = await sql`
        select count(*)::int as chunk_total, count(*) filter (where c.embedding is not null)::int as chunk_embedded
        from chunks c
        join documents d on d.id = c.document_id
        where d.source = ${source}
      `;
      return jsonResponse({ count: rows[0]?.count ?? 0, chunk_total: chunkRows[0]?.chunk_total ?? 0, chunk_embedded: chunkRows[0]?.chunk_embedded ?? 0 });
    }

    if (action === "list") {
      const limit = Math.min(Math.max(parseInt(body?.limit, 10) || 20, 1), 100);
      const offset = Math.max(parseInt(body?.offset, 10) || 0, 0);
      const search = typeof body?.search === "string" && body.search.trim() ? body.search.trim() : null;
      const searchPattern = search ? `%${search}%` : null;
      const whereSearch = searchPattern ? sql`and (d.title ilike ${searchPattern} or d.external_id ilike ${searchPattern})` : sql``;

      const countRows = await sql`select count(*)::int as count from documents d where d.source = ${source} ${whereSearch}`;
      const total = countRows[0]?.count ?? 0;

      const rows = await sql`
        select d.id, d.title, d.url, d.decision_date, d.is_current, d.external_id, d.issuer, d.status,
               coalesce(c.total, 0)::int as chunk_total,
               coalese(c.embedded, 0)::int as chunk_embedded
        from documents d
        left join (
          select document_id, count(*) as total, count(*) filter (where embedding is not null) as embedded
          from chunks group by document_id
        ) c on c.document_id = d.id
        where d.source = ${source} ${whereSearch}
        order by d.decision_date desc nulls last, d.created_at desc
        limit ${limit} offset ${offset}
      `;

      const docType = docTypeForSource(source);
      const data = rows.map((r: any) => ({
        id: r.id,
        title: r.title,
        doc_type: docType,
        issuer: r.issuer || (source === "soudni_prezkum" ? "Soud" : "ÚOHS"),
        decision_date: r.decision_date,
        url: r.url,
        external_id: r.external_id,
        is_current: r.is_current,
        chunk_total: r.chunk_total,
        chunk_embedded: r.chunk_embedded,
      }));

      return jsonResponse({ data, count: total });
    }

    return jsonResponse({ error: "unknown_action" }, 400);
  } catch (e) {
    return jsonResponse({ error: String(e) }, 500);
  }
});
