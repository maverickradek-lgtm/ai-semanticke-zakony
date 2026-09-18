import { createClient } from "jsr:@supabase/supabase-js@2";
import postgres from "npm:postgres";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
};

function jsonResponse(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { ...corsHeaders, "Content-Type": "application/json" },
  });
}

// Ctyri rok-shardovane Neon projekty pro zakony, v pevnem vzestupnem
// poradi podle roku predpisu (do1997 <= 1998_2007 <= 2008_2020 <= 2021_dosud,
// pricemz 2021_dosud drzi i radky s predpis_rok IS NULL - ty se v ramci
// shardu radi az na konec, stejne jako ve stavajicim Supabase dotazu).
// Diky tomu, ze shardy jsou navzajem NEPREKRYVAJICI se rozsahy let, jde
// spravne globalne strankovat bez nutnosti tahat vsechna data ze vsech
// shardu najednou - staci pro kazdou stranku spocitat, kolik radku je v
// kazdem shardu (s aktualnim filtrem), a podle toho vzit jen tu cast
// stranky, ktera do daneho shardu spada.
const SHARDS = ["do1997", "1998_2007", "2008_2020", "2021_dosud"] as const;
type Shard = typeof SHARDS[number];

const clients: Partial<Record<Shard, ReturnType<typeof postgres>>> = {};

async function getSql(
  adminClient: ReturnType<typeof createClient>,
  shard: Shard
): Promise<ReturnType<typeof postgres> | null> {
  try {
    const existing = clients[shard];
    if (existing) return existing;
    const { data: url, error } = await adminClient.rpc("get_neon_zakony_db_url", { p_shard: shard });
    if (error || !url || typeof url !== "string") return null;
    const client = postgres(url, { max: 2, idle_timeout: 20, connect_timeout: 8, prepare: false });
    clients[shard] = client;
    return client;
  } catch {
    return null;
  }
}

Deno.serve(async (req: Request) => {
  if (req.method === "OPTIONS") return new Response("ok", { headers: corsHeaders });
  if (req.method !== "POST") return jsonResponse({ error: "method_not_allowed" }, 405);

  try {
    const supabaseUrl = Deno.env.get("SUPABASE_URL")!;
    const anonKey = Deno.env.get("SUPABASE_ANON_KEY")!;
    const serviceKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
    const authHeader = req.headers.get("Authorization") ?? "";

    const userClient = createClient(supabaseUrl, anonKey, {
      global: { headers: { Authorization: authHeader } },
    });
    const adminClient = createClient(supabaseUrl, serviceKey);

    const body = await req.json().catch(() => ({}));
    const action = body?.action;

    if (action === "stats") {
      const docType = typeof body?.doc_type === "string" && body.doc_type ? body.doc_type : null;
      let total = 0;
      for (const shard of SHARDS) {
        const sql = await getSql(adminClient, shard);
        if (!sql) continue;
        // Radek 2026-09-02: pocitat jen is_current=true - historicke/archivovane
        // verze zakonu se nemaji zapocitavat do zobrazovanych statistik, jinak
        // pocty pusobi zavadejicim dojmem "vic zaznamu nez realne aktualnich zakonu".
        const rows = docType
          ? await sql`select count(*)::int as count from documents where doc_type = ${docType} and is_current = true`
          : await sql`select count(*)::int as count from documents where is_current = true`;
        total += rows[0]?.count ?? 0;
      }
      return jsonResponse({ count: total });
    }

    if (action === "list") {
      const limit = Math.min(Math.max(parseInt(body?.limit, 10) || 20, 1), 100);
      const offset = Math.max(parseInt(body?.offset, 10) || 0, 0);
      const search = typeof body?.search === "string" && body.search.trim() ? body.search.trim() : null;
      const docType = typeof body?.doc_type === "string" && body.doc_type ? body.doc_type : null;
      const searchPattern = search ? `%${search}%` : null;

      // 1) spocitej pocet radku v kazdem shardu (se stejnym filtrem)
      const shardCounts: number[] = [];
      for (const shard of SHARDS) {
        const sql = await getSql(adminClient, shard);
        if (!sql) { shardCounts.push(0); continue; }
        const whereType = docType ? sql`and d.doc_type = ${docType}` : sql``;
        const whereSearch = searchPattern ? sql`and (d.title ilike ${searchPattern} or d.external_id ilike ${searchPattern})` : sql``;
        // Radek 2026-09-02: is_current = true - jinak se ve vypisu Databaze tabu
        // ukazuje aktualni I VSECHNY historicke/archivovane verze stejneho
        // zakona jako samostatne radky vedle sebe (stejne cislo/rok), coz
        // vypada jako duplicita. Historicke verze jsou dostupne pres funkci
        // "historicka zneni" (as_of_date), ne pres tento bezny seznam.
        const rows = await sql`select count(*)::int as count from documents d where d.is_current = true ${whereType} ${whereSearch}`;
        shardCounts.push(rows[0]?.count ?? 0);
      }
      const total = shardCounts.reduce((a, b) => a + b, 0);

      // 2) zjisti, ktere shardy pokryvaji pozadovane okno [offset, offset+limit)
      const data: Record<string, unknown>[] = [];
      let cum = 0;
      let remaining = limit;
      for (let i = 0; i < SHARDS.length && remaining > 0; i++) {
        const shard = SHARDS[i];
        const count = shardCounts[i];
        const shardStart = cum;
        const shardEnd = cum + count;
        cum = shardEnd;
        if (offset >= shardEnd) continue;
        const localOffset = Math.max(0, offset - shardStart);
        const localLimit = Math.min(remaining, count - localOffset);
        if (localLimit <= 0) continue;

        const sql = await getSql(adminClient, shard);
        if (!sql) continue;
        const whereType = docType ? sql`and d.doc_type = ${docType}` : sql``;
        const whereSearch = searchPattern ? sql`and (d.title ilike ${searchPattern} or d.external_id ilike ${searchPattern})` : sql``;
        const rows = await sql`
          select d.id, d.title, d.doc_type, d.issuer, d.decision_date, d.url, d.external_id,
                 d.embed_priority, d.is_current,
                 coalesce(c.total, 0)::int as chunk_total,
                 coalesce(c.embedded, 0)::int as chunk_embedded
          from documents d
          left join (
            select document_id, count(*) as total, count(*) filter (where embedding is not null) as embedded
            from chunks group by document_id
          ) c on c.document_id = d.id
          where d.is_current = true ${whereType} ${whereSearch}
          order by d.predpis_rok asc nulls last, d.predpis_cislo asc nulls last, d.created_at desc
          limit ${localLimit} offset ${localOffset}
        `;
        for (const r of rows) data.push({ ...r, shard });
        remaining -= rows.length;
      }

      return jsonResponse({ data, count: total });
    }

    if (action === "set_priority" || action === "set_priority_top") {
      const { data: userData, error: userErr } = await userClient.auth.getUser();
      if (userErr || !userData?.user) return jsonResponse({ error: "unauthorized" }, 401);

      const { data: profile } = await adminClient
        .from("profiles")
        .select("is_admin, can_prioritize_embedding")
        .eq("id", userData.user.id)
        .single();
      const isAdmin = !!profile?.is_admin;
      const canPrioritize = isAdmin || !!profile?.can_prioritize_embedding;

      const documentId = body?.document_id;
      const shard = body?.shard;
      if (!documentId || !SHARDS.includes(shard)) return jsonResponse({ error: "bad_request" }, 400);

      const sql = await getSql(adminClient, shard);
      if (!sql) return jsonResponse({ error: "neon_unavailable" }, 502);

      if (action === "set_priority") {
        if (!canPrioritize) return jsonResponse({ error: "forbidden", message: "Nemate opravneni nastavovat prioritu." }, 403);
        const value = body?.priority ? 200 : 0;
        await sql`update documents set embed_priority = ${value} where id = ${documentId}`;
        return jsonResponse({ success: true });
      } else {
        if (!isAdmin) return jsonResponse({ error: "forbidden", message: "Jen spravce muze nastavit TOP prioritu." }, 403);
        const value = body?.top ? 300 : 200;
        await sql`update documents set embed_priority = ${value} where id = ${documentId}`;
        return jsonResponse({ success: true });
      }
    }

    return jsonResponse({ error: "unknown_action" }, 400);
  } catch (e) {
    return jsonResponse({ error: String(e) }, 500);
  }
});
