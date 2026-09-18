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

Deno.serve(async (req: Request) => {
  if (req.method === "OPTIONS") return new Response("ok", { headers: corsHeaders });
  if (req.method !== "POST") return jsonResponse({ error: "method_not_allowed" }, 405);

  try {
    const authHeader = req.headers.get("Authorization") ?? "";
    const supabaseUrl = Deno.env.get("SUPABASE_URL")!;
    const anonKey = Deno.env.get("SUPABASE_ANON_KEY")!;
    const serviceKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

    const userClient = createClient(supabaseUrl, anonKey, { global: { headers: { Authorization: authHeader } } });
    const { data: userData, error: userErr } = await userClient.auth.getUser();
    if (userErr || !userData?.user) return jsonResponse({ error: "unauthorized" }, 401);

    const adminClient = createClient(supabaseUrl, serviceKey);
    const { data: profile } = await adminClient.from("profiles").select("is_admin").eq("id", userData.user.id).single();
    if (!profile?.is_admin) return jsonResponse({ error: "forbidden" }, 403);

    const body = await req.json().catch(() => ({}));
    const rpcName: string = body?.rpc_name;
    const rpcArgs: Record<string, unknown> = body?.rpc_args ?? {};
    const action: string = body?.action;
    const sql: string | undefined = body?.sql;

    if (!rpcName) return jsonResponse({ error: "missing rpc_name" }, 400);

    const { data: url, error: rpcErr } = await adminClient.rpc(rpcName, rpcArgs);
    if (rpcErr || !url || typeof url !== "string") {
      return jsonResponse({ error: "neon_url_unavailable", detail: rpcErr?.message ?? null }, 502);
    }

    const client = postgres(url, { max: 1, idle_timeout: 10, connect_timeout: 8, prepare: false });
    try {
      if (action === "introspect") {
        const rows = await client.unsafe(
          "select table_name, column_name, data_type, is_nullable, column_default from information_schema.columns where table_schema='public' order by table_name, ordinal_position"
        );
        const idx = await client.unsafe(
          "select indexname, indexdef from pg_indexes where schemaname='public'"
        );
        return jsonResponse({ columns: rows, indexes: idx });
      }
      if (action === "exec") {
        if (!sql) return jsonResponse({ error: "missing sql" }, 400);
        const result = await client.unsafe(sql);
        return jsonResponse({ ok: true, rowCount: Array.isArray(result) ? result.length : null, rows: Array.isArray(result) ? result.slice(0, 20) : null });
      }
      return jsonResponse({ error: "unknown_action" }, 400);
    } finally {
      await client.end({ timeout: 3 });
    }
  } catch (e) {
    return jsonResponse({ error: String(e) }, 500);
  }
});
