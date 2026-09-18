import { createClient } from "jsr:@supabase/supabase-js@2";
import postgres from "npm:postgres";

const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
};

Deno.serve(async (req: Request) => {
  if (req.method === "OPTIONS") return new Response("ok", { headers: CORS });
  try {
    const adminClient = createClient(Deno.env.get("SUPABASE_URL")!, Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!);
    const { data: url, error } = await adminClient.rpc("get_neon_uohs_db_url");
    if (error || !url) return new Response(JSON.stringify({ error: "no_url", detail: error }), { status: 500, headers: CORS });
    const sql = postgres(url, { max: 1, idle_timeout: 10, connect_timeout: 8, prepare: false });
    try {
      const docCols = await sql`select column_name, data_type from information_schema.columns where table_name = 'documents' order by ordinal_position`;
      const chunkCols = await sql`select column_name, data_type from information_schema.columns where table_name = 'chunks' order by ordinal_position`;
      const indexes = await sql`select indexname, indexdef from pg_indexes where tablename in ('documents','chunks')`;
      const sampleDoc = await sql`select * from documents limit 1`;
      return new Response(JSON.stringify({ docCols, chunkCols, indexes, sampleDoc }, null, 2), { headers: { "Content-Type": "application/json", ...CORS } });
    } finally {
      await sql.end();
    }
  } catch (e) {
    return new Response(JSON.stringify({ error: String(e) }), { status: 500, headers: CORS });
  }
});
