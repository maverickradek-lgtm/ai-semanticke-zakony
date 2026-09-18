// TEMPORARY admin-only utility: force-embed pending chunks of ONE document
// directly, bypassing get_pending_chunks_prioritized (times out under PostgREST
// at 200k+ rows). Small delay between Gemini calls to avoid per-minute rate
// limits on the shared admin key.

const ADMIN_SECRET = "parAlf-tmp-embed-8f3d9a2c-2026";
const ADMIN_USER_ID = "2648f5db-bea6-4cac-b490-ad0ec59723df";
const EMBED_MODEL = "gemini-embedding-001";
const EMBED_DIM = 256;

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
};

function json(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), { status, headers: { ...corsHeaders, "Content-Type": "application/json" } });
}

async function embedText(text: string, geminiKey: string): Promise<number[] | null> {
  const res = await fetch(`https://generativelanguage.googleapis.com/v1beta/models/${EMBED_MODEL}:embedContent`, {
    method: "POST",
    headers: { "Content-Type": "application/json", "x-goog-api-key": geminiKey },
    body: JSON.stringify({
      content: { parts: [{ text: text.slice(0, 8000) }] },
      taskType: "RETRIEVAL_DOCUMENT",
      outputDimensionality: EMBED_DIM,
    }),
  });
  if (!res.ok) {
    const t = await res.text().catch(() => "");
    throw new Error(`embed ${res.status}: ${t.slice(0, 200)}`);
  }
  const j = await res.json();
  let vec = j?.embedding?.values;
  if (!Array.isArray(vec)) return null;
  if (vec.length !== 3072) {
    const norm = Math.sqrt(vec.reduce((s: number, v: number) => s + v * v, 0));
    if (norm > 0) vec = vec.map((v: number) => v / norm);
  }
  return vec;
}

Deno.serve(async (req: Request) => {
  if (req.method === "OPTIONS") return new Response("ok", { headers: corsHeaders });
  if (req.method !== "POST") return json({ error: "method_not_allowed" }, 405);

  try {
    const body = await req.json();
    if (body?.secret !== ADMIN_SECRET) return json({ error: "unauthorized" }, 401);

    const documentId = body?.document_id;
    const limit = Number(body?.limit) > 0 ? Number(body.limit) : 20;
    const delayMs = Number(body?.delay_ms) >= 0 ? Number(body.delay_ms) : 1200;
    if (!documentId) return json({ error: "missing_params" }, 400);

    const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
    const SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

    const keyRes = await fetch(`${SUPABASE_URL}/rest/v1/rpc/get_user_gemini_key`, {
      method: "POST",
      headers: { apikey: SERVICE_KEY, Authorization: `Bearer ${SERVICE_KEY}`, "Content-Type": "application/json" },
      body: JSON.stringify({ p_user_id: ADMIN_USER_ID }),
    });
    if (!keyRes.ok) return json({ error: "gemini_key_fetch_failed", detail: await keyRes.text() }, 502);
    const geminiKey = await keyRes.json();
    if (!geminiKey) return json({ error: "no_gemini_key" }, 400);

    const chunksRes = await fetch(
      `${SUPABASE_URL}/rest/v1/chunks?document_id=eq.${documentId}&embedding=is.null&select=id,content,content_migrated&order=chunk_index.asc&limit=${limit}`,
      { headers: { apikey: SERVICE_KEY, Authorization: `Bearer ${SERVICE_KEY}` } }
    );
    if (!chunksRes.ok) return json({ error: "fetch_chunks_failed", detail: await chunksRes.text() }, 502);
    const chunks = await chunksRes.json();

    const totalRes = await fetch(
      `${SUPABASE_URL}/rest/v1/chunks?document_id=eq.${documentId}&embedding=is.null&select=id`,
      { headers: { apikey: SERVICE_KEY, Authorization: `Bearer ${SERVICE_KEY}`, Prefer: "count=exact", Range: "0-0" } }
    );
    const contentRange = totalRes.headers.get("content-range");
    const remainingBefore = contentRange ? parseInt(contentRange.split("/")[1] || "0", 10) : chunks.length;

    let embedded = 0;
    let failed = 0;
    const errors: string[] = [];

    for (const c of chunks) {
      let text: string | null = c.content;
      if ((c.content_migrated || !text) && c.id) {
        try {
          const sres = await fetch(`${SUPABASE_URL}/storage/v1/object/chunk-content/${c.id}.txt`, {
            headers: { apikey: SERVICE_KEY, Authorization: `Bearer ${SERVICE_KEY}` },
          });
          if (sres.ok) text = await sres.text();
        } catch {
          // leave text as-is
        }
      }
      if (!text) {
        failed++;
        errors.push(`${c.id}: no content available`);
        continue;
      }

      try {
        const vec = await embedText(text, geminiKey);
        if (!vec) {
          failed++;
          errors.push(`${c.id}: no embedding vector returned`);
          continue;
        }
        const updRes = await fetch(`${SUPABASE_URL}/rest/v1/chunks?id=eq.${c.id}`, {
          method: "PATCH",
          headers: {
            apikey: SERVICE_KEY,
            Authorization: `Bearer ${SERVICE_KEY}`,
            "Content-Type": "application/json",
            Prefer: "return=minimal",
          },
          body: JSON.stringify({ embedding: vec }),
        });
        if (updRes.ok) {
          embedded++;
        } else {
          failed++;
          errors.push(`${c.id}: update ${updRes.status}`);
        }
      } catch (e) {
        failed++;
        errors.push(`${c.id}: ${String(e)}`);
      }

      if (delayMs > 0) await new Promise((r) => setTimeout(r, delayMs));
    }

    return json({ remaining_before: remainingBefore, batch_size: chunks.length, embedded, failed, errors: errors.slice(0, 15) });
  } catch (e) {
    return json({ error: String(e) }, 500);
  }
});
