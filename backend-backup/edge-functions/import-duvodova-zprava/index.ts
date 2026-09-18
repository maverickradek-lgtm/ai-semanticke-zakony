import { createClient } from "jsr:@supabase/supabase-js@2";
import { getDocumentProxy } from "npm:unpdf@0.11.0";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
};

const SBIRKA_URL = "https://www.psp.cz/sqw/sbirka.sqw";
const TISKT_URL = "https://www.psp.cz/sqw/text/tiskt.sqw";
const PDF_URL = "https://www.psp.cz/sqw/text/orig2.sqw";
const MAX_CHARS_TOTAL = 200000;
const MAX_CHARS_PER_CHUNK = 1500;
// Once we're collecting DZ text, stop scanning further pages once we have
// comfortably more than MAX_CHARS_TOTAL - no point parsing hundreds more
// pages just to throw the text away at the truncation step anyway. This is
// the main memory-saving lever versus extracting the whole 400+ page PDF.
const COLLECT_STOP_CHARS = MAX_CHARS_TOTAL + 20000;

function jsonResponse(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { ...corsHeaders, "Content-Type": "application/json" },
  });
}

function decodeWin1250(buf: ArrayBuffer): string {
  try {
    return new TextDecoder("windows-1250").decode(buf);
  } catch {
    return new TextDecoder("utf-8", { fatal: false }).decode(buf);
  }
}

function splitIntoChunks(text: string, maxChars = MAX_CHARS_PER_CHUNK): string[] {
  const paragraphs = text.split(/\n\n+/).map((p) => p.trim()).filter(Boolean);
  const chunks: string[] = [];
  let current = "";
  for (const para of paragraphs) {
    if (current && current.length + para.length + 2 > maxChars) {
      chunks.push(current);
      current = para;
    } else {
      current = current ? current + "\n\n" + para : para;
    }
  }
  if (current) chunks.push(current);
  return chunks;
}

async function findHistorieLink(cislo: string, rok: string): Promise<{ o: string; t: string } | null> {
  const res = await fetch(`${SBIRKA_URL}?cz=${cislo}&r=${rok}`);
  if (!res.ok) return null;
  const html = decodeWin1250(await res.arrayBuffer());
  const m = html.match(/historie\.sqw\?o=(\d+)&(?:amp;)?t=([\w\-]+)/i);
  if (!m) return null;
  return { o: m[1], t: m[2] };
}

async function findPdfIdd(o: string, t: string): Promise<{ idd: string; tisktUrl: string } | null> {
  const tisktUrl = `${TISKT_URL}?o=${o}&ct=${t}&ct1=0`;
  const res = await fetch(tisktUrl);
  if (!res.ok) return null;
  const html = decodeWin1250(await res.arrayBuffer());
  const headingIdx = html.indexOf("zákona včetně důvodové zprávy");
  if (headingIdx === -1) return null;
  const after = html.slice(headingIdx);
  const m = after.match(/orig2\.sqw\?idd=(\d+)"[^>]*title="Dokument PDF"/i);
  if (!m) return null;
  return { idd: m[1], tisktUrl };
}

async function extractDuvodovaZprava(pdfBuf: ArrayBuffer): Promise<{ text: string; pagesScanned: number; totalPages: number }> {
  const pdf = await getDocumentProxy(new Uint8Array(pdfBuf));
  const re = /D[uů]vodov[áa]\s+zpr[áa]va/i;
  let collecting = false;
  const parts: string[] = [];
  let collectedLen = 0;
  let pagesScanned = 0;
  for (let i = 1; i <= pdf.numPages; i++) {
    pagesScanned = i;
    const page = await pdf.getPage(i);
    const content = await page.getTextContent();
    const pageText = content.items.map((it: any) => it.str).join(" ");
    if (typeof page.cleanup === "function") {
      try { page.cleanup(); } catch { /* ignore */ }
    }
    if (!collecting) {
      const m = pageText.match(re);
      if (m && m.index !== undefined) {
        collecting = true;
        parts.push(pageText.slice(m.index));
        collectedLen += pageText.length - m.index;
        continue;
      }
    } else {
      parts.push(pageText);
      collectedLen += pageText.length;
      if (collectedLen > COLLECT_STOP_CHARS) break;
    }
  }
  if (typeof pdf.destroy === "function") {
    try { await pdf.destroy(); } catch { /* ignore */ }
  }
  return { text: parts.join("\n\n").trim(), pagesScanned, totalPages: pdf.numPages };
}

async function markChecked(adminClient: ReturnType<typeof createClient>, documentId: string, result: string) {
  try {
    await adminClient.from("psp_dz_check_log").upsert(
      { document_id: documentId, result, checked_at: new Date().toISOString() },
      { onConflict: "document_id" }
    );
  } catch {
    // non-critical
  }
}

Deno.serve(async (req: Request) => {
  if (req.method === "OPTIONS") {
    return new Response("ok", { headers: corsHeaders });
  }
  if (req.method !== "POST") {
    return jsonResponse({ error: "method_not_allowed" }, 405);
  }

  try {
    const authHeader = req.headers.get("Authorization") ?? "";
    const supabaseUrl = Deno.env.get("SUPABASE_URL")!;
    const anonKey = Deno.env.get("SUPABASE_ANON_KEY")!;
    const serviceKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

    const userClient = createClient(supabaseUrl, anonKey, {
      global: { headers: { Authorization: authHeader } },
    });
    const { data: userData, error: userErr } = await userClient.auth.getUser();
    if (userErr || !userData?.user) {
      return jsonResponse({ error: "unauthorized" }, 401);
    }

    const adminClient = createClient(supabaseUrl, serviceKey);
    const { data: profile } = await adminClient
      .from("profiles")
      .select("is_admin")
      .eq("id", userData.user.id)
      .single();
    if (!profile?.is_admin) {
      return jsonResponse({ error: "forbidden" }, 403);
    }

    const body = await req.json();
    const documentId: string = (body?.document_id ?? "").toString();
    if (!documentId) {
      return jsonResponse({ error: "missing_document_id" }, 400);
    }

    const { data: law, error: lawErr } = await adminClient
      .from("documents")
      .select("id,external_id,title,embed_priority")
      .eq("id", documentId)
      .single();
    if (lawErr || !law) {
      return jsonResponse({ error: "law_not_found" }, 404);
    }

    const m = (law.external_id || "").match(/^(\d{1,4})\/(\d{4})\s*Sb\.?$/);
    if (!m) {
      return jsonResponse({ error: "bad_external_id", external_id: law.external_id }, 400);
    }
    const cislo = m[1];
    const rok = m[2];
    const citace = `${cislo}/${rok}`;

    const { data: sourceRow } = await adminClient
      .from("sources")
      .select("id")
      .eq("code", "psp_tisky")
      .maybeSingle();
    let sourceId = sourceRow?.id as string | undefined;
    if (!sourceId) {
      const { data: created, error: createErr } = await adminClient
        .from("sources")
        .insert({
          code: "psp_tisky",
          name: "Poslanecka snemovna - snemovni tisky (duvodove zpravy)",
          base_url: "https://www.psp.cz/sqw/hp.sqw?k=1300",
        })
        .select("id")
        .single();
      if (createErr) return jsonResponse({ error: "source_create_failed", detail: createErr.message }, 500);
      sourceId = created.id;
    }

    const hist = await findHistorieLink(cislo, rok);
    if (!hist) {
      await markChecked(adminClient, documentId, "not_found");
      return jsonResponse({ status: "not_found", stage: "historie", citace });
    }

    const pdfInfo = await findPdfIdd(hist.o, hist.t);
    if (!pdfInfo) {
      await markChecked(adminClient, documentId, "not_found");
      return jsonResponse({ status: "not_found", stage: "pdf_link", citace, tisk: `${hist.t}/${hist.o}` });
    }

    const pdfRes = await fetch(`${PDF_URL}?idd=${pdfInfo.idd}`);
    if (!pdfRes.ok) {
      await markChecked(adminClient, documentId, "not_found");
      return jsonResponse({ status: "not_found", stage: "pdf_fetch", citace }, 200);
    }
    const pdfBuf = await pdfRes.arrayBuffer();

    let text: string;
    let pagesScanned = 0;
    let totalPages = 0;
    try {
      const extracted = await extractDuvodovaZprava(pdfBuf);
      text = extracted.text;
      pagesScanned = extracted.pagesScanned;
      totalPages = extracted.totalPages;
    } catch (e) {
      await markChecked(adminClient, documentId, "error");
      return jsonResponse({ status: "error", stage: "pdf_parse", citace, detail: String(e) }, 200);
    }

    if (!text || text.length < 200) {
      await markChecked(adminClient, documentId, "not_found");
      return jsonResponse({ status: "not_found", stage: "dz_heading", citace, pdfBytes: pdfBuf.byteLength, pagesScanned, totalPages });
    }

    let truncated = false;
    if (text.length > MAX_CHARS_TOTAL) {
      text = text.slice(0, MAX_CHARS_TOTAL);
      truncated = true;
    }

    const chunks = splitIntoChunks(text);

    const { data: docRow, error: docErr } = await adminClient
      .from("documents")
      .upsert(
        {
          source_id: sourceId,
          doc_type: "duvodova_zprava",
          external_id: `dz-${citace}`,
          title: `Duvodova zprava k zakonu c. ${citace} Sb.`,
          url: pdfInfo.tisktUrl,
          status: "platny",
          is_current: true,
          explains_document_id: law.id,
          embed_priority: law.embed_priority ?? 0,
        },
        { onConflict: "source_id,external_id" }
      )
      .select("id")
      .single();
    if (docErr || !docRow) {
      await markChecked(adminClient, documentId, "error");
      return jsonResponse({ error: "document_upsert_failed", detail: docErr?.message }, 500);
    }

    await adminClient.from("chunks").delete().eq("document_id", docRow.id);

    const chunkRows = chunks.map((content, i) => ({
      document_id: docRow.id,
      chunk_index: i,
      heading: i === 0 ? "Duvodova zprava" : null,
      content,
    }));
    const { error: chunkErr } = await adminClient.from("chunks").insert(chunkRows);
    if (chunkErr) {
      return jsonResponse({ error: "chunk_insert_failed", detail: chunkErr.message }, 500);
    }

    await markChecked(adminClient, documentId, "found");

    return jsonResponse({
      status: "found",
      citace,
      tisk: `${hist.t}/${hist.o}`,
      chunks_saved: chunkRows.length,
      text_length: text.length,
      truncated,
      pagesScanned,
      totalPages,
    });
  } catch (e) {
    return jsonResponse({ error: String(e) }, 500);
  }
});
