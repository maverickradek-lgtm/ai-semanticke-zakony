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

const ESBIRKA_API_BASE = "https://api.e-sbirka.gov.cz";
const CITATION_RE = /(\d+[a-z]?)\s*\/\s*(\d{4})/;

// Stejne id jako radek v hlavni Supabase tabulce "sources" (code='esbirka').
const SOURCE_ID = "5804ffaa-c5c6-4f35-b5c7-48da040ed457";

// Druhy Supabase projekt (NS/NSS judikatura + UOHS) - anon klic je verejny,
// stejny jako pouziva edge funkce ai-query.
const NS_SUPABASE_URL = "https://ejjkyrprdzetxfwlkboa.supabase.co";
const NS_ANON_KEY =
  "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVqamt5cnByZHpldHhmd2xrYm9hIiwicm9sZSI6ImFub24iLCJpYXQiOjE3ODQzOTczMDIsImV4cCI6MjA5OTk3MzMwMn0.7owC8Bdp6hSsiWZ8EvUnm2lHHxMAeJnxRLvvEzBjzlA";

const SHARDS = ["do1997", "1998_2007", "2008_2020", "2021_dosud"] as const;
type Shard = typeof SHARDS[number];

function bucketForYear(rok: number | null): Shard {
  if (rok === null || Number.isNaN(rok)) return "2021_dosud";
  if (rok <= 1997) return "do1997";
  if (rok <= 2007) return "1998_2007";
  if (rok <= 2020) return "2008_2020";
  return "2021_dosud";
}

function parsePredpis(externalId: string): { cislo: number | null; rok: number | null } {
  const m = externalId.match(/^(\d+)\/(\d{4})/);
  if (!m) return { cislo: null, rok: null };
  return { cislo: parseInt(m[1], 10), rok: parseInt(m[2], 10) };
}

async function getNeonSql(
  adminClient: ReturnType<typeof createClient>,
  rpcName: string,
  rpcArgs: Record<string, unknown> = {}
): Promise<ReturnType<typeof postgres> | null> {
  const { data: url, error } = await adminClient.rpc(rpcName, rpcArgs);
  if (error || !url || typeof url !== "string") return null;
  return postgres(url, { max: 1, idle_timeout: 10, connect_timeout: 6, prepare: false });
}

function guessDocType(nazev: string): string {
  const n = (nazev || "").toLowerCase();
  if (n.startsWith("vyhláška")) return "vyhlaska";
  if (n.startsWith("nař�mzení")) return "narizeni";
  if (n.startsWith("opatření")) return "opatreni";
  if (n.startsWith("sdělení")) return "jiny_predpis";
  if (n.startsWith("zákon")) return "zakon";
  return "zakon";
}

function headingFromEli(eli: string, fallback: string): string {
  const m = eli.match(/\/par_(\d+[a-z]?)(?:\/|$)/);
  return m ? `§ ${m[1]}` : fallback;
}

function plainText(xhtml: string): string {
  return (xhtml || "").replace(/<[^>]+>/g, "").replace(/\s+/g, " ").trim();
}

// ---------------------------------------------------------------------------
// Predpisy EU (narizeni Rady/Komise/EP) - EUR-Lex, samostatna vetev mimo
// e-Sbirku (ktera unijni predpisy vubec nezna a vraci na ne chybu).
// ---------------------------------------------------------------------------

// Diakritiku pisu jako \uXXXX escape sekvence primo v regexu/retezcich, aby
// se predeslo problemum s prenosem UTF-8 znaku pres nastroj (opakovane
// zjisteny mojibake bug pri psani ceskych znaku do JS retezce timto kanalem).
const EU_REG_RE = new RegExp(
  "nařízení\\s+(?:evropského parlamentu a rady|rady|komise)?[^0-9(]{0,20}\\((EU|ES|EHS|Euratom)\\)\\s*č\\.?\\s*(\\d{1,4})\\s*/\\s*(\\d{2,4})",
  "i"
);

function celexForEuRegulation(num: string, yearRaw: string): string {
  const yearNum = yearRaw.length === 2
    ? (parseInt(yearRaw, 10) <= 30 ? 2000 + parseInt(yearRaw, 10) : 1900 + parseInt(yearRaw, 10))
    : parseInt(yearRaw, 10);
  const paddedNum = num.padStart(4, "0");
  return `3${yearNum}R${paddedNum}`;
}

function euHtmlToPlainText(fragment: string): string {
  let t = fragment;
  t = t.replace(/<(script|style)[^>]*>[\s\S]*?<\/\1>/gi, " ");
  t = t.replace(/<(br|\/p|\/div|\/li|\/tr)\s*\/?>/gi, "\n");
  t = t.replace(/<[^>]+>/g, " ");
  t = t
    .replace(/&nbsp;/g, " ")
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'");
  t = t.replace(/[ \t]+/g, " ").replace(/\n[ \t]*\n+/g, "\n").trim();
  return t;
}

function extractEuArticles(html: string): { heading: string; content: string }[] {
  const artRe = /<div class="eli-subdivision" id="(art_[^"]+)">/g;
  const positions: { idx: number; id: string }[] = [];
  let m: RegExpExecArray | null;
  while ((m = artRe.exec(html)) !== null) positions.push({ idx: m.index, id: m[1] });
  if (positions.length === 0) return [];
  const results: { heading: string; content: string }[] = [];
  for (let i = 0; i < positions.length; i++) {
    const start = positions[i].idx;
    const end = i + 1 < positions.length ? positions[i + 1].idx : html.length;
    const segment = html.slice(start, end);
    const text = euHtmlToPlainText(segment);
    if (!text) continue;
    const artNum = positions[i].id.replace(/^art_/, "");
    results.push({ heading: `Článek ${artNum}`, content: text });
  }
  return results;
}

const EU_EMBED_MODEL = "gemini-embedding-001";
const EU_EMBED_DIM = 256;

async function embedText(text: string, geminiKey: string): Promise<number[] | null> {
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const resp = await fetch(
        `https://generativelanguage.googleapis.com/v1beta/models/${EU_EMBED_MODEL}:embedContent`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json", "x-goog-api-key": geminiKey },
          body: JSON.stringify({
            content: { parts: [{ text: text.slice(0, 8000) }] },
            taskType: "RETRIEVAL_DOCUMENT",
            outputDimensionality: EU_EMBED_DIM,
          }),
        }
      );
      if (resp.status === 429) {
        await new Promise((r) => setTimeout(r, 4000 * (attempt + 1)));
        continue;
      }
      if (!resp.ok) return null;
      const j = await resp.json();
      const vec = j?.embedding?.values;
      return Array.isArray(vec) ? vec : null;
    } catch {
      await new Promise((r) => setTimeout(r, 2000 * (attempt + 1)));
    }
  }
  return null;
}

async function handleEuRegulation(
  adminClient: ReturnType<typeof createClient>,
  requestId: string,
  queryText: string,
  match: RegExpMatchArray,
  callerId: string
): Promise<Response> {
  const acronym = match[1].toUpperCase();
  const num = match[2];
  const yearRaw = match[3];
  const celex = celexForEuRegulation(num, yearRaw);

  const eurLexUrl = `https://eur-lex.europa.eu/legal-content/CS/TXT/HTML/?uri=CELEX:${celex}`;
  let html: string;
  try {
    const res = await fetch(eurLexUrl, { headers: { "User-Agent": "Mozilla/5.0 (compatible; ParagrAlfBot/1.0)" } });
    if (!res.ok) {
      await adminClient.from("predpis_requests").update({
        status: "not_found",
        note: `Jde o předpis EU (nařízení (${acronym}) č. ${num}/${yearRaw}, CELEX ${celex}), ale EUR-Lex vrátil stav ${res.status}. Zkuste to prosím později nebo zkontrolujte číslo/rok.`,
        processed_at: new Date().toISOString(),
      }).eq("id", requestId);
      return jsonResponse({ success: true, result: "not_found", celex });
    }
    html = await res.text();
  } catch (e) {
    await adminClient.from("predpis_requests").update({
      status: "not_found",
      note: `Jde o předpis EU (CELEX ${celex}), ale stažení z EUR-Lex selhalo: ${String(e).slice(0, 200)}.`,
      processed_at: new Date().toISOString(),
    }).eq("id", requestId);
    return jsonResponse({ success: true, result: "not_found", celex });
  }

  const articles = extractEuArticles(html);
  let groups: { heading: string; content: string }[];
  let usedFallback = false;
  if (articles.length > 0) {
    groups = articles;
  } else {
    const bodyMatch = html.match(/<body[^>]*>([\s\S]*)<\/body>/i);
    const wholeText = euHtmlToPlainText(bodyMatch ? bodyMatch[1] : html);
    if (!wholeText) {
      await adminClient.from("predpis_requests").update({
        status: "not_found",
        note: `Jde o předpis EU (CELEX ${celex}), na EUR-Lex byl nalezen, ale nepodařilo se z něj sestavit žádný text.`,
        processed_at: new Date().toISOString(),
      }).eq("id", requestId);
      return jsonResponse({ success: true, result: "not_found", celex });
    }
    groups = [{ heading: "Text předpisu", content: wholeText.slice(0, 100000) }];
    usedFallback = true;
  }

  const sql = await getNeonSql(adminClient, "get_neon_mf_db_url");
  if (!sql) {
    return jsonResponse({ error: "neon_unavailable", message: "Nepodařilo se připojit k Neon projektu pro MF/EU předpisy." }, 502);
  }

  const title = queryText.trim().split("\n")[0].slice(0, 500);
  const docUrl = `https://eur-lex.europa.eu/legal-content/CS/TXT/?uri=CELEX:${celex}`;

  const { data: geminiKey } = await adminClient.rpc("get_user_gemini_key", { p_user_id: callerId });

  let docId: string;
  let embeddedCount = 0;
  try {
    const existingRows = await sql`
      select id from documents where source = 'predpis_eu' and external_id = ${celex}
    `;
    if (existingRows.length > 0) {
      docId = existingRows[0].id;
      await sql`delete from chunks where document_id = ${docId}`;
      await sql`
        update documents set title = ${title}, url = ${docUrl}, pdf_url = ${docUrl},
          is_current = true, updated_at = now()
        where id = ${docId}
      `;
    } else {
      const inserted = await sql`
        insert into documents (source, series, external_id, title, url, pdf_url, is_current)
        values ('predpis_eu', 'Předpisy EU', ${celex}, ${title}, ${docUrl}, ${docUrl}, true)
        returning id
      `;
      docId = inserted[0].id;
    }

    for (let i = 0; i < groups.length; i++) {
      const g = groups[i];
      let embeddingLiteral: string | null = null;
      if (typeof geminiKey === "string" && geminiKey) {
        const vec = await embedText(g.content, geminiKey);
        if (vec) {
          embeddingLiteral = `[${vec.join(",")}]`;
          embeddedCount++;
        }
      }
      if (embeddingLiteral) {
        await sql`
          insert into chunks (document_id, chunk_index, heading, content, embedding)
          values (${docId}, ${i}, ${g.heading}, ${g.content}, ${embeddingLiteral}::vector)
        `;
      } else {
        await sql`
          insert into chunks (document_id, chunk_index, heading, content)
          values (${docId}, ${i}, ${g.heading}, ${g.content})
        `;
      }
    }
  } finally {
    await sql.end({ timeout: 3 });
  }

  const embedNote = embeddedCount === groups.length
    ? "Všechny části jsou již vyhledatelné."
    : embeddedCount > 0
      ? `${embeddedCount}/${groups.length} částí je vyhledatelných, zbytek bude doplněn při dalším běhu embeddingu.`
      : "Vyhledatelné bude až po nejbližším běhu embeddingu (chybí Gemini klíč nebo selhalo volání).";

  await adminClient.from("predpis_requests").update({
    status: "fetched",
    note: `Naimportováno z EUR-Lex jako předpis EU (CELEX ${celex}), ${groups.length} ${usedFallback ? "částí" : "článků"} - "${title}". ${embedNote}`,
    processed_at: new Date().toISOString(),
  }).eq("id", requestId);

  return jsonResponse({ success: true, result: "imported", title, chunks: groups.length, doc_id: docId, celex, embedded: embeddedCount });
}

// ---------------------------------------------------------------------------

// Zname obecne predpony typu predpisu, ktere uzivatele casto pisou pred
// skutecny identifikator ("metodika GFŘ-D-72" -> hledej "GFŘ-D-72") - kdyz je
// odstranime, hledani v ostatnich databazich je mnohem presnejsi.
const GENERIC_PREFIXES = new Set([
  "metodika", "metodiky", "pokyn", "pokyny", "zakon", "zákon", "vyhlaska", "vyhláška",
  "narizeni", "nař�mzení", "opatreni", "opatření", "sdeleni", "sdělení",
  "predpis", "předpis", "judikat", "judikát", "rozhodnuti", "rozhodnutí",
]);

function extractSearchTerm(queryText: string): string {
  const words = queryText
    .trim()
    .split(/\s+/)
    .filter((w) => !GENERIC_PREFIXES.has(w.toLowerCase().replace(/[.,]/g, "")));
  return words.length > 0 ? words.join(" ") : queryText.trim();
}

type FoundElsewhere = { source: string; title: string; url: string | null };

// Prohleda vsechny ostatni databaze podle nazvu (title ILIKE %term%) - hlavni
// Supabase, druhy Supabase projekt (NS/NSS/UOHS), Neon MF metodiky, Neon FS
// metodiky a vsechny 4 Neon zakony shardy. Vraci
// prvni shodu, nebo null.
async function searchOtherDatabases(
  adminClient: ReturnType<typeof createClient>,
  term: string
): Promise<FoundElsewhere | null> {
  const pattern = `%${term}%`;

  try {
    const { data } = await adminClient.from("documents").select("title, url, doc_type").ilike("title", pattern).limit(1);
    if (data && data.length > 0) {
      return { source: "hlavní databáze (Supabase)", title: data[0].title, url: data[0].url ?? null };
    }
  } catch { /* ignore, zkus dal */ }

  try {
    const nsClient = createClient(NS_SUPABASE_URL, NS_ANON_KEY);
    const { data } = await nsClient.from("documents").select("title, url, doc_type").ilike("title", pattern).limit(1);
    if (data && data.length > 0) {
      return { source: "judikatura NS/NSS/ÚOHS", title: data[0].title, url: data[0].url ?? null };
    }
  } catch { /* ignore */ }

  try {
    const sql = await getNeonSql(adminClient, "get_neon_mf_db_url");
    if (sql) {
      try {
        const rows = await sql`select title, url from documents where title ilike ${pattern} limit 1`;
        if (rows.length > 0) return { source: "Metodika MF", title: rows[0].title, url: rows[0].url ?? null };
      } finally {
        await sql.end({ timeout: 2 });
      }
    }
  } catch { /* ignore */ }

  try {
    const sql = await getNeonSql(adminClient, "get_neon_fs_db_url");
    if (sql) {
      try {
        const rows = await sql`select title, url from documents where title ilike ${pattern} limit 1`;
        if (rows.length > 0) return { source: "Metodika FS (Pokyny D)", title: rows[0].title, url: rows[0].url ?? null };
      } finally {
        await sql.end({ timeout: 2 });
      }
    }
  } catch { /* ignore */ }

  try {
    const sql = await getNeonSql(adminClient, "get_neon_celni_db_url");
    if (sql) {
      try {
        const rows = await sql`select title, url from documents where title ilike ${pattern} limit 1`;
        if (rows.length > 0) return { source: "Metodika Celn�: správy", title: rows[0].title, url: rows[0].url ?? null };
      } finally {
        await sql.end({ timeout: 2 });
      }
    }
  } catch { /* ignore */ }

  for (const shard of SHARDS) {
    try {
      const sql = await getNeonSql(adminClient, "get_neon_zakony_db_url", { p_shard: shard });
      if (sql) {
        try {
          const rows = await sql`select title, url from documents where title ilike ${pattern} and is_current = true limit 1`;
          if (rows.length > 0) return { source: `Zákony (Neon, shard ${shard})`, title: rows[0].title, url: rows[0].url ?? null };
        } finally {
          await sql.end({ timeout: 2 });
        }
      }
    } catch { /* ignore, zkus dalsi shard */ }
  }

  return null;
}

async function tryFallbackSearch(
  adminClient: ReturnType<typeof createClient>,
  queryText: string
): Promise<FoundElsewhere | null> {
  const primaryTerm = extractSearchTerm(queryText);
  let found = await searchOtherDatabases(adminClient, primaryTerm);
  if (!found && primaryTerm !== queryText.trim()) {
    found = await searchOtherDatabases(adminClient, queryText.trim());
  }
  if (!found) {
    // posledni pokus - nejdelsi jednotlive slovo z dotazu (pomaha u preklepu
    // nebo kdyz je presna fraze jinak formulovana nez skutecny nazev)
    const words = primaryTerm.split(/\s+/).filter((w) => w.length >= 3);
    if (words.length > 0) {
      const longest = words.reduce((a, b) => (b.length > a.length ? b : a));
      if (longest !== primaryTerm) {
        found = await searchOtherDatabases(adminClient, longest);
      }
    }
  }
  return found;
}

Deno.serve(async (req: Request) => {
  if (req.method === "OPTIONS") return new Response("ok", { headers: corsHeaders });
  if (req.method !== "POST") return jsonResponse({ error: "method_not_allowed" }, 405);

  try {
    const authHeader = req.headers.get("Authorization") ?? "";
    const supabaseUrl = Deno.env.get("SUPABASE_URL")!;
    const anonKey = Deno.env.get("SUPABASE_ANON_KEY")!;
    const serviceKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

    const userClient = createClient(supabaseUrl, anonKey, {
      global: { headers: { Authorization: authHeader } },
    });
    const { data: userData, error: userErr } = await userClient.auth.getUser();
    if (userErr || !userData?.user) return jsonResponse({ error: "unauthorized" }, 401);
    const callerId = userData.user.id;

    const adminClient = createClient(supabaseUrl, serviceKey);

    const { data: callerProfile } = await adminClient.from("profiles").select("is_admin").eq("id", callerId).single();
    if (!callerProfile?.is_admin) {
      return jsonResponse({ error: "forbidden", message: "Jen správce může spouštět import předpisu." }, 403);
    }

    const body = await req.json().catch(() => ({}));
    const requestId: string | null = body?.request_id ?? null;
    if (!requestId) return jsonResponse({ error: "bad_request", message: "Chybí request_id." }, 400);

    const { data: reqRow, error: reqErr } = await adminClient
      .from("predpis_requests")
      .select("id, query_text, status")
      .eq("id", requestId)
      .single();
    if (reqErr || !reqRow) return jsonResponse({ error: "not_found", message: "Žádost nenalezena." }, 404);

    // Nejdriv zkontrolovat, jestli nejde o unijni predpis (nařízení Rady/
    // Komise/EP) - ty e-Sbírka vubec nezna, musi jit pres EUR-Lex.
    const euMatch = EU_REG_RE.exec(reqRow.query_text);
    if (euMatch) {
      return await handleEuRegulation(adminClient, requestId, reqRow.query_text, euMatch, callerId);
    }

    const m = CITATION_RE.exec(reqRow.query_text);

    async function resolveNotFound(reason: string): Promise<Response> {
      const found = await tryFallbackSearch(adminClient, reqRow.query_text);
      if (found) {
        await adminClient.from("predpis_requests").update({
          status: "fetched",
          note: `Nalezeno v jiné databázi: ${found.source} - "${found.title}"${found.url ? " (" + found.url + ")" : ""}. Již je v aplikaci vyhledávatelné.`,
          processed_at: new Date().toISOString(),
        }).eq("id", requestId);
        return jsonResponse({ success: true, result: "found_elsewhere", source: found.source, title: found.title, url: found.url });
      }
      await adminClient.from("predpis_requests").update({
        status: "not_found",
        note: `${reason} A nenalezeno ani v ostatních databázích (metodiky MF/FS/Celní správa, judikatura NS/NSS/ÚOHS, zákony v Neonu).`,
        processed_at: new Date().toISOString(),
      }).eq("id", requestId);
      return jsonResponse({ success: true, result: "not_found" });
    }

    if (!m) {
      return await resolveNotFound("Nepodařilo se rozpoznat číslo/rok předpisu (očekáváno např. 134/2016).");
    }
    const cislo = m[1];
    const rok = m[2];

    const { data: apiKey, error: keyErr } = await adminClient.rpc("get_esbirka_api_key");
    if (keyErr || !apiKey) {
      return jsonResponse({ error: "no_api_key", message: "Klíč k e-Sbírka REST API není k dispozici." }, 500);
    }

    const staleUrlBase = `/sb/${rok}/${cislo}`;
    const metaRes = await fetch(`${ESBIRKA_API_BASE}/dokumenty-sbirky/${encodeURIComponent(staleUrlBase)}`, {
      headers: { "esel-api-access-key": apiKey },
    });

    if (metaRes.status === 404) {
      return await resolveNotFound("Ani v oficiálním rejstříku e-Sbírky nebyl takový předpis nalezen.");
    }
    if (!metaRes.ok) {
      // Drive tady zustavala zadost trvale ve stavu "čeká" (chyba se jen
      // vratila klientovi jako 502, ale radek se do DB nikdy nezapsal).
      // Ted uz i tenhle pripad zaznamename, aby admin videl vysledek.
      return await resolveNotFound(`e-Sbírka API vrátilo stav ${metaRes.status} při pokusu o import.`);
    }
    const meta = await metaRes.json();

    const staleUrl: string = meta.staleUrl;
    const docType = guessDocType(meta.nazev ?? "");
    const externalId: string = meta.kodDokumentuSbirky ?? `${cislo}/${rok} Sb.`;
    const title: string = meta.nazev ?? externalId;
    const effectiveDate: string | null = meta.datumUcinnostiOd ?? null;

    const encodedStaleUrl = encodeURIComponent(staleUrl);
    let allFragments: any[] = [];
    let pocetStranek = 1;
    for (let page = 0; page < pocetStranek; page++) {
      const fragRes = await fetch(
        `${ESBIRKA_API_BASE}/dokumenty-sbirky/${encodedStaleUrl}/fragmenty?cisloStranky=${page}`,
        { headers: { "esel-api-access-key": apiKey } },
      );
      if (!fragRes.ok) {
        return jsonResponse({ error: "esbirka_unavailable", message: `Nepodařilo se stáhnout obsah (stránka ${page}, stav ${fragRes.status}).` }, 502);
      }
      const fragJson = await fragRes.json();
      pocetStranek = fragJson.pocetStranek ?? 1;
      allFragments = allFragments.concat((fragJson.seznam ?? []).filter(Boolean));
    }

    function groupByParagraf(): { heading: string; content: string }[] {
      const groups: { heading: string; parts: string[] }[] = [];
      let current: { heading: string; parts: string[] } | null = null;
      for (const f of allFragments) {
        if (f.kodTypuFragmentu === "Paragraf") {
          if (current) groups.push(current);
          current = { heading: headingFromEli(f.eli, f.zkracenaCitace ?? "§ ?"), parts: [f.xhtml ?? ""] };
        } else if (current) {
          current.parts.push(f.xhtml ?? "");
        }
      }
      if (current) groups.push(current);
      return groups.map((g) => ({ heading: g.heading, content: g.parts.join(" ") }));
    }

    function groupByHeadingFragments(): { heading: string; content: string }[] {
      const headingCounts: Record<string, number> = {};
      for (const f of allFragments) {
        if (typeof f.kodTypuFragmentu === "string" && /^DD_Nadpis_\d+$/.test(f.kodTypuFragmentu)) {
          headingCounts[f.kodTypuFragmentu] = (headingCounts[f.kodTypuFragmentu] ?? 0) + 1;
        }
      }
      const candidateLevels = Object.keys(headingCounts)
        .filter((k) => headingCounts[k] > 1)
        .sort((a, b) => {
          const na = parseInt(a.replace("DD_Nadpis_", ""), 10);
          const nb = parseInt(b.replace("DD_Nadpis_", ""), 10);
          return na - nb;
        });
      const headingType = candidateLevels[0];
      if (!headingType) return [];

      const groups: { heading: string; parts: string[] }[] = [];
      let current: { heading: string; parts: string[] } | null = null;
      let preamble: string[] = [];
      for (const f of allFragments) {
        if (f.kodTypuFragmentu === headingType) {
          if (current) groups.push(current);
          current = { heading: plainText(f.xhtml) || "Část", parts: [] };
        } else if (
          f.kodTypuFragmentu === "Prosty_Text" ||
          f.kodTypuFragmentu === "Odrazka_3" ||
          (typeof f.kodTypuFragmentu === "string" && /^DD_Nadpis_\d+$/.test(f.kodTypuFragmentu))
        ) {
          const text = f.xhtml ?? "";
          if (current) current.parts.push(text);
          else preamble.push(text);
        }
      }
      if (current) groups.push(current);
      const result = groups.map((g) => ({ heading: g.heading, content: g.parts.join(" ") }));
      if (preamble.length && result.length) {
        result[0] = { heading: result[0].heading, content: preamble.join(" ") + " " + result[0].content };
      }
      return result;
    }

    let groups = groupByParagraf();
    let usedFallback = false;
    if (groups.length === 0) {
      groups = groupByHeadingFragments();
      usedFallback = true;
    }

    if (groups.length === 0) {
      return jsonResponse({ error: "no_content", message: "Předpis byl nalezen, ale nepodařilo se z něj sestavit žádný text." }, 500);
    }

    const { cislo: predpisCislo, rok: predpisRok } = parsePredpis(externalId);
    const shard = bucketForYear(predpisRok);
    const sql = await getNeonSql(adminClient, "get_neon_zakony_db_url", { p_shard: shard });
    if (!sql) {
      return jsonResponse({ error: "neon_unavailable", message: `Nepodařilo se připojit k Neon shardu ${shard}.` }, 502);
    }

    let docId: string;
    try {
      const existingRows = await sql`
        select id from documents where source_id = ${SOURCE_ID} and external_id = ${externalId}
      `;
      const docUrl = `https://e-sbirka.cz${staleUrl}`;

      if (existingRows.length > 0) {
        docId = existingRows[0].id;
        await sql`delete from chunks where document_id = ${docId}`;
        await sql`
          update documents set
            doc_type = ${docType}, title = ${title}, url = ${docUrl},
            status = 'platny', is_current = true, embed_priority = 300,
            effective_date = ${effectiveDate}, predpis_cislo = ${predpisCislo},
            predpis_rok = ${predpisRok}, updated_at = now()
          where id = ${docId}
        `;
      } else {
        docId = crypto.randomUUID();
        await sql`
          insert into documents (
            id, source_id, external_id, doc_type, title, url, status,
            is_current, embed_priority, effective_date, predpis_cislo, predpis_rok
          ) values (
            ${docId}, ${SOURCE_ID}, ${externalId}, ${docType}, ${title}, ${docUrl}, 'platny',
            true, 300, ${effectiveDate}, ${predpisCislo}, ${predpisRok}
          )
        `;
      }

      const chunkRows = groups.map((g, i) => [crypto.randomUUID(), docId, i, g.heading, g.content]);
      if (chunkRows.length > 0) {
        await sql`
          insert into chunks (id, document_id, chunk_index, heading, content)
          values ${sql(chunkRows)}
        `;
      }
    } finally {
      await sql.end({ timeout: 3 });
    }

    await adminClient.from("predpis_requests").update({
      status: "fetched",
      note: `Naimportováno ${groups.length} ${usedFallback ? "částí" : "paragrafů (§)"} - "${title}" (Neon shard ${shard}). Vyhledávatelné bude po nejbližším běhu embeddingu.`,
      processed_at: new Date().toISOString(),
    }).eq("id", requestId);

    return jsonResponse({ success: true, result: "imported", title, chunks: groups.length, doc_id: docId, shard, usedFallback });
  } catch (e) {
    return jsonResponse({ error: String(e) }, 500);
  }
});
