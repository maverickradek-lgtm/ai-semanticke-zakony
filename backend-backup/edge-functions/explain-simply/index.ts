import { createClient } from "jsr:@supabase/supabase-js@2";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
};

const CHAT_MODEL = "gemini-flash-lite-latest";
const GEN_TIMEOUT_MS = 25_000;
const TRIAL_QUERY_LIMIT = 20;

function jsonResponse(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { ...corsHeaders, "Content-Type": "application/json" },
  });
}

async function fetchWithTimeout(url: string, options: RequestInit, timeoutMs: number): Promise<Response> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, { ...options, signal: controller.signal });
  } finally {
    clearTimeout(timer);
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
    const userId = userData.user.id;

    const adminClient = createClient(supabaseUrl, serviceKey);

    const { data: profile, error: profileErr } = await adminClient
      .from("profiles")
      .select("status, trial_queries_used")
      .eq("id", userId)
      .single();
    if (profileErr || !profile) {
      return jsonResponse({ error: "not_approved", message: "Váš účet ještě není schválen." }, 403);
    }

    const isApproved = profile.status === "approved";
    const isTrialEligible = profile.status === "pending" && profile.trial_queries_used < TRIAL_QUERY_LIMIT;
    if (!isApproved && !isTrialEligible) {
      const message = profile.status === "pending"
        ? "Vyčerpali jste všech " + TRIAL_QUERY_LIMIT + " zkušebních dotazů. Počkejte prosím na schválení účtu správcem aplikace."
        : "Váš účet ještě není schválen.";
      return jsonResponse({ error: "not_approved", message }, 403);
    }
    const isTrial = !isApproved && isTrialEligible;

    const body = await req.json();
    const answerText: string = (body?.answer_text ?? "").toString().trim();
    const originalQuery: string = (body?.query ?? "").toString().trim();
    if (!answerText) {
      return jsonResponse({ error: "missing_answer_text" }, 400);
    }

    const { data: geminiKey, error: keyErr } = await adminClient.rpc("get_user_gemini_key", {
      p_user_id: userId,
    });
    if (keyErr || !geminiKey) {
      return jsonResponse({ error: "no_gemini_key", message: "Nejprve si v nastavení uložte vlastní Google Gemini API klíč." }, 400);
    }

    const prompt = `Přepiš následující odpověď z právní databáze do jednoduché, srozumitelné běžné češtiny pro člověka bez právního vzdělání. Vysvětli hlavní myšlenku obyčejnými slovy, bez právnického žargonu, stručně (několik vět až krátký odstavec). Nepřidávej žádné nové právní informace, které v textu níže nejsou, a nic si nevymýšlej. Pokud text obsahuje číslované odkazy na zdroje ve tvaru [1], [2] apod., zachovej je tam, kde dává smysl.\n\nPůvodní dotaz uživatele: ${originalQuery || "(neuvedeno)"}\n\nOdpověď k zjednodušení:\n${answerText}`;

    let genRes: Response | null = null;
    try {
      genRes = await fetchWithTimeout(
        `https://generativelanguage.googleapis.com/v1beta/models/${CHAT_MODEL}:generateContent`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json", "x-goog-api-key": geminiKey },
          body: JSON.stringify({ contents: [{ role: "user", parts: [{ text: prompt }] }] }),
        },
        GEN_TIMEOUT_MS
      );
    } catch {
      genRes = null;
    }

    if (!genRes || !genRes.ok) {
      const detail = genRes ? await genRes.text().catch(() => "") : "timeout";
      return jsonResponse({ error: "generation_failed", detail }, 502);
    }

    const genJson = await genRes.json();
    const candidate = genJson?.candidates?.[0];
    const simplified: string = candidate?.content?.parts?.map((p: any) => p.text).join("\n") ?? "";
    const usage = genJson?.usageMetadata ?? {};

    await adminClient.from("usage_log").insert({
      user_id: userId,
      request_type: "chat",
      model: CHAT_MODEL,
      input_tokens: (usage as any).promptTokenCount ?? null,
      output_tokens: (usage as any).candidatesTokenCount ?? null,
      query_preview: "[vysvětlení] " + answerText.slice(0, 180),
    });

    let trialRemaining: number | null = null;
    if (isTrial) {
      const newCount = profile.trial_queries_used + 1;
      await adminClient.from("profiles").update({ trial_queries_used: newCount }).eq("id", userId);
      trialRemaining = Math.max(0, TRIAL_QUERY_LIMIT - newCount);
    }

    return jsonResponse({ simplified, trial_remaining: trialRemaining });
  } catch (e) {
    return jsonResponse({ error: String(e) }, 500);
  }
});
