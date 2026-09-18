import { createClient } from "jsr:@supabase/supabase-js@2";

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
    const callerId = userData.user.id;

    const adminClient = createClient(supabaseUrl, serviceKey);

    const body = await req.json().catch(() => ({}));
    const targetUserId: string | null = body?.target_user_id ?? null;

    let userIdToDelete = callerId;

    // Admin-initiated deletion of a DIFFERENT user (used from the
    // Administrace panel). Self-deletion (no target_user_id, or
    // target_user_id === callerId) is always allowed for the caller's own
    // account; deleting someone else's account requires the caller to be
    // an admin, checked server-side (never trust a client-supplied flag).
    if (targetUserId && targetUserId !== callerId) {
      const { data: callerProfile, error: callerProfileErr } = await adminClient
        .from("profiles")
        .select("is_admin")
        .eq("id", callerId)
        .single();
      if (callerProfileErr || !callerProfile?.is_admin) {
        return jsonResponse({ error: "forbidden", message: "Jen správce může mazat účty jiných uživatelů." }, 403);
      }
      // Prevent deleting other admins through this endpoint (avoids a stray
      // click removing the only admin account).
      const { data: targetProfile, error: targetProfileErr } = await adminClient
        .from("profiles")
        .select("is_admin")
        .eq("id", targetUserId)
        .single();
      if (targetProfileErr || !targetProfile) {
        return jsonResponse({ error: "not_found", message: "Uživatel nenalezen." }, 404);
      }
      if (targetProfile.is_admin) {
        return jsonResponse({ error: "forbidden", message: "Účet správce nelze smazat touto cestou." }, 403);
      }
      userIdToDelete = targetUserId;
    }

    // Best-effort: explicitly remove the encrypted Gemini key from Vault
    // first (not covered by the FK cascade below, since vault.secrets isn't
    // linked by a declared foreign key from profiles).
    try {
      await adminClient.rpc("delete_user_gemini_key", { p_user_id: userIdToDelete });
    } catch {
      // non-fatal - proceed with account deletion regardless
    }

    // Deleting the auth user cascades (ON DELETE CASCADE) to profiles,
    // usage_log, and predpis_requests rows for this user - confirmed via
    // pg_constraint before building this.
    const { error: delErr } = await adminClient.auth.admin.deleteUser(userIdToDelete);
    if (delErr) {
      return jsonResponse({ error: "delete_failed", message: delErr.message }, 500);
    }

    return jsonResponse({ success: true });
  } catch (e) {
    return jsonResponse({ error: String(e) }, 500);
  }
});
