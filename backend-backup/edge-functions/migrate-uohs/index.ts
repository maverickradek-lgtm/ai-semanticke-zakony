// Temporary one-off migration function (2026-08-04), migration completed and this is now retired.
Deno.serve(() => new Response(JSON.stringify({ error: "retired" }), { status: 410, headers: { "Content-Type": "application/json" } }));
