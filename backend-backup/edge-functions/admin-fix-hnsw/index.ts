// One-off admin fix, already applied 2026-09-03 (hnsw.ef_search=100 on all 4 zakony shards' match_chunks()).
// Disabled after use - do not re-enable without review.
Deno.serve(async (_req: Request) => {
  return new Response(JSON.stringify({ disabled: true, note: "one-off fix already applied 2026-09-03" }), { status: 410 });
});
