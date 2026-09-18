# backend-backup

Manual snapshot of the ParagrAlf backend that otherwise exists only inside Supabase's own dashboard: the 23 Edge Functions' source code (`edge-functions/<slug>/`) and a readable dump of both Supabase projects' database schema, RLS policies, functions, and triggers (`schema/`). Supabase's free plan has no built-in project backups, so before this snapshot none of this was versioned anywhere — see project audit item K-02.

This is a point-in-time snapshot taken 2026-09-18, **not auto-updated**. It will drift from the live state as functions/schema change in Supabase, so treat it as "something to diff against and restore from," not a live mirror. It should be refreshed by hand occasionally (or eventually by an automated job — not built as part of this task) whenever meaningful backend changes are made.
