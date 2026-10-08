"""
Overeni + uklid Supabase pro predpisy kopirovane do RESHARDU (Radek 2026-10-08).

Tenke obalovani verify_and_cleanup_zakony_supabase.py (bezpecnostni logika -
pocet chunku, md5 obsahu, FK odkazy, limit mazani - zustava BEZE ZMENY).
Meni se jen:
- cilove shardy = reshardy (NEON_RESHARD_0N_DB_URL) misto 4 puvodnich,
- obsah v reshardech je OCISTENY od HTML, proto se md5 na strane Supabase
  pocita z clean_html(obsah) (stejna funkce, jakou psal migrate_zakony_to_reshard),
- cteni z Neonu se omezi jen na kandidaty (ne cely shard).
"""

import hashlib
import os

for _k in ("DO1997", "1998_2007", "2008_2020", "2021_DOSUD"):
    os.environ.setdefault("NEON_ZAKONY_" + _k + "_DB_URL", "unused")

import verify_and_cleanup_zakony_supabase as v
from reshard_and_clean_zakony import clean_html

v.NEON_URLS = {
    "reshard-0" + str(i): os.environ["NEON_RESHARD_0" + str(i) + "_DB_URL"]
    for i in range(1, 10)
    if os.environ.get("NEON_RESHARD_0" + str(i) + "_DB_URL")
}

_candidate_ids = []
_orig_candidates = v.get_migrated_candidates


def _candidates():
    rows = _orig_candidates()
    _candidate_ids[:] = list(rows)
    return rows


def _neon_present(shard_key):
    """{doc_id: (pocet_chunku, md5)} jen pro kandidaty, kteri maji v shardu >=1 chunk."""
    if not _candidate_ids:
        return {}
    conn = v.db_connect(v.NEON_URLS[shard_key])
    try:
        with conn.cursor() as cur:
            cur.execute(
                "select d.id, count(c.id), "
                "md5(string_agg(coalesce(c.content, ''), '|' order by c.chunk_index)) "
                "from documents d join chunks c on c.document_id = d.id "
                "where d.id = any(%s::uuid[]) group by d.id",
                (_candidate_ids,),
            )
            return {str(i): (cnt, h) for i, cnt, h in cur.fetchall()}
    finally:
        conn.close()


def _supabase_hashes(doc_ids):
    counts = {}
    hashes = {}
    if not doc_ids:
        return counts, hashes
    chunks_by_doc = {}
    ids = list(doc_ids)
    for i in range(0, len(ids), 20):
        batch = ids[i : i + 20]
        offset = 0
        while True:
            rows = v.sb_get(
                "chunks",
                {
                    "select": "id,document_id,chunk_index,content",
                    "document_id": "in.(" + ",".join(batch) + ")",
                    "order": "id.asc",
                    "limit": "1000",
                    "offset": str(offset),
                },
            )
            if not rows:
                break
            for r in rows:
                content = r.get("content") or ""
                if not content:
                    content = v._fetch_storage_content(r["id"]) or ""
                chunks_by_doc.setdefault(r["document_id"], []).append(
                    (r.get("chunk_index") or 0, clean_html(content))
                )
            offset += 1000
            if len(rows) < 1000:
                break
    for doc_id, parts in chunks_by_doc.items():
        parts.sort(key=lambda p: p[0])
        counts[doc_id] = len(parts)
        hashes[doc_id] = hashlib.md5("|".join(p[1] for p in parts).encode("utf-8")).hexdigest()
    return counts, hashes


v.get_migrated_candidates = _candidates
v.get_neon_present = _neon_present
v.get_supabase_chunk_hashes = _supabase_hashes

if __name__ == "__main__":
    v.main()
