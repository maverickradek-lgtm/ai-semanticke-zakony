"""
Embedding pending chunku ve 4 Neon shardech zakonu (do1997 / 1998_2007 /
2008_2020 / 2021_dosud) - viz projektova pamet zakony_neon_sharding_plan a
scripts/migrate_zakony_to_neon.py, ktery data do techto shardu kopiruje.

Kazdy shard uz ma z ensure_schema() (viz migrate_zakony_to_neon.py) pripravenou
funkci get_pending_chunks_prioritized(), takze tento skript ji jen vola - stejna
logika prioritizace (embed_priority, has_pending_chunks) jako v hlavni Supabase.

Beh je round-robin pres vsechny 4 shardy (jeden davkovy dotaz z kazdeho, pak
dalsi kolo), aby zadny shard nebyl systematicky odsouvan na konec spolecneho
casoveho/API rozpoctu - zejmena 2021_dosud, ktery dale roste.
"""

import os
import sys
import time
import psycopg2
import requests

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SERVICE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
ADMIN_USER_ID = "2648f5db-bea6-4cac-b490-ad0ec59723df"
GEMINI_API_KEY_OVERRIDE = os.environ.get("GEMINI_API_KEY_OVERRIDE")

EMBED_MODEL = "gemini-embedding-001"
EMBED_DIM = 256
BATCH_PER_SHARD = int(os.environ.get("BATCH_PER_SHARD", "20"))
TIME_BUDGET_SECONDS = int(os.environ.get("TIME_BUDGET_SECONDS", "3000"))

NEON_URLS = {
    "do1997": os.environ["NEON_ZAKONY_DO1997_DB_URL"],
    "1998_2007": os.environ["NEON_ZAKONY_1998_2007_DB_URL"],
    "2008_2020": os.environ["NEON_ZAKONY_2008_2020_DB_URL"],
    "2021_dosud": os.environ["NEON_ZAKONY_2021_DOSUD_DB_URL"],
}

START_TIME = time.time()


def log(*a):
    print(*a)
    sys.stdout.flush()


def time_left():
    return TIME_BUDGET_SECONDS - (time.time() - START_TIME)


def sb_headers():
    return {
        "apikey": SERVICE_KEY,
        "Authorization": f"Bearer {SERVICE_KEY}",
        "Content-Type": "application/json",
    }


def get_admin_gemini_key():
    if GEMINI_API_KEY_OVERRIDE:
        return GEMINI_API_KEY_OVERRIDE
    r = requests.post(
        f"{SUPABASE_URL}/rest/v1/rpc/get_user_gemini_key",
        headers=sb_headers(),
        json={"p_user_id": ADMIN_USER_ID},
        timeout=30,
    )
    r.raise_for_status()
    key = r.json()
    if not key:
        raise RuntimeError("Admin Gemini key neni k dispozici")
    return key


def embed_text(text, gemini_key, retries=3):
    for attempt in range(retries):
        try:
            resp = requests.post(
                f"https://generativelanguage.googleapis.com/v1beta/models/{EMBED_MODEL}:embedContent",
                headers={"Content-Type": "application/json", "x-goog-api-key": gemini_key},
                json={
                    "content": {"parts": [{"text": (text or "")[:8000]}]},
                    "taskType": "RETRIEVAL_DOCUMENT",
                    "outputDimensionality": EMBED_DIM,
                },
                timeout=30,
            )
            if resp.status_code == 429:
                time.sleep(5 * (attempt + 1))
                continue
            resp.raise_for_status()
            vec = resp.json().get("embedding", {}).get("values")
            return vec or None
        except requests.RequestException:
            if attempt == retries - 1:
                raise
            time.sleep(3 * (attempt + 1))
    return None


def embed_shard_batch(conn, gemini_key, shard_key):
    """Vezme az BATCH_PER_SHARD pending chunku z jednoho shardu a zembeduje je.
    Vraci pocet uspesne zembedovanych chunku (0 = shard nema nic pending)."""
    with conn.cursor() as cur:
        cur.execute(
            "select id, content from get_pending_chunks_prioritized(%s)",
            (BATCH_PER_SHARD,),
        )
        rows = cur.fetchall()
    if not rows:
        return 0

    done = 0
    for chunk_id, content in rows:
        if time_left() <= 20:
            break
        try:
            vec = embed_text(content, gemini_key)
        except Exception as e:
            log(f"   [{shard_key}] WARN embed selhal {chunk_id}: {e}")
            continue
        if not vec:
            continue
        vec_str = "[" + ",".join(f"{x:.8f}" for x in vec) + "]"
        try:
            with conn.cursor() as cur2:
                cur2.execute(
                    "update chunks set embedding = %s::vector where id = %s",
                    (vec_str, chunk_id),
                )
            conn.commit()
            done += 1
        except Exception as e:
            conn.rollback()
            log(f"   [{shard_key}] WARN update selhal {chunk_id}: {e}")
    return done


def main():
    log("Zacinam embedding pending chunku ve 4 Neon shardech zakonu (round-robin)...")

    gemini_key = get_admin_gemini_key()

    neon_conns = {}
    for key, url in NEON_URLS.items():
        neon_conns[key] = psycopg2.connect(url, connect_timeout=15)
    log(f"Pripojeno ke vsem {len(neon_conns)} shardum.")

    totals = {key: 0 for key in NEON_URLS}
    exhausted = {key: False for key in NEON_URLS}
    round_num = 0

    while time_left() > 30 and not all(exhausted.values()):
        round_num += 1
        for key, conn in neon_conns.items():
            if exhausted[key] or time_left() <= 30:
                continue
            try:
                done = embed_shard_batch(conn, gemini_key, key)
            except Exception as e:
                log(f"   [{key}] CHYBA behem davky: {e}")
                try:
                    conn.rollback()
                except Exception:
                    pass
                done = 0
            totals[key] += done
            if done == 0:
                exhausted[key] = True
        if round_num % 5 == 0:
            log(f"...kolo {round_num}: " + ", ".join(f"{k}={v}" for k, v in totals.items()))

    for conn in neon_conns.values():
        conn.close()

    grand_total = sum(totals.values())
    log(f"Hotovo. Celkem zembedovano {grand_total} chunku: " + ", ".join(f"{k}={v}" for k, v in totals.items()))


if __name__ == "__main__":
    main()
