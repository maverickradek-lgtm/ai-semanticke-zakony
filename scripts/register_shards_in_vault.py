"""
Zaregistruje connection stringy reshard shardu (GitHub secrets
NEON_RESHARD_01..09_DB_URL) do Supabase Vaultu pod nazvem
neon_zakony_reshard-NN_db_url, odkud je cte vyhledavani v aplikaci (edge
funkce ai-query a zakony-db pres RPC get_neon_zakony_db_url). Tim se nove
reshardy automaticky objevi ve vyhledavani, bez rucniho zapisovani URL do
Supabase.

Zaroven na kazdem nastavenem reshardu zajisti schema (ensure_schema -
idempotentni; mj. vytvori funkci match_chunks, bez ktere by vyhledavani v
reshardu nic nenaslo).

Connection stringy se NIKDE NEVYPISUJI (jen jmeno shardu a vysledek).
Idempotentni - bezpecne spoustet opakovane. Shardy bez nastaveneho secretu se
preskoci.
"""

import os
import sys
import time

import requests

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SERVICE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

RESHARDS = [f"reshard-{n:02d}" for n in range(1, 10)]


def log(*a):
    print(*a)
    sys.stdout.flush()


def register(shard, url):
    last = None
    for attempt in range(5):
        try:
            r = requests.post(
                f"{SUPABASE_URL}/rest/v1/rpc/set_neon_zakony_db_url",
                headers={
                    "apikey": SERVICE_KEY,
                    "Authorization": f"Bearer {SERVICE_KEY}",
                    "Content-Type": "application/json",
                },
                json={"p_shard": shard, "p_url": url},
                timeout=30,
            )
            if r.status_code in (200, 204):
                return True
            last = f"HTTP {r.status_code}: {r.text[:200]}"
        except requests.RequestException as e:
            last = type(e).__name__
        time.sleep(10 * (attempt + 1))
    log(f"   [{shard}] registrace selhala: {last}")
    return False


def main():
    failed = 0
    try:
        import migrate_zakony_to_neon as neonlib  # ensure_schema
    except Exception as e:  # noqa: BLE001
        neonlib = None
        log(f"POZOR: ensure_schema neni k dispozici ({type(e).__name__}), jen registrace.")

    for shard in RESHARDS:
        env_key = "NEON_" + shard.upper().replace("-", "_") + "_DB_URL"
        url = os.environ.get(env_key, "").strip()
        if not url:
            log(f"   [{shard}] preskoceno - secret {env_key} neni nastaven.")
            continue
        if neonlib is not None:
            try:
                conn = neonlib.db_connect(url)
                try:
                    neonlib.ensure_schema(conn)
                finally:
                    conn.close()
                log(f"   [{shard}] schema v poradku (match_chunks, indexy).")
            except Exception as e:  # noqa: BLE001
                log(f"   [{shard}] ensure_schema selhalo: {type(e).__name__}")
                failed += 1
                continue
        if register(shard, url):
            log(f"   [{shard}] zaregistrovano ve Vaultu (neon_zakony_{shard}_db_url).")
        else:
            failed += 1
    if failed:
        log(f"=== SELHANI: {failed} shardu se nepodarilo zpracovat ===")
        sys.exit(1)
    log("=== Hotovo ===")


if __name__ == "__main__":
    main()
