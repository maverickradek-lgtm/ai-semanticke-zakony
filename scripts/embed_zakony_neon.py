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

Zalozni klic (GEMINI_API_KEY_BACKUP, viz gemini_quota.py): kdyz primarnimu
klici shardu dojde trpelivost (RateLimitStop po MAX_CONSECUTIVE_429 chybach
429 v rade), shard dostane JEDNU sanci prepnout se na sdileny zalozni klic
misto trvaleho vyrazeni pro zbytek behu. Zalozni klic muze mit vlastni denni
strop (GEMINI_KEY_QUOTA_CAPS) - viz gemini_quota.py, ktery si strop meri sam
misto spolehani na (uz neplatne) zverejnene RPD limity od Googlu.
"""

import json
import os
import sys
import time
import psycopg2

import gemini_quota


def db_connect(url, timeout=15):
    """Pripoji se k Neonu se 4 pokusy - NAS self-hosted runner ma obcas
    docasny DNS vypadek (Temporary failure in name resolution), jednorazovy
    pokus bez retry pak shodi cely beh zbytecne."""
    last_err = None
    for attempt in range(4):
        try:
            return psycopg2.connect(url, connect_timeout=timeout)
        except Exception as e:
            last_err = e
            print("db_connect selhalo (pokus " + str(attempt + 1) + "/4): " + str(e), flush=True)
            time.sleep(3)
    raise last_err
import requests

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SERVICE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
ADMIN_USER_ID = "2648f5db-bea6-4cac-b490-ad0ec59723df"
GEMINI_API_KEY_OVERRIDE = os.environ.get("GEMINI_API_KEY_OVERRIDE")
GEMINI_API_KEY_POOL = [k.strip() for k in os.environ.get("GEMINI_API_KEY_POOL", "").split(",") if k.strip()]
GEMINI_API_KEY_POOL_LABELS = [k.strip() for k in os.environ.get("GEMINI_API_KEY_POOL_LABELS", "").split(",") if k.strip()]
GEMINI_API_KEY_BACKUP = os.environ.get("GEMINI_API_KEY_BACKUP")
GEMINI_API_KEY_BACKUP_LABEL = os.environ.get("GEMINI_API_KEY_BACKUP_LABEL", "GEMINI_API_KEY_BACKUP")
# Napr. {"GEMINI_API_KEY_BACKUP": 0.6} - klic v tomto slovniku se nesmi
# pouzit nad zadany podil (cap_fraction) sveho POZOROVANEHO denniho stropu
# (viz gemini_quota.py - Google presne RPD cisla uz nezverejnuje). Klic,
# ktery tu neni uveden, se meri, ale neomezuje - snadno se pridavaji dalsi
# klice do tohoto omezovani proste pridanim do teto JSON promenne.
GEMINI_KEY_QUOTA_CAPS = json.loads(os.environ.get("GEMINI_KEY_QUOTA_CAPS", "{}") or "{}")

EMBED_MODEL = "gemini-embedding-001"
EMBED_DIM = 256
BATCH_PER_SHARD = int(os.environ.get("BATCH_PER_SHARD", "20"))
TIME_BUDGET_SECONDS = int(os.environ.get("TIME_BUDGET_SECONDS", "3000"))
MAX_CONSECUTIVE_429 = int(os.environ.get("MAX_CONSECUTIVE_429", "10"))

NEON_URLS = {
    "do1997": os.environ["NEON_ZAKONY_DO1997_DB_URL"],
    "1998_2007": os.environ["NEON_ZAKONY_1998_2007_DB_URL"],
    "2008_2020": os.environ["NEON_ZAKONY_2008_2020_DB_URL"],
    "2021_dosud": os.environ["NEON_ZAKONY_2021_DOSUD_DB_URL"],
}

# Radek (2026-09-02): novejsi predpisy jsou prioritnejsi nez historie do roku 2000 -
# vahy urcuji, kolikrat za "velke kolo" se dany shard zpracuje (viz build_round_schedule).
# do1997 neni vyrazen uplne, jen zpomalen oproti ostatnim.
SHARD_WEIGHTS = {
    "do1997": 1,
    "1998_2007": 2,
    "2008_2020": 3,
    "2021_dosud": 4,
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
    last_err = None
    for attempt in range(4):
        try:
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
        except Exception as e:
            last_err = e
            print("get_admin_gemini_key selhalo (pokus " + str(attempt + 1) + "/4): " + str(e), flush=True)
            time.sleep(3)
    raise last_err


class RateLimitStop(Exception):
    """Vyvolano, kdyz pro dany Gemini klic (shard) narazime na MAX_CONSECUTIVE_429
    chyb 429 v rade. Bez GEMINI_API_KEY_POOL maji vsechny shardy stejny klic,
    takze v tom pripade se povazuji za vycerpane vsechny naraz - s poolem jen
    ten jeden shard, jehoz klic je aktualne rate-limitovany, ostatni bezi dal."""


class QuotaCapped(RateLimitStop):
    """Vyvolano, kdyz klic dosahl sveho nastaveneho denniho stropu (viz
    GEMINI_KEY_QUOTA_CAPS / gemini_quota.py) - na rozdil od RateLimitStop
    (skutecna chyba 429 od Googlu) jde o preventivni vlastni omezeni, aby
    klic nevycerpal celou kvotu na ukor zivych uzivatelu appky. Dedi z
    RateLimitStop, aby ji volajici kod (main - prepnuti na zalozni klic /
    vyrazeni shardu) zpracoval uplne stejne, bez zvlastni vetve navic."""


_consecutive_429 = {}  # track_key (shard) -> pocet po sobe jdoucich 429 pro dany klic


def embed_text(text, gemini_key, gemini_key_label, retries=3, track_key="default"):
    global _consecutive_429

    cap = GEMINI_KEY_QUOTA_CAPS.get(gemini_key_label)
    if cap is not None and not gemini_quota.check_quota(SUPABASE_URL, SERVICE_KEY, gemini_key_label, cap_fraction=cap):
        raise QuotaCapped(
            f"klic '{gemini_key_label}' dosahl dnesniho stropu ({int(cap * 100)} % pozorovaneho denniho limitu) - preskakuji"
        )

    last_status = None
    last_body = None
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
            last_status = resp.status_code
            last_body = resp.text[:1500]
            if resp.status_code == 429:
                gemini_quota.report_429(SUPABASE_URL, SERVICE_KEY, gemini_key_label)
                n = _consecutive_429.get(track_key, 0) + 1
                _consecutive_429[track_key] = n
                if n >= MAX_CONSECUTIVE_429:
                    raise RateLimitStop(
                        f"{n} po sobe jdoucich 429 (rate limit) chyb pro klic shardu "
                        f"'{track_key}' - koncim tento klic misto dalsiho plytvani casem."
                    )
                time.sleep(5 * (attempt + 1))
                continue
            resp.raise_for_status()
            vec = resp.json().get("embedding", {}).get("values")
            _consecutive_429[track_key] = 0
            return vec or None
        except requests.RequestException as e:
            last_status = getattr(getattr(e, "response", None), "status_code", None)
            last_body = str(e)[:1500]
            if attempt == retries - 1:
                log(f"   DEBUG embed_text: vyjimka po vycerpani pokusu (status={last_status}): {last_body}")
                raise
            time.sleep(3 * (attempt + 1))
    log(f"   DEBUG embed_text: vycerpany pocet pokusu bez vyjimky (posledni status={last_status}): {last_body}")
    return None


def embed_shard_batch(conn, key_info, shard_key):
    """Vezme az BATCH_PER_SHARD pending chunku z jednoho shardu a zembeduje je.
    Vraci pocet uspesne zembedovanych chunku (0 = shard nema nic pending).
    key_info je {"value": <raw klic>, "label": <stabilni nazev klice pro
    kvotovy system>}."""
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
            vec = embed_text(content, key_info["value"], key_info["label"], track_key=shard_key)
        except RateLimitStop:
            raise
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


def ensure_conn(neon_conns, key):
    """Vrati zive spojeni pro dany shard - pokud stavajici spojeni zemrelo
    (napr. Neon uspal necinny branch behem dlouheho zpracovani jineho
    shardu), tise ho znovu naveze, misto aby se dany shard omylem oznacil
    za 'exhausted' jen kvuli spadlemu spojeni."""
    conn = neon_conns.get(key)
    if conn is not None and conn.closed == 0:
        try:
            with conn.cursor() as cur:
                cur.execute("select 1")
            return conn
        except Exception:
            try:
                conn.close()
            except Exception:
                pass
    conn = db_connect(NEON_URLS[key])
    neon_conns[key] = conn
    log(f"   [{key}] (znovu navazano spojeni)")
    return conn


def build_round_schedule():
    """Vraci poradi shardu pro jedno 'velke kolo' podle SHARD_WEIGHTS - shard s
    vahou 3 se v nem objevi 3x, shard s vahou 1 jen 1x. Tim dostavaji novejsi
    (dulezitejsi) shardy vetsi podil casoveho rozpoctu nez stary do1997."""
    schedule = []
    for key, weight in SHARD_WEIGHTS.items():
        schedule.extend([key] * weight)
    return schedule


def main():
    log("Zacinam embedding pending chunku ve 4 Neon shardech zakonu (round-robin)...")

    if GEMINI_API_KEY_POOL:
        shard_names = list(NEON_URLS.keys())
        labels = GEMINI_API_KEY_POOL_LABELS if len(GEMINI_API_KEY_POOL_LABELS) == len(GEMINI_API_KEY_POOL) else [
            f"pool_{i}" for i in range(len(GEMINI_API_KEY_POOL))
        ]
        shard_keys = {
            name: {
                "value": GEMINI_API_KEY_POOL[i % len(GEMINI_API_KEY_POOL)],
                "label": labels[i % len(labels)],
            }
            for i, name in enumerate(shard_names)
        }
        log(f"Pouzivam pool {len(GEMINI_API_KEY_POOL)} Gemini klicu rozdelenych po shardech (vic klicu = vic paralelni kvoty).")
    else:
        admin_key = get_admin_gemini_key()
        shard_keys = {name: {"value": admin_key, "label": "admin"} for name in NEON_URLS}

    if GEMINI_API_KEY_BACKUP:
        log(f"Zalozni klic '{GEMINI_API_KEY_BACKUP_LABEL}' k dispozici pro shardy, kterym dojde primarni klic.")
    if GEMINI_KEY_QUOTA_CAPS:
        log(f"Denni stropy pro klice: {GEMINI_KEY_QUOTA_CAPS}")

    neon_conns = {}
    for key, url in NEON_URLS.items():
        last_err = None
        for attempt in range(4):
            try:
                neon_conns[key] = db_connect(url)
                last_err = None
                break
            except Exception as e:
                last_err = e
                log(f"   [{key}] WARN pripojeni selhalo (pokus {attempt + 1}/4): {e}")
                time.sleep(5 * (attempt + 1))
        if last_err is not None:
            raise last_err
    log(f"Pripojeno ke vsem {len(neon_conns)} shardum.")

    totals = {key: 0 for key in NEON_URLS}
    exhausted = {key: False for key in NEON_URLS}
    zero_streak = {key: 0 for key in NEON_URLS}
    backup_used = {key: False for key in NEON_URLS}
    rate_limited_shards = set()
    round_num = 0

    round_schedule = build_round_schedule()

    while time_left() > 30 and not all(exhausted.values()):
        round_num += 1
        for key in round_schedule:
            if exhausted[key] or time_left() <= 30:
                continue
            try:
                conn = ensure_conn(neon_conns, key)
                done = embed_shard_batch(conn, shard_keys[key], key)
            except RateLimitStop as e:
                log(f"   [{key}] STOP na klici '{shard_keys[key]['label']}': {e}")
                if GEMINI_API_KEY_BACKUP and not backup_used[key]:
                    log(f"   [{key}] prepinam na zalozni klic '{GEMINI_API_KEY_BACKUP_LABEL}' misto vyrazeni shardu")
                    shard_keys[key] = {"value": GEMINI_API_KEY_BACKUP, "label": GEMINI_API_KEY_BACKUP_LABEL}
                    backup_used[key] = True
                    _consecutive_429[key] = 0
                else:
                    rate_limited_shards.add(key)
                    exhausted[key] = True
                    if not GEMINI_API_KEY_POOL:
                        # bez poolu maji vsechny shardy stejny klic - je vycerpany pro vsechny
                        for k in exhausted:
                            exhausted[k] = True
                            rate_limited_shards.add(k)
                done = 0
            except Exception as e:
                log(f"   [{key}] CHYBA behem davky (zkusim znovu pristi kolo): {e}")
                try:
                    neon_conns[key].rollback()
                except Exception:
                    pass
                done = -1
            totals[key] += max(done, 0)
            if done == 0:
                zero_streak[key] += 1
                # 0 muze byt i prechodny zaskyk (napr. cerstve otevrene spojeni) -
                # az 3x po sobe 0 v rade povazujeme shard za doopravdy vycerpany,
                # jinak bychom mohli shard nespravedlive vyradit na cely beh
                # (viz run #22 - 2021_dosud mel 0 v kole 1, ale chunky pak realne mel).
                if zero_streak[key] >= 3:
                    exhausted[key] = True
            else:
                zero_streak[key] = 0
        if round_num % 5 == 0:
            log(f"...kolo {round_num}: " + ", ".join(f"{k}={v}" for k, v in totals.items()))

    for conn in neon_conns.values():
        try:
            conn.close()
        except Exception:
            pass

    grand_total = sum(totals.values())
    suffix = f" (rate limit/kvota zastavila: {', '.join(sorted(rate_limited_shards))})" if rate_limited_shards else ""
    used_backup = [k for k, v in backup_used.items() if v]
    if used_backup:
        suffix += f" (zalozni klic pouzit pro: {', '.join(sorted(used_backup))})"
    log(f"Hotovo. Celkem zembedovano {grand_total} chunku: " + ", ".join(f"{k}={v}" for k, v in totals.items()) + suffix)


if __name__ == "__main__":
    main()
