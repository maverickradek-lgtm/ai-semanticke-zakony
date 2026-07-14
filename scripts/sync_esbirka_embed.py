"""
FAZE B: denni dobihajici vypocet embeddingu pro useky (chunks), ktere je
jeste nemaji (embedding IS NULL) - typicky proto, ze je tam faze A
(sync_esbirka_text.py) teprve nedavno nahrala.

Bezplatny Gemini klic ma denni limit poctu volani (RPD - requests per day),
proto se tu zpracovava jen omezena denni davka (DAILY_EMBED_BUDGET) a zbytek
pockej na dalsi den - tento skript je navrzeny tak, aby bezel na cronu kazdy
den a postupne dobihal, dokud neni vse ohodnoceno; pak uz jen drzi krok s
novinkami.

POZNAMKA 1: Supabase/PostgREST vraci na jeden dotaz max PAGE_SIZE radku bez
ohledu na pozadovany "limit" parametr, proto se cela denni davka stahuje
a zpracovava po strankach (viz main()).

POZNAMKA 2: DAILY_EMBED_BUDGET musi zustat pod skutecnym denni kvotou
free-tier Gemini API pro embeddingy (v praxi kolem 900-1000 pozadavku/den -
presne cislo Google nezverejnuje a muze se menit). Pokud narazime na
opakovane 429 chyby i po vycerpani vsech pokusu na jednom useku, znamena
to, ze je denni kvota jiz vycerpana - dalsi cekani ji stejne neobnovi
(reset je az o pulnoci pacifickeho casu), takze skript v tom pripade rovnou
skonci, misto aby zbytecne plytval casem (a GitHub Actions minutami) na
nekonecne opakovani.
"""

import os
import time

import requests

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SERVICE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
ADMIN_USER_ID = os.environ["ADMIN_USER_ID"]

DAILY_EMBED_BUDGET = int(os.environ.get("DAILY_EMBED_BUDGET", "900"))
PAGE_SIZE = int(os.environ.get("EMBED_PAGE_SIZE", "1000"))

# Kolik useku po sobe muze selhat (vycerpat vsech 5 pokusu kvuli 429), nez to
# vyhodnotime jako "denni kvota je pryc" a skoncime rovnou.
MAX_CONSECUTIVE_QUOTA_FAILURES = int(os.environ.get("MAX_CONSECUTIVE_QUOTA_FAILURES", "3"))

EMBED_MODEL = "gemini-embedding-001"
EMBED_DIM = 768

SESSION = requests.Session()


def log(*a):
    print(*a, flush=True)


def get_admin_gemini_key():
    r = SESSION.post(
        f"{SUPABASE_URL}/rest/v1/rpc/get_user_gemini_key",
        headers={
            "apikey": SERVICE_KEY,
            "Authorization": f"Bearer {SERVICE_KEY}",
            "Content-Type": "application/json",
        },
        json={"p_user_id": ADMIN_USER_ID},
        timeout=30,
    )
    r.raise_for_status()
    key = r.json()
    if not key:
        raise RuntimeError("Admin nema ulozeny Gemini klic.")
    return key


def embed_text(text, gemini_key, task_type="RETRIEVAL_DOCUMENT"):
    """Vraci (embedding, quota_exhausted). embedding je None pri selhani;
    quota_exhausted je True, pokud jsme vycerpali vsechny pokusy kvuli 429
    (silny signal, ze je denni kvota pryc, ne jen kratkodoby spicak)."""
    for attempt in range(5):
        r = SESSION.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/{EMBED_MODEL}:embedContent",
            headers={"Content-Type": "application/json", "x-goog-api-key": gemini_key},
            json={
                "content": {"parts": [{"text": text[:8000]}]},
                "taskType": task_type,
                "outputDimensionality": EMBED_DIM,
            },
            timeout=60,
        )
        if r.status_code == 429:
            wait = 15 * (attempt + 1)
            log(f"   rate limit, cekam {wait}s...")
            time.sleep(wait)
            continue
        if not r.ok:
            log("   embed chyba:", r.status_code, r.text[:300])
            return None, False
        values = r.json()["embedding"]["values"]
        norm = sum(v * v for v in values) ** 0.5
        if norm > 0:
            values = [v / norm for v in values]
        return values, False
    log("   embedding se nepovedlo ziskat po 5 pokusech (rate limit)")
    return None, True


def fetch_pending_chunks(limit):
    # Vola RPC misto primeho dotazu na tabulku, protoze poradi zpracovani
    # uz neni proste "created_at asc" - upreднostnuje se dokumenty s vyssi
    # embed_priority (par nejdulezitejsich aktualnich zakonu, napr. obcansky
    # zakonik) a preskakuji se dokumenty se skip_embedding = true (stare
    # jednorazove novely, jejichz obsah uz je vstrebany v aktualnim zneni
    # zakonu, ktere menily - viz migrace add_embed_priority_and_skip_flag).
    r = SESSION.post(
        f"{SUPABASE_URL}/rest/v1/rpc/get_pending_chunks_prioritized",
        headers={
            "apikey": SERVICE_KEY,
            "Authorization": f"Bearer {SERVICE_KEY}",
            "Content-Type": "application/json",
        },
        json={"p_limit": limit},
        timeout=60,
    )
    r.raise_for_status()
    return r.json()


def fetch_document_titles(document_ids):
    if not document_ids:
        return {}
    ids_param = "in.(" + ",".join(document_ids) + ")"
    r = SESSION.get(
        f"{SUPABASE_URL}/rest/v1/documents",
        headers={"apikey": SERVICE_KEY, "Authorization": f"Bearer {SERVICE_KEY}"},
        params={"select": "id,title", "id": ids_param},
        timeout=60,
    )
    r.raise_for_status()
    return {d["id"]: d["title"] for d in r.json()}


def update_chunk_embedding(chunk_id, embedding):
    r = SESSION.patch(
        f"{SUPABASE_URL}/rest/v1/chunks",
        headers={
            "apikey": SERVICE_KEY,
            "Authorization": f"Bearer {SERVICE_KEY}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal",
        },
        params={"id": f"eq.{chunk_id}"},
        json={"embedding": embedding},
        timeout=30,
    )
    r.raise_for_status()


def main():
    log("=== Embedding dobihani (faze B): start ===")
    log(f"DAILY_EMBED_BUDGET={DAILY_EMBED_BUDGET}, PAGE_SIZE={PAGE_SIZE}")
    gemini_key = get_admin_gemini_key()

    total_done = 0
    total_failed = 0
    remaining = DAILY_EMBED_BUDGET
    consecutive_quota_failures = 0
    quota_exhausted = False

    while remaining > 0 and not quota_exhausted:
        batch_limit = min(PAGE_SIZE, remaining)
        chunks = fetch_pending_chunks(batch_limit)
        if not chunks:
            log("Zadne dalsi useky bez embeddingu, hotovo.")
            break

        log(f"Stranka: nalezeno {len(chunks)} useku bez embeddingu (zbyva z denni davky: {remaining})")
        titles = fetch_document_titles(list({c["document_id"] for c in chunks}))

        for c in chunks:
            title = titles.get(c["document_id"], "")
            text = f"{title} {c['heading']}: {c['content']}"
            embedding, quota_hit = embed_text(text, gemini_key)
            if embedding is None:
                total_failed += 1
                if quota_hit:
                    consecutive_quota_failures += 1
                    if consecutive_quota_failures >= MAX_CONSECUTIVE_QUOTA_FAILURES:
                        log(
                            f"Denni kvota Gemini API je nejspis vycerpana "
                            f"({consecutive_quota_failures}x za sebou selhalo po vsech pokusech) - koncim, "
                            f"zbytek doplni zitrejsi beh."
                        )
                        quota_exhausted = True
                        break
                else:
                    consecutive_quota_failures = 0
                continue
            consecutive_quota_failures = 0
            update_chunk_embedding(c["id"], embedding)
            total_done += 1
            if total_done % 100 == 0:
                log(f" ...ohodnoceno celkem {total_done}")

        remaining -= len(chunks)

        if quota_exhausted:
            break

        if len(chunks) < batch_limit:
            log("Posledni neuplna stranka vracena, dalsi kolo by bylo prazdne - koncim.")
            break

    log(f"=== Embedding dobihani: hotovo, ohodnoceno {total_done}, selhalo {total_failed} ===")


if __name__ == "__main__":
    main()
