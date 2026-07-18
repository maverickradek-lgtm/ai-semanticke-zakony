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
to, ze je denni kvota pryc, aby nedoslo na
nekonecne opakovani.

POZNAMKA 3: pokud je nastaveny GEMINI_API_KEY_OVERRIDE (druhy, samostatny
Gemini klic - napr. z jineho Google uctu), pouzije se rovnou misto klice
ulozeneho v Supabase pro ADMIN_USER_ID. To umoznuje spustit druhy beh se
svou vlastni nezavislou denni kvotou soubezne s tim hlavnim - viz
REVERSE_ORDER, ktery zaridi, aby si oba behy co nejmin prekazely.
"""

import os
import time

import requests

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SERVICE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
ADMIN_USER_ID = os.environ.get("ADMIN_USER_ID", "")
GEMINI_API_KEY_OVERRIDE = os.environ.get("GEMINI_API_KEY_OVERRIDE", "").strip()

DAILY_EMBED_BUDGET = int(os.environ.get("DAILY_EMBED_BUDGET", "900"))
PAGE_SIZE = int(os.environ.get("EMBED_PAGE_SIZE", "1000"))
REVERSE_ORDER = os.environ.get("REVERSE_ORDER", "false").lower() == "true"

# Kdyz je nastaveno, tento beh embedduje VYHRADNE useky daneho doc_type
# (napr. "rozhodnuti_uohs") pres samostatnou frontu/RPC, misto obecne
# prioritizovane fronty pro predpisy a judikaty - viz migrace
# dedicate_uohs_embedding_queue. Umoznuje vyhradit tretimu Gemini klici
# vlastni nezavislou frontu, aby nemusela cekat za desetitisici jinych
# cekajicich useku.
DOC_TYPE_FILTER = os.environ.get("DOC_TYPE_FILTER", "").strip() or None

# Pevna prodleva mezi jednotlivymi pozadavky na embedContent (ne jen reaktivni
# cekani az PO chybe 429) - drzi tempo pod free-tier RPM stropem, ktery je u
# embedding modelu casto nizky, a predchazi tomu, aby beh hned od prvniho
# pozadavku narazel na rate limit kvuli prilis rychlemu odesilani.
REQUEST_DELAY_SECONDS = float(os.environ.get("REQUEST_DELAY_SECONDS", "2"))

MAX_CONSECUTIVE_QUOTA_FAILURES = int(os.environ.get("MAX_CONSECUTIVE_QUOTA_FAILURES", "3"))

EMBED_MODEL = "gemini-embedding-001"
EMBED_DIM = 256  # Matryoshka zkraceni z puvodnich 768 kvuli uspore mista v Supabase free tier (2026-07-18)

SESSION = requests.Session()


def log(*a):
    print(*a, flush=True)


def get_admin_gemini_key():
    if GEMINI_API_KEY_OVERRIDE:
        return GEMINI_API_KEY_OVERRIDE
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
    key = (r.json() or "").strip()
    if not key:
        raise RuntimeError("Admin nema ulozeny Gemini klic.")
    return key


def embed_text(text, gemini_key, task_type="RETRIEVAL_DOCUMENT"):
    """Vraci (embedding, quota_exhausted); embedding je None pri selhani;
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
    # uz neni proste "created_at asc" - upřednostnuje dokumenty s vyssi
    # embed_priority (par nejdulezitejsich aktualnich zakonu, napr. obcansky
    # zakonik) a preskakuji se dokumenty se skip_embedding = true (stare
    # jednorazove novely, jejichz obsah uz je vstrebany v aktualnim zneni
    # zakonu, ktere menily - viz migrace add_embed_priority_and_skip_flag).
    #
    # REVERSE_ORDER=true (druhy soubezny beh s druhym Gemini klicem) obraci
    # razeni (nejnizsi priorita/nejnovejsi useky prvni), aby se oba behy co
    # nejmin prekryvaly a nedelaly zbytecne duplicitni praci.
    if DOC_TYPE_FILTER:
        r = SESSION.post(
            f"{SUPABASE_URL}/rest/v1/rpc/get_pending_chunks_by_doctype",
            headers={
                "apikey": SERVICE_KEY,
                "Authorization": f"Bearer {SERVICE_KEY}",
                "Content-Type": "application/json",
            },
            json={"p_doc_type": DOC_TYPE_FILTER, "p_limit": limit},
            timeout=60,
        )
        r.raise_for_status()
        return r.json()

    r = SESSION.post(
        f"{SUPABASE_URL}/rest/v1/rpc/get_pending_chunks_prioritized",
        headers={
            "apikey": SERVICE_KEY,
            "Authorization": f"Bearer {SERVICE_KEY}",
            "Content-Type": "application/json",
        },
        json={"p_limit": limit, "p_ascending": REVERSE_ORDER},
        timeout=60,
    )
    r.raise_for_status()
    return r.json()


def fetch_document_titles(document_ids):
    # Davkuje se po malych skupinach, protoze jedno GET s "in.(...)" seznamem
    # VSECH id najednou muze pri velkem poctu ruznych dokumentu (napr. u
    # rozhodnuti UOHS, kde je skoro 1 usek = 1 dokument) vytvorit tak dlouhou
    # URL, ze ji Supabase odmitne (400 Bad Request).
    if not document_ids:
        return {}
    unique_ids = list(dict.fromkeys(document_ids))
    titles = {}
    batch_size = 150
    for i in range(0, len(unique_ids), batch_size):
        batch = unique_ids[i:i + batch_size]
        ids_param = "in.(" + ",".join(batch) + ")"
        r = SESSION.get(
            f"{SUPABASE_URL}/rest/v1/documents",
            headers={"apikey": SERVICE_KEY, "Authorization": f"Bearer {SERVICE_KEY}"},
            params={"select": "id,title", "id": ids_param},
            timeout=60,
        )
        r.raise_for_status()
        for d in r.json():
            titles[d["id"]] = d["title"]
    return titles


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
    log(f"DAILY_EMBED_BUDGET={DAILY_EMBED_BUDGET}, PAGE_SIZE={PAGE_SIZE}, REVERSE_ORDER={REVERSE_ORDER}, REQUEST_DELAY_SECONDS={REQUEST_DELAY_SECONDS}, DOC_TYPE_FILTER={DOC_TYPE_FILTER}")
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
            log("Zadne dalsi useky bez embeddingu.")
            break

        document_ids = list(set(c["document_id"] for c in chunks))
        titles = fetch_document_titles(document_ids)

        for c in chunks:
            title = titles.get(c["document_id"], "")
            heading = c["heading"]
            content = c["content"]
            text = f"{title} {heading}: {content}"
            time.sleep(REQUEST_DELAY_SECONDS)
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
