#!/usr/bin/env python3
"""
sync_sbirka_ns_embed.py - Faze B: dopocita embeddingy pro chunky ze Sbirky
Nejvyssiho soudu (druhy, samostatny Supabase projekt pro judikaturu).

Na rozdil od hlavniho sync_esbirka_embed.py (ktery pouziva Radkuv Gemini
klic ulozeny v Supabase Vault, protoze bezi proti hlavnimu projektu se
skutecnymi uzivatelskymi ucty) tento skript cte Gemini klic primo z env
promenne GEMINI_API_KEY - druhy projekt nema zadne uzivatelske ucty ani
Vault zaznamy, je to cistě backendova synchronizacni uloha.

Dimenze: 256 (Matryoshka truncation gemini-embedding-001), stejne jako
hlavni projekt - viz storage_scaling_plan v pameti. Po zkraceni se vektor
renormalizuje na jednotkovou delku (nutne pro spravnou cosine podobnost).
"""

import os
import time

import requests

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SERVICE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
GEMINI_API_KEY = os.environ["GEMINI_API_KEY"]

EMBED_DIM = int(os.environ.get("EMBED_DIM", "256"))
DAILY_EMBED_BUDGET = int(os.environ.get("DAILY_EMBED_BUDGET", "2500"))
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "50"))

GEMINI_URL = (
    "https://generativelanguage.googleapis.com/v1beta/models/"
    "gemini-embedding-001:embedContent"
)

SESSION = requests.Session()


def log(*a):
    print(*a, flush=True)


def sb_headers():
    return {
        "apikey": SERVICE_KEY,
        "Authorization": f"Bearer {SERVICE_KEY}",
        "Content-Type": "application/json",
    }


def get_pending_chunks(limit):
    """Nacte chunky, ktere jeste nemaji embedding, serazene podle
    documents.embed_priority sestupne (dulezitejsi zdroje driv), pak
    podle stari zaznamu."""
    r = SESSION.get(
        f"{SUPABASE_URL}/rest/v1/chunks",
        headers=sb_headers(),
        params={
            "select": "id,content",
            "embedding": "is.null",
            "order": "created_at.asc",
            "limit": str(limit),
        },
        timeout=60,
    )
    r.raise_for_status()
    return r.json()


class QuotaExhausted(Exception):
    """Signalizuje, ze denni/uctova kvota tohoto Gemini klice je
    pravdepodobne vycerpana - nema smysl dal zkouset dalsi chunky,
    kazdy dalsi pozadavek skoro jiste take dostane 429."""


def embed_text(text, max_retries=2):
    """Zavola Gemini embedContent, vrati renormalizovany vektor o
    EMBED_DIM prvcich, nebo None pri trvale (ne-kvotove) chybe.

    Zamerne KRATKE retry (max 2x, kratky backoff) misto dlouheho
    cekani - pokud je 429 zpusobene vycerpanou denni kvotou uctu (ne
    prechodnym prekrocenim RPM), dalsi cekani nepomuze a jen to plyta
    minuty GitHub Actions behu (viz stejna lekce u sync_psp_tisky.py -
    fail fast, nezkouset dokola donekonecna)."""
    payload = {
        "model": "models/gemini-embedding-001",
        "content": {"parts": [{"text": text[:20000]}]},
        "outputDimensionality": EMBED_DIM,
        "taskType": "RETRIEVAL_DOCUMENT",
    }
    for attempt in range(max_retries):
        try:
            r = SESSION.post(
                GEMINI_URL,
                headers={"x-goog-api-key": GEMINI_API_KEY},
                json=payload,
                timeout=60,
            )
            if r.status_code == 429:
                if attempt < max_retries - 1:
                    log("  429 rate limit, kratka pauza 5s a jeste jeden pokus...")
                    time.sleep(5)
                    continue
                # druhy 429 v rade - temer jiste vycerpana denni kvota uctu,
                # ne jen prechodny RPM limit. Vzdavame se cele ulohy pro dnesek.
                raise QuotaExhausted(
                    "Gemini 429 i po kratkem retry - denni kvota uctu je "
                    "pravdepodobne vycerpana (typicky jinym behem, ktery "
                    "pouziva stejny ucet drive behem dne)."
                )
            if not r.ok:
                log(f"  Gemini chyba {r.status_code}: {r.text[:300]}")
                if r.status_code >= 500:
                    time.sleep(5)
                    continue
                return None
            values = r.json()["embedding"]["values"]
            norm = sum(v * v for v in values) ** 0.5
            if norm > 0:
                values = [v / norm for v in values]
            return values
        except requests.RequestException as e:
            log(f"  chyba pri volani Gemini: {e}")
            time.sleep(5)
    return None


def save_embedding(chunk_id, values):
    r = SESSION.patch(
        f"{SUPABASE_URL}/rest/v1/chunks",
        headers={**sb_headers(), "Prefer": "return=minimal"},
        params={"id": f"eq.{chunk_id}"},
        json={"embedding": values},
        timeout=60,
    )
    if not r.ok:
        log(f"  Supabase chyba pri ukladani {chunk_id}: {r.status_code} {r.text[:300]}")
        r.raise_for_status()


def main():
    log(f"=== sync_sbirka_ns_embed start (EMBED_DIM={EMBED_DIM}, "
        f"DAILY_EMBED_BUDGET={DAILY_EMBED_BUDGET}) ===")

    done = 0
    errors = 0
    consecutive_errors = 0
    # Obecna pojistka navic k QuotaExhausted (ktera resi jen opakovane 429):
    # pokud selze 5 chunku v rade z JAKEHOKOLIV duvodu (spatny klic, spatny
    # format pozadavku, vypadek API...), je to znamka strukturalni chyby,
    # ne nahodneho spatneho obsahu jednoho chunku - nema smysl propalit
    # celych 300/den na opakovani te same chyby (presne tohle se stalo
    # 2026-07-19: spatna metoda autentizace vracela 401 na kazdem chunku).
    MAX_CONSECUTIVE_ERRORS = 5

    try:
        while done < DAILY_EMBED_BUDGET:
            batch = get_pending_chunks(min(BATCH_SIZE, DAILY_EMBED_BUDGET - done))
            if not batch:
                log("Zadne dalsi chunky bez embeddingu - hotovo.")
                break

            for chunk in batch:
                values = embed_text(chunk["content"])
                if values is None:
                    errors += 1
                    consecutive_errors += 1
                    if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                        log(f"  {consecutive_errors}x chyba v rade - "
                            f"pravdepodobne strukturalni problem (spatny "
                            f"klic, spatny format pozadavku...), ne nahoda. "
                            f"Koncim drive misto marneho opakovani.")
                        raise QuotaExhausted(
                            "Prilis mnoho chyb v rade, koncim drive."
                        )
                    continue
                consecutive_errors = 0
                save_embedding(chunk["id"], values)
                done += 1

            log(f"  ...zpracovano {done}/{DAILY_EMBED_BUDGET} (chyb {errors})")
    except QuotaExhausted as e:
        # Fail fast: nekoncime jako chyba behu (exit 1), protoze tohle je
        # ocekavany denni stav (sdileny ucet uz vycerpal kvotu jinym behem),
        # ne bug. Kdyz uz je progres > 0, cely den nebyl marny.
        log(f"  {e}")
        log(f"  Koncim drive, misto marneho zkouseni dalsich chunku.")

    log(f"=== Hotovo: embedovano {done}, chyb {errors} ===")


if __name__ == "__main__":
    main()
