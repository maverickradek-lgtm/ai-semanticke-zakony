"""
Kontrola rucne zadanych zadosti o predpis (tabulka predpis_requests) proti
aktualnimu stavu databaze.

Toto NENI plnohodnotne automaticke stazeni chybejiciho predpisu z e-Sbirky -
jen kontroluje, jestli pozadovany predpis mezitim nahodou nenahral bezny denni
sync (sync_esbirka_text.py), a pokud ano, oznaci zadost jako vyrizenou. Pokud
predpis v databazi porad neni, zadost zustava "pending" a ceka na rucni
zpracovani - napojeni na primy cilene stazeni konkretniho predpisu z e-Sbirky
je slozitejsi ukol (viz komplexni parsovani fragmentu v sync_esbirka_text.py),
zatim zamerne neresime automaticky, aby nedoslo k chybe v produkcni databazi.
"""
import os
import re
from datetime import datetime, timezone

import requests

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SERVICE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

HEADERS = {
    "apikey": SERVICE_KEY,
    "Authorization": f"Bearer {SERVICE_KEY}",
    "Content-Type": "application/json",
}

CITATION_RE = re.compile(r"(\d{1,4})\s*/\s*(\d{4})")


def log(msg):
    print(msg, flush=True)


def fetch_pending():
    r = requests.get(
        f"{SUPABASE_URL}/rest/v1/predpis_requests",
        headers=HEADERS,
        params={"status": "eq.pending", "select": "id,query_text"},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def find_matching_document(citace):
    r = requests.get(
        f"{SUPABASE_URL}/rest/v1/documents",
        headers=HEADERS,
        params={
            "external_id": f"ilike.{citace}%",
            "is_current": "eq.true",
            "select": "id,external_id,title",
            "limit": "1",
        },
        timeout=30,
    )
    r.raise_for_status()
    rows = r.json()
    return rows[0] if rows else None


def update_request(req_id, status, note):
    r = requests.patch(
        f"{SUPABASE_URL}/rest/v1/predpis_requests",
        headers=HEADERS,
        params={"id": f"eq.{req_id}"},
        json={
            "status": status,
            "note": note,
            "processed_at": datetime.now(timezone.utc).isoformat(),
        },
        timeout=30,
    )
    r.raise_for_status()


def main():
    pending = fetch_pending()
    log(f"Nalezeno {len(pending)} cekajicich zadosti.")
    for req in pending:
        text = req["query_text"]
        m = CITATION_RE.search(text)
        if not m:
            log(f"  '{text}': neobsahuje rozpoznatelne cislo predpisu (napr. 141/1961), preskakuji.")
            continue
        citace = f"{m.group(1)}/{m.group(2)}"
        doc = find_matching_document(citace)
        if doc:
            log(f"  '{text}': jiz v databazi ({doc['title']}), oznacuji jako vyrizene.")
            update_request(req["id"], "fetched", f"Nalezeno v databazi: {doc['title']}")
        else:
            log(f"  '{text}': v databazi zatim neni, zustava cekajici (potreba rucni zpracovani).")


if __name__ == "__main__":
    main()
