"""
Kontrola rucne zadanych zadosti o predpis (tabulka predpis_requests) proti
aktualnimu stavu databaze, s doplnkovym overenim proti oficialnimu REST API
e-Sbirky (Ministerstvo vnitra CR), pokud je nastaven ESBIRKA_REST_API_KEY.

Toto STALE NENI plnohodnotne automaticke stazeni CELEHO TEXTU chybejiciho
predpisu do nasi databaze - jen kontroluje:
  1) jestli pozadovany predpis mezitim nahodou nenahral bezny denni sync
     (sync_esbirka_text.py) - pokud ano, oznaci zadost jako vyrizenou.
  2) pokud v nasi DB porad neni, zepta se oficialniho REST API e-Sbirky
     (viz e-sbirka.gov.cz/restful-api, endpoint /dokumenty-sbirky/{staleUrl}),
     jestli takovy predpis vubec oficialne existuje a je platny. Pokud ano,
     zustava zadost "pending", ale s vysvetlujici poznamkou (potvrzeno
     oficialne, jen jeste nenaimportovano k nam). Pokud neexistuje ani tam,
     oznaci se "not_found" s vysvetlenim pro uzivatele.

Napojeni na CILENE STAZENI CELEHO TEXTU (fragmenty, viz
/dokumenty-sbirky/{staleUrl}/fragmenty) je zamerne DALSI krok, ne soucast
tohoto skriptu - vyzaduje spravne poskladat hierarchii fragmentu (Paragraf ->
Odstavec -> Pismeno), coz je stejna trida slozite logiky, ktera uz v tomto
projektu dvakrat zpusobila realne produkcni bugy (viz pamet
duplicate_is_current_bug_fix, cron_overlap_incident). Zamerne se to nedela
narychlo/bez zivého otestovani s Radkem - viz pamet
predpis_request_scope_decision a esbirka_rest_api_key.
"""
import os
import re
import time
from datetime import datetime, timezone

import requests

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SERVICE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
ESBIRKA_API_KEY = os.environ.get("ESBIRKA_REST_API_KEY", "").strip()
ESBIRKA_API_BASE = "https://api.e-sbirka.gov.cz"

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


def check_official_api(cislo, rok):
    """Zepta se oficialniho REST API e-Sbirky (MV CR), jestli predpis
    cislo/rok existuje a je (byval) platny. Vraci dict s 'title' pri
    nalezeni, None pri nenalezeni, nebo 'unavailable' pokud klic neni
    nastaveny nebo API selze - v tom pripade se chovame jako kdyby
    overeni neprobehlo (nechceme false-negative kvuli vypadku API)."""
    if not ESBIRKA_API_KEY:
        return "unavailable"
    stale_url = f"/sb/{rok}/{cislo}"
    encoded = stale_url.replace("/", "%2F")
    try:
        r = requests.get(
            f"{ESBIRKA_API_BASE}/dokumenty-sbirky/{encoded}",
            headers={"esel-api-access-key": ESBIRKA_API_KEY},
            timeout=20,
        )
    except requests.RequestException as e:
        log(f"    (varovani: dotaz na oficialni e-Sbirka API selhal: {e})")
        return "unavailable"
    if r.status_code == 200:
        data = r.json()
        return {
            "title": data.get("uplnaCitace") or data.get("nazev") or f"{cislo}/{rok} Sb.",
            "eli": data.get("eli"),
        }
    if r.status_code == 404:
        return None
    log(f"    (varovani: oficialni e-Sbirka API vratilo stav {r.status_code}, overeni preskakuji)")
    return "unavailable"


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
    if not ESBIRKA_API_KEY:
        log("ESBIRKA_REST_API_KEY neni nastaveny - overeni proti oficialnimu API se preskakuje, "
            "chova se jako drive (jen kontrola proti nasi DB).")

    for req in pending:
        text = req["query_text"]
        m = CITATION_RE.search(text)
        if not m:
            log(f" '{text}': neobsahuje rozpoznatelne cislo predpisu (napr. 141/1961), preskakuji.")
            continue
        cislo, rok = m.group(1), m.group(2)
        citace = f"{cislo}/{rok}"

        doc = find_matching_document(citace)
        if doc:
            log(f" '{text}': jiz v databazi ({doc['title']}), oznacuji jako vyrizene.")
            update_request(req["id"], "fetched", f"Nalezeno v databazi: {doc['title']}")
            continue

        official = check_official_api(cislo, rok)
        time.sleep(1)

        if official == "unavailable":
            log(f" '{text}': v nasi databazi zatim neni, oficialni API nelze overit, zustava cekajici.")
        elif official is None:
            log(f" '{text}': nenalezeno ani v nasi DB, ani v oficialnim registru e-Sbirky.")
            update_request(
                req["id"],
                "not_found",
                "Nenalezeno ani v nasi databazi, ani v oficialnim rejstriku e-Sbirky "
                "(Ministerstvo vnitra CR). Zkontrolujte prosim zadane cislo predpisu "
                "(napr. '89/2012' pro zakon c. 89/2012 Sb.).",
            )
        else:
            log(f" '{text}': potvrzeno oficialnim API e-Sbirky ({official['title']}), "
                f"jeste nenaimportovano do nasi databaze plneho textu, zustava cekajici.")
            update_request(
                req["id"],
                "pending",
                f"Potvrzeno v oficialnim rejstriku e-Sbirky jako platny predpis "
                f"('{official['title']}'). Zatim neni naimportovan plny text do nasi "
                f"databaze - pridano do fronty k rucnimu zpracovani.",
            )


if __name__ == "__main__":
    main()
