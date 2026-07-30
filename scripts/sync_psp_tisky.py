"""
Fetcher duvodovych zprav ke stavajicim zakonum z otevrenych dat Poslanecke
snemovny (psp.cz).

Postup pro kazdy zakon v nasi databazi (documents:doc_type='zakon',
is_current=true), ktery jeste nema pripojenou duvodovou zpravu:

1. Z external_id ("89/2012 Sb.") ziskame cislo a rok a dotazeme se
   https://www.psp.cz/sqw/sbirka.sqw?cz={cislo}&r={rok} - stranka Sbirky
   zakonu obsahuje odkaz na "historie.sqw?o={o}&t={t}", tj. na snemovni tisk,
   ze ktereho zakon vzesel (o = volebni obdobi, t = cislo tisku).
2. Z toho sestavime https://www.psp.cz/sqw/text/tiskt.sqw?o={o}&ct={t}&ct1=0
   - hlavni (puvodni) verze tisku. Stranka obsahuje sekci s nadpisem
   "...zakona vcetne duvodove zpravy" a pod ni odkaz na PDF verzi
   (<a href="orig2.sqw?idd=...", title="Dokument PDF">) - POZOR, tento
   odkaz uz (od cca 2026-07) NEMA parametr "&pdf=1" v URL, jak puvodne
   ocekaval tento skript (zjisteno a opraveno 2026-07-28 - viz find_pdf_idd).
   PDF obsahuje jak text navrhu zakona, tak pripojenou "Duvodovou zpravu"
   (viz dale ve stejnem souboru).
3. Stahneme PDF a pomoci pypdf z nej vytahneme text. Najdeme nadpis
   "Duvodova zprava" (case-insensitive, ignoruje se diakritika u
   "u"/"ů") a vezmeme vse od tohoto mista do konce dokumentu.
4. Ulozime jako novy dokument (doc_type='duvodova_zprava') s vazbou
   documents.explains_document_id na puvodni zakon, a text rozsekame na
   useky stejne jako ostatni fetchery. Embedding dobehne pres existujici
   sync_esbirka_embed.py (bezi napric celou databazi bez ohledu na zdroj).

Ne u kazdeho zakona se podari najit odpovidajici tisk (stara data pred
rokem cca 1993 nemusi byt v teto databazi, nebo tisk nema PDF verzi) -
takove zakony se proste presko a zkusi znovu az v nekterem z dalsich behu
(pripadne muzeme pozdeji pridat i parsovani .doc pro starsi pripady).

MAX_ITEMS omezuje pocet NOVE zpracovanych zakonu za jeden beh (kazdy stoji
3 HTTP pozadavky + parsovani PDF, ktere muze byt u velkych zakoniku
(napr. obcansky zakonik) opravdu velke - kvuli timeoutu GitHub Actions).
"""

import os
import re
import time
from datetime import datetime, timedelta, timezone

import requests
from pypdf import PdfReader
from io import BytesIO

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SERVICE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

MAX_ITEMS = int(os.environ.get("MAX_ITEMS", "30"))
# Zakony jsou razeny podle external_id.asc, coz NENI chronologicky (retezcove
# razeni cisla/roku), a stare zakony (pred cca 1993) casto v psp.cz databazi
# vubec nemaji odpovidajici tisk. Bez tohoto limitu by se skript mohl zaseknout
# na dlouhe serii "nenalezeno" u starych zakonu a za 60 min timeoutu GitHub
# Actions by nestihl zpracovat ani jeden MAX_ITEMS uspech. MAX_ATTEMPTS
# omezuje celkovy pocet PROVERENYCH zakonu (uspesnych i neuspesnych) za beh.
MAX_ATTEMPTS = int(os.environ.get("MAX_ATTEMPTS", "250"))

# Kolik dni si pamatujeme neuspesny pokus (not_found/error), nez ho
# zkusime znovu - brani tomu, aby se ten stejny "nenalezitelny" zakon
# zkousel uplne kazdy den a MAX_ATTEMPTS se tak porad utracel na to same,
# misto aby se posunul k novym, jeste neproverenym zakonum.
RECHECK_AFTER_DAYS = int(os.environ.get("RECHECK_AFTER_DAYS", "30"))

# Tvrdy strop na celkovy cas behu (v sekundach), aby skript vzdy skoncil
# cistě sam, misto aby ho zabil az GitHub Actions 60min timeout.
TIME_BUDGET_SECONDS = int(os.environ.get("TIME_BUDGET_SECONDS", "2700"))
MAX_CHARS_PER_CHUNK = 1500
# Ochrana proti extremne velkym zakonikum (napr. obcansky zakonik ma
# duvodovou zpravu pres 1.5 mil. znaku, tj. ~1000 useku) - to by samo
# spolklo vetsinu denniho embedovaciho rozpoctu na dlouhou dobu. Radeji
# ulozime jen prvni cast (obecna cast + zacatek zvlastni casti), coz byva
# obsahove nejcennejsi, a zbytek oriznem.
MAX_CHARS_TOTAL = int(os.environ.get("MAX_CHARS_TOTAL", "200000"))

SBIRKA_URL = "https://www.psp.cz/sqw/sbirka.sqw"
TISKT_URL = "https://www.psp.cz/sqw/text/tiskt.sqw"
PDF_URL = "https://www.psp.cz/sqw/text/orig2.sqw"

CITACE_RE = re.compile(r"^(\d{1,4})/(\d{4})\s*Sb\.?$")
HISTORIE_RE = re.compile(r"historie\.sqw\?o=(\d+)&(?:amp;)?t=([\w\-]+)", re.IGNORECASE)
# POZOR: puvodni verze tohoto regexu hledala "&pdf=1" v URL - to uz na
# aktualni strance psp.cz vubec neni (zjisteno 2026-07-28 pri rucnim overeni
# proti tisku 637/7). Novy pristup: najit nadpis sekce "...vcetne duvodove
# zpravy" a vzit prvni odkaz na PDF (title="Dokument PDF") za nim - presne
# tak, jak stranka soucasne PDF verzi navrhu zakona+duvodove zpravy oznacuje.
DZ_SECTION_HEADING = "z\u00e1kona v\u010detn\u011b d\u016fvodov\u00e9 zpr\u00e1vy"
PDF_IDD_RE = re.compile(r'orig2\.sqw\?idd=(\d+)"[^>]*title="Dokument PDF"', re.IGNORECASE)
DZ_HEADING_RE = re.compile(r"D[uů]vodov[áa]\s+zpr[áa]va", re.IGNORECASE)

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "ai-semanticke-zakony/1.0 (nekomercni projekt)"})

# psp.cz obcas na kratkou dobu neodpovida / timeoutuje (zejmena z IP adres
# GitHub Actions runneru) - misto rovnou vzdat celý zakon to zkusime jeste
# 2x s malou prodlevou, nez to oznacime za "nenalezeno".
def psp_get(url, params, timeout=20, retries=1):
    # Prvni reala vzorka (2026-07-18) ukazala, ze vetsina "chyb" jsou tiche
    # connect-timeouty, ne docasne vykyvy - dalsi pokusy je nezachranuji,
    # jen prodluzuji beh (2 retries * 45s timeout stalo az 150s na jeden
    # neuspesny zakon). Radsi rychle selhat (kratsi timeout, jen 1 kratky
    # retry) a zkusit vic zakonu za stejny cas, nez utratit vsechen cas
    # opakovanymi pokusy o spojeni, ktere stejne casto neprojde.
    last_exc = None
    for attempt in range(retries + 1):
        try:
            return SESSION.get(url, params=params, timeout=timeout)
        except requests.exceptions.RequestException as e:
            last_exc = e
            if attempt < retries:
                time.sleep(2)
    raise last_exc


def log(*a):
    print(*a, flush=True)


def sb_headers():
    return {
        "apikey": SERVICE_KEY,
        "Authorization": f"Bearer {SERVICE_KEY}",
        "Content-Type": "application/json",
    }


def get_or_create_source():
    r = SESSION.get(
        f"{SUPABASE_URL}/rest/v1/sources",
        headers=sb_headers(),
        params={"select": "id", "code": "eq.psp_tisky"},
        timeout=30,
    )
    r.raise_for_status()
    rows = r.json()
    if rows:
        return rows[0]["id"]
    r = SESSION.post(
        f"{SUPABASE_URL}/rest/v1/sources",
        headers={**sb_headers(), "Prefer": "return=representation"},
        json={
            "code": "psp_tisky",
            "name": "Poslanecka snemovna - snemovni tisky (duvodove zpravy)",
            "base_url": "https://www.psp.cz/sqw/hp.sqw?k=1300",
        },
        timeout=30,
    )
    r.raise_for_status()
    return r.json()[0]["id"]


def get_current_laws():
    """Vsechny aktualne platne zakony, ktere jeste nemaji pripojenou duvodovou zpravu.

    PostgREST vraci defaultne max 1000 radku na dotaz - bez strankovani bychom
    tise videli jen prvni tisicovku (razeno dle external_id.asc), ne vsech
    ~3200 aktualnich zakonu.
    """
    all_rows = []
    page_size = 1000
    offset = 0
    while True:
        r = SESSION.get(
            f"{SUPABASE_URL}/rest/v1/documents",
            headers={**sb_headers(), "Range-Unit": "items", "Range": f"{offset}-{offset + page_size - 1}"},
            params={
                "select": "id,external_id,title,embed_priority",
                "doc_type": "eq.zakon",
                "is_current": "eq.true",
                "order": "embed_priority.desc,external_id.asc",
            },
            timeout=60,
        )
        if r.status_code not in (200, 206):
            r.raise_for_status()
        rows = r.json()
        all_rows.extend(rows)
        if len(rows) < page_size:
            break
        offset += page_size
    return all_rows


def get_existing_explains_ids():
    """id_tisk zakonu (documents.id), ke kterym uz duvodovou zpravu mame."""
    r = SESSION.get(
        f"{SUPABASE_URL}/rest/v1/documents",
        headers=sb_headers(),
        params={"select": "explains_document_id", "doc_type": "eq.duvodova_zprava"},
        timeout=30,
    )
    r.raise_for_status()
    return {row["explains_document_id"] for row in r.json() if row.get("explains_document_id")}


def get_recently_checked_ids():
    """id zakonu, ktere jsme si nedavno (RECHECK_AFTER_DAYS) overili bez uspechu
    (not_found/error) - preskakuji se, aby se kazdy den nezkousel porad ten
    stejny uvaznuty zbytek a MAX_ATTEMPTS se utratil na neco noveho."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=RECHECK_AFTER_DAYS)).isoformat()
    r = SESSION.get(
        f"{SUPABASE_URL}/rest/v1/psp_dz_check_log",
        headers=sb_headers(),
        params={"select": "document_id", "checked_at": f"gte.{cutoff}", "result": "in.(not_found,error)"},
        timeout=30,
    )
    r.raise_for_status()
    return {row["document_id"] for row in r.json()}


def mark_checked(document_id, result):
    """Zapise/aktualizuje vysledek overeni do psp_dz_check_log (upsert). Zapis
    neni kriticky - pri chybe se jen tise preskoci, nechceme kvuli tomu
    shodit cely beh."""
    try:
        SESSION.post(
            f"{SUPABASE_URL}/rest/v1/psp_dz_check_log",
            headers={**sb_headers(), "Prefer": "resolution=merge-duplicates"},
            json={"document_id": document_id, "result": result, "checked_at": datetime.now(timezone.utc).isoformat()},
            timeout=15,
        )
    except requests.exceptions.RequestException:
        pass


def find_historie_link(cislo, rok):
    r = psp_get(SBIRKA_URL, params={"cz": cislo, "r": rok})
    if not r.ok:
        return None
    html = r.content.decode("cp1250", errors="replace")
    m = HISTORIE_RE.search(html)
    if not m:
        return None
    return m.group(1), m.group(2)


def find_pdf_idd(o, t):
    r = psp_get(TISKT_URL, params={"o": o, "ct": t, "ct1": "0"})
    if not r.ok:
        return None
    html = r.content.decode("cp1250", errors="replace")
    heading_idx = html.find(DZ_SECTION_HEADING)
    if heading_idx == -1:
        return None
    m = PDF_IDD_RE.search(html, heading_idx)
    if not m:
        return None
    return m.group(1)


def extract_duvodova_zprava(pdf_bytes):
    reader = PdfReader(BytesIO(pdf_bytes))
    full_text = ""
    dz_start_page = None
    # Nejdriv hledame stranku, kde zacina "Duvodova zprava" jako skutecny
    # nadpis kapitoly - jednoduse bereme prvni vyskyt v cele fronte stranek,
    # cteme postupne a jakmile se objevi, zacneme sbirat text od tohoto bodu.
    collecting = False
    parts = []
    for page in reader.pages:
        try:
            t = page.extract_text() or ""
        except Exception:
            continue
        if not collecting:
            m = DZ_HEADING_RE.search(t)
            if m:
                collecting = True
                parts.append(t[m.start():])
                continue
        else:
            parts.append(t)
    return "\n\n".join(parts).strip()


def split_into_chunks(text, max_chars=None):
    if max_chars is None:
        max_chars = MAX_CHARS_PER_CHUNK
    paragraphs = [p.strip() for p in text.split("\n\n") if p.strip()]
    chunks = []
    current = ""
    for para in paragraphs:
        if current and len(current) + len(para) + 2 > max_chars:
            chunks.append(current)
            current = para
        else:
            current = f"{current}\n\n{para}" if current else para
    if current:
        chunks.append(current)
    return chunks


def save_duvodova_zprava(source_id, law, citace, tiskt_url, text):
    # Dulezite predpisy (obcansky zakonik, trestni zakonik, danovy rad...)
    # maji rucne nastavenou vyssi documents.embed_priority, aby se embedovaly
    # driv nez obrovska obecna fronta. Duvodova zprava k takovemu predpisu at
    # zdedi stejnou prioritu - jinak by cekala na konci fronty spolu se vsemi
    # ostatnimi mene dulezitymi useky.
    doc_payload = {
        "source_id": source_id,
        "doc_type": "duvodova_zprava",
        "external_id": f"dz-{citace}",
        "title": f"Duvodova zprava k zakonu c. {citace} Sb.",
        "url": tiskt_url,
        "status": "platny",
        "explains_document_id": law["id"],
        "embed_priority": law.get("embed_priority") or 0,
    }
    r = SESSION.post(
        f"{SUPABASE_URL}/rest/v1/documents",
        headers={**sb_headers(), "Prefer": "return=representation"},
        json=doc_payload,
        timeout=30,
    )
    if not r.ok:
        log(f"   Ulozeni dokumentu selhalo ({r.status_code}): {r.text[:300]}")
        return 0
    doc_id = r.json()[0]["id"]

    chunks = split_into_chunks(text)
    saved = 0
    for i, chunk in enumerate(chunks):
        chunk_payload = {
            "document_id": doc_id,
            "chunk_index": i,
            "heading": "Duvodova zprava" if i == 0 else None,
            "content": chunk,
        }
        cr = SESSION.post(
            f"{SUPABASE_URL}/rest/v1/chunks",
            headers={**sb_headers(), "Prefer": "return=minimal"},
            json=chunk_payload,
            timeout=30,
        )
        if cr.ok:
            saved += 1
        else:
            log(f"   Ulozeni useku selhalo: {cr.status_code} {cr.text[:200]}")
    return saved


def main():
    start_time = time.time()
    log("=== Duvodove zpravy (psp.cz): start ===")
    log(f"MAX_ITEMS={MAX_ITEMS}")
    source_id = get_or_create_source()

    log("Nacitam existujici vazby...")
    existing = get_existing_explains_ids()
    log(f"Jiz propojenych zakonu: {len(existing)}")

    recently_checked = get_recently_checked_ids()
    log(f"Nedavno bez uspechu overenych (preskakuji): {len(recently_checked)}")

    log("Nacitam seznam aktualne platnych zakonu...")
    laws = get_current_laws()
    log(f"Celkem aktualne platnych zakonu v databazi: {len(laws)}")

    processed = 0
    attempted = 0
    not_found = 0
    errors = 0
    for law in laws:
        if processed >= MAX_ITEMS or attempted >= MAX_ATTEMPTS:
            break
        if time.time() - start_time > TIME_BUDGET_SECONDS:
            log(f"Casovy rozpocet ({TIME_BUDGET_SECONDS}s) vycerpan, koncim cistě.")
            break
        if law["id"] in existing:
            continue
        if law["id"] in recently_checked:
            continue
        m = CITACE_RE.match(law["external_id"] or "")
        if not m:
            continue
        attempted += 1
        cislo, rok = m.group(1), m.group(2)
        citace = f"{cislo}/{rok}"
        try:
            hist = find_historie_link(cislo, rok)
            if not hist:
                not_found += 1
                mark_checked(law["id"], "not_found")
                continue
            o, t = hist
            idd = find_pdf_idd(o, t)
            if not idd:
                not_found += 1
                mark_checked(law["id"], "not_found")
                continue
            tiskt_url = f"{TISKT_URL}?o={o}&ct={t}&ct1=0"
            pdf_r = psp_get(PDF_URL, params={"idd": idd, "pdf": "1"}, timeout=90, retries=0)
            if not pdf_r.ok:
                not_found += 1
                mark_checked(law["id"], "not_found")
                continue
            text = extract_duvodova_zprava(pdf_r.content)
            if not text or len(text) < 200:
                log(f"   {citace}: duvodova zprava nenalezena / prazdna v PDF, preskakuji")
                not_found += 1
                mark_checked(law["id"], "not_found")
                continue
            if len(text) > MAX_CHARS_TOTAL:
                log(f"   {citace}: text ma {len(text)} znaku, oriznuto na {MAX_CHARS_TOTAL}")
                text = text[:MAX_CHARS_TOTAL]
            saved = save_duvodova_zprava(source_id, law, citace, tiskt_url, text)
            processed += 1
            mark_checked(law["id"], "found")
            log(f"   {citace}: ulozeno {saved} useku (tisk {t}/{o})")
        except Exception as e:
            errors += 1
            mark_checked(law["id"], "error")
            log(f"   {citace}: chyba - {e}")
        time.sleep(1)  # slusne tempo dotazu vuci psp.cz

    log(f"=== Hotovo: proverenych {attempted}, zpracovano {processed}, nenalezeno {not_found}, chyb {errors}, cas {time.time()-start_time:.0f}s ===")


if __name__ == "__main__":
    main()
