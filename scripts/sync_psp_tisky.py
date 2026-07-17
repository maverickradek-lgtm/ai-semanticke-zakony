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
   - hlavni (puvodni) verze tisku, ktera obsahuje na strance odkaz
   "Verze PDF" (orig2.sqw?idd=...&pdf=1). PDF obsahuje jak text navrhu
   zakona, tak pripojenou "Duvodovou zpravu" (viz dale ve stejnem souboru).
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

import requests
from pypdf import PdfReader
from io import BytesIO

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SERVICE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

MAX_ITEMS = int(os.environ.get("MAX_ITEMS", "15"))
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
PDF_IDD_RE = re.compile(r"orig2\.sqw\?idd=(\d+)&(?:amp;)?pdf=1", re.IGNORECASE)
DZ_HEADING_RE = re.compile(r"D[uů]vodov[áa]\s+zpr[áa]va", re.IGNORECASE)

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "ai-semanticke-zakony/1.0 (nekomercni projekt)"})


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
    """Vsechny aktualne platne zakony, ktere jeste nemaji pripojenou duvodovou zpravu."""
    r = SESSION.get(
        f"{SUPABASE_URL}/rest/v1/documents",
        headers=sb_headers(),
        params={
            "select": "id,external_id,title",
            "doc_type": "eq.zakon",
            "is_current": "eq.true",
            "order": "external_id.asc",
        },
        timeout=60,
    )
    r.raise_for_status()
    return r.json()


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


def find_historie_link(cislo, rok):
    r = SESSION.get(SBIRKA_URL, params={"cz": cislo, "r": rok}, timeout=30)
    if not r.ok:
        return None
    html = r.content.decode("cp1250", errors="replace")
    m = HISTORIE_RE.search(html)
    if not m:
        return None
    return m.group(1), m.group(2)


def find_pdf_idd(o, t):
    r = SESSION.get(TISKT_URL, params={"o": o, "ct": t, "ct1": "0"}, timeout=30)
    if not r.ok:
        return None
    html = r.content.decode("cp1250", errors="replace")
    m = PDF_IDD_RE.search(html)
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
    doc_payload = {
        "source_id": source_id,
        "doc_type": "duvodova_zprava",
        "external_id": f"dz-{citace}",
        "title": f"Duvodova zprava k zakonu c. {citace} Sb.",
        "url": tiskt_url,
        "status": "platny",
        "explains_document_id": law["id"],
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
    log("=== Duvodove zpravy (psp.cz): start ===")
    log(f"MAX_ITEMS={MAX_ITEMS}")
    source_id = get_or_create_source()

    log("Nacitam existujici vazby...")
    existing = get_existing_explains_ids()
    log(f"Jiz propojenych zakonu: {len(existing)}")

    log("Nacitam seznam aktualne platnych zakonu...")
    laws = get_current_laws()
    log(f"Celkem aktualne platnych zakonu v databazi: {len(laws)}")

    processed = 0
    not_found = 0
    errors = 0
    for law in laws:
        if processed >= MAX_ITEMS:
            break
        if law["id"] in existing:
            continue
        m = CITACE_RE.match(law["external_id"] or "")
        if not m:
            continue
        cislo, rok = m.group(1), m.group(2)
        citace = f"{cislo}/{rok}"
        try:
            hist = find_historie_link(cislo, rok)
            if not hist:
                not_found += 1
                continue
            o, t = hist
            idd = find_pdf_idd(o, t)
            if not idd:
                not_found += 1
                continue
            tiskt_url = f"{TISKT_URL}?o={o}&ct={t}&ct1=0"
            pdf_r = SESSION.get(PDF_URL, params={"idd": idd, "pdf": "1"}, timeout=120)
            if not pdf_r.ok:
                not_found += 1
                continue
            text = extract_duvodova_zprava(pdf_r.content)
            if not text or len(text) < 200:
                log(f"   {citace}: duvodova zprava nenalezena / prazdna v PDF, preskakuji")
                not_found += 1
                continue
            if len(text) > MAX_CHARS_TOTAL:
                log(f"   {citace}: text ma {len(text)} znaku, oriznuto na {MAX_CHARS_TOTAL}")
                text = text[:MAX_CHARS_TOTAL]
            saved = save_duvodova_zprava(source_id, law, citace, tiskt_url, text)
            processed += 1
            log(f"   {citace}: ulozeno {saved} useku (tisk {t}/{o})")
        except Exception as e:
            errors += 1
            log(f"   {citace}: chyba - {e}")

    log(f"=== Hotovo: zpracovano {processed}, nenalezeno {not_found}, chyb {errors} ===")


if __name__ == "__main__":
    main()
