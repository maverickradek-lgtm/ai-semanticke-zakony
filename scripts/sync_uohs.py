"""
Fetcher pro otevrena data UOHS (Urad pro ochranu hospodarske souteze).

Zdroj: https://uohs.gov.cz/opendata/rozhodnuti.jsonld - jeden velky JSON-LD
soubor se vsemi rozhodnutimi (metadata), plny text je ale az v pripojenem PDF
(pole "dokument"."url"), ne primo v JSON. Proto se u kazdeho noveho zaznamu
PDF stahne a text se z nej extrahuje pomoci pypdf.

Stejne jako ostatni fetchery dela tento skript jen FAZI A (import textu,
embedding=NULL) - embedding dobehne dalsi den pres existujici
sync_esbirka_embed.py, ktery bere pending useky napric celou databazi bez
ohledu na zdroj.

MAX_ITEMS omezuje pocet NOVE stazenych rozhodnuti za jeden beh (kvuli
timeoutu GitHub Actions) - skript preskakuje uz existujici zaznamy podle
external_id, takze dalsi beh pokracuje tam, kde skoncil predchozi.
"""

import os
import re
from io import BytesIO

import requests
from pypdf import PdfReader

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SERVICE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

UOHS_JSONLD_URL = "https://uohs.gov.cz/opendata/rozhodnuti.jsonld"
MAX_CHARS_PER_CHUNK = int(os.environ.get("MAX_CHARS_PER_CHUNK", "4000"))
MAX_ITEMS = int(os.environ.get("MAX_ITEMS", "300"))

SESSION = requests.Session()


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
        params={"select": "id", "code": "eq.uohs"},
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
            "code": "uohs",
            "name": "Urad pro ochranu hospodarske souteze (UOHS)",
            "url": UOHS_JSONLD_URL,
        },
        timeout=30,
    )
    r.raise_for_status()
    return r.json()[0]["id"]


def get_existing_external_ids():
    ids = set()
    offset = 0
    page = 1000
    while True:
        r = SESSION.get(
            f"{SUPABASE_URL}/rest/v1/documents",
            headers=sb_headers(),
            params={
                "select": "external_id",
                "doc_type": "eq.rozhodnuti_uohs",
                "limit": page,
                "offset": offset,
            },
            timeout=60,
        )
        r.raise_for_status()
        rows = r.json()
        ids.update(row["external_id"] for row in rows if row.get("external_id"))
        if len(rows) < page:
            break
        offset += page
    return ids


def extract_pdf_text(pdf_bytes):
    reader = PdfReader(BytesIO(pdf_bytes))
    parts = []
    for page in reader.pages:
        t = page.extract_text() or ""
        parts.append(t)
    text = "\n".join(parts)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def split_into_chunks(text, max_chars=None):
    if max_chars is None:
        max_chars = MAX_CHARS_PER_CHUNK
    paragraphs = [p2.strip() for p2 in text.split("\n\n") if p2.strip()]
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


def upsert_document(source_id, item):
    external_id = item.get("číslo_jednací") or item.get("spisová_značka")
    title = (item.get("věc") or {}).get("cs") or item.get("spisová_značka") or external_id
    dokumenty = item.get("dokument") or []
    if not dokumenty:
        return None, None
    pdf_url = dokumenty[0].get("url")
    if not pdf_url:
        return None, None

    r = SESSION.get(pdf_url, timeout=60)
    if not r.ok:
        log(f"   PDF stahovani selhalo ({r.status_code}): {pdf_url}")
        return None, None
    try:
        text = extract_pdf_text(r.content)
    except Exception as e:
        log(f"   PDF extrakce selhala: {e}")
        return None, None
    if not text or len(text) < 50:
        log(f"   Prazdny/kratky text po extrakci, preskakuji: {external_id}")
        return None, None

    datum = (item.get("datum_právní_moci") or {}).get("datum")

    doc_payload = {
        "source_id": source_id,
        "doc_type": "rozhodnuti_uohs",
        "external_id": external_id,
        "title": title,
        "issuer": "UOHS",
        "decision_date": datum,
        "url": item.get("iri") or pdf_url,
        "status": "platny",
    }
    r = SESSION.post(
        f"{SUPABASE_URL}/rest/v1/documents",
        headers={**sb_headers(), "Prefer": "return=representation"},
        json=doc_payload,
        timeout=30,
    )
    if not r.ok:
        log(f"   Ulozeni dokumentu selhalo ({r.status_code}): {r.text[:300]}")
        return None, None
    doc_id = r.json()[0]["id"]

    chunks = split_into_chunks(text)
    for i, chunk_text in enumerate(chunks):
        heading = f"cast {i + 1}/{len(chunks)}" if len(chunks) > 1 else None
        chunk_payload = {
            "document_id": doc_id,
            "heading": heading,
            "content": chunk_text[:8000],
        }
        cr = SESSION.post(
            f"{SUPABASE_URL}/rest/v1/chunks",
            headers={**sb_headers(), "Prefer": "return=minimal"},
            json=chunk_payload,
            timeout=30,
        )
        if not cr.ok:
            log(f"   Ulozeni useku selhalo: {cr.status_code} {cr.text[:200]}")

    return doc_id, len(chunks)


def main():
    log("=== UOHS sync: start ===")
    log(f"MAX_ITEMS={MAX_ITEMS}")
    source_id = get_or_create_source()
    log("Nacitam existujici external_id z databaze...")
    existing = get_existing_external_ids()
    log(f"Jiz v databazi: {len(existing)} rozhodnuti UOHS")

    log("Stahuji rozhodnuti.jsonld (muze trvat, cca 16 MB)...")
    r = SESSION.get(UOHS_JSONLD_URL, timeout=180)
    r.raise_for_status()
    data = r.json()
    items = data.get("rozhodnutí", [])
    log(f"Nalezeno {len(items)} rozhodnuti v datove sade")

    imported = 0
    skipped = 0
    errors = 0
    for item in items:
        external_id = item.get("číslo_jednací") or item.get("spisová_značka")
        if not external_id or external_id in existing:
            skipped += 1
            continue
        if MAX_ITEMS and imported >= MAX_ITEMS:
            log(f"Dosazen MAX_ITEMS={MAX_ITEMS}, koncim (zbytek doplni dalsi beh).")
            break
        try:
            doc_id, n_chunks = upsert_document(source_id, item)
            if doc_id:
                imported += 1
                existing.add(external_id)
                if imported % 25 == 0:
                    log(f" ...naimportovano {imported}")
            else:
                errors += 1
        except Exception as e:
            log(f"   Chyba u {external_id}: {e}")
            errors += 1

    log(
        f"=== UOHS sync: hotovo, naimportovano {imported}, "
        f"preskoceno {skipped}, chyb {errors} ==="
    )


if __name__ == "__main__":
    main()
