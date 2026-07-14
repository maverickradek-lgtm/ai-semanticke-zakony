"""
FAZE A: hromadny import SYROVEHO TEXTU vsech aktualne platnych ceskych
pravnich predpisu (zakony, ustavni zakony, vyhlasky, narizeni vlady,
opatreni, historicke dekrety prezidenta republiky - vse, co e-Sbirka eviduje
jako typ "pravni predpis" a NENI zruseno/jiz neucinne) z otevrenych dat
e-Sbirky do Supabase (documents + chunks).

DULEZITE: tato faze NEPOCITA embeddingy (to dela az sync_esbirka_embed.py na
denni bazi, protoze bezplatny Gemini klic ma denni limit poctu volani).
Radky v "chunks" se tedy ukladaji s embedding = NULL a doplni se pozdeji -
diky tomu se text objevi v databazi rychle, bez cekani na embedding limit.

Zdroj: https://opendata.eselpoint.gov.cz/datove-sady-esbirka/
Zadna registrace/API klic neni potreba - jde o volne dostupna otevrena data.
"""

import bisect
import gzip
import json
import os
import re
import sys
import tempfile
import time
from datetime import date
from http.client import IncompleteRead as HTTPIncompleteRead

import ijson
import requests
from requests.exceptions import ChunkedEncodingError
from urllib3.exceptions import IncompleteRead as Urllib3IncompleteRead
from urllib3.exceptions import ProtocolError

BASE = "https://opendata.eselpoint.gov.cz/datove-sady-esbirka"
SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SERVICE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

# Bezpecnostni pojistka proti patologickym pripadum (mel by byt vzdy vyssi
# nez realny pocet paragrafu i u nejvetsich zakoniku)
MAX_SECTIONS_PER_ACT = int(os.environ.get("MAX_SECTIONS_PER_ACT", "3000"))

# cis-esb-podtyp-pravni-akt-polozka -> nas doc_type
SUBTYPE_TO_DOCTYPE = {
    "ZAKON": "zakon",
    "ZAKONUST": "zakon",      # ustavni zakon
    "VYHLASKA": "vyhlaska",
    "NARIZENI": "narizeni",
    "OPATRSEN": "opatreni",
    "DEKRET": "dekret",
    "DEKRETUST": "dekret",
}

SESSION = requests.Session()

_NETWORK_ERRORS = (
    HTTPIncompleteRead,
    Urllib3IncompleteRead,
    ProtocolError,
    ChunkedEncodingError,
    requests.exceptions.ConnectionError,
    requests.exceptions.ReadTimeout,
)

def log(*a):
    print(*a, flush=True)

def _with_retry(fn, max_retries=3, label="Operace"):
    """Spusti fn(), pri sitovych chybach opakuje s exponential backoff."""
    for attempt in range(max_retries):
        try:
            return fn()
        except _NETWORK_ERRORS as e:
            if attempt == max_retries - 1:
                raise
            wait = 30 * (attempt + 1)
            log(f"{label} prerusena (pokus {attempt + 1}/{max_retries}), cekam {wait}s: {e}")
            time.sleep(wait)

def _download_with_resume(url, tmp_path, max_retries=50):
    """Stahne soubor do tmp_path s podporou resume pres HTTP Range requests.
    Pri preruseni TCP spojeni pokracuje od mista kde skoncil, ne od zacatku.
    Vhodne pro velke soubory (napr. 003PravniAktZneniFragment.json.gz ~ 1.16 GB)
    kde server opendata.eselpoint.gov.cz prerusuji spojeni po 5-30 MB."""
    for attempt in range(max_retries):
        existing_size = os.path.getsize(tmp_path) if os.path.exists(tmp_path) else 0
        headers = {}
        if existing_size > 0:
            headers['Range'] = f'bytes={existing_size}-'
            log(f'  Resume od {existing_size:,} B (pokus {attempt + 1}/{max_retries})')

        try:
            with SESSION.get(url, headers=headers, stream=True, timeout=1800) as r:
                if r.status_code == 416:
                    # Range Not Satisfiable - soubor je uz kompletni
                    log(f'  Status 416: soubor je kompletni ({os.path.getsize(tmp_path):,} B)')
                    return

                r.raise_for_status()

                # Zjisti celkovou velikost souboru
                content_range = r.headers.get('Content-Range', '')
                total = None
                if content_range:
                    try:
                        total = int(content_range.split('/')[-1])
                    except (ValueError, IndexError):
                        pass
                elif 'Content-Length' in r.headers:
                    total = existing_size + int(r.headers['Content-Length'])

                mode = 'ab' if existing_size > 0 else 'wb'
                with open(tmp_path, mode) as f:
                    for chunk in r.iter_content(chunk_size=512 * 1024):  # 512 KB chunks
                        if chunk:
                            f.write(chunk)

                current_size = os.path.getsize(tmp_path)
                if total is None or current_size >= total:
                    log(f'  Stazeno kompletne: {current_size:,} B')
                    return
                else:
                    log(f'  Preruseno na {current_size:,}/{total:,} B, zkousim znovu...')

        except _NETWORK_ERRORS as e:
            current_size = os.path.getsize(tmp_path) if os.path.exists(tmp_path) else 0
            log(f'  Chyba na {current_size:,} B: {e}, zkousim znovu...')
            time.sleep(5)
            continue

    raise RuntimeError(f"Nepodarilo se stahnout {url} po {max_retries} pokusech")


def fetch_gunzip_stream(path):
    """Stahne .gz soubor s podporou resume a vrati dekomprimovany stream.

    Soubor se nejprve cely stahne do docasneho souboru s podporou HTTP Range
    requests (pri preruseni pokracuje od mista kde skoncil), pak se otevr
    jako gzip stream pro ijson. Docasny soubor je okamzite unlinkovany -
    na Linuxu data zustavaji dostupna pres file descriptor az do zavreni.
    """
    url = f"{BASE}/{path}"
    log(f"-> stahuji {url}")

    tmp_fd, tmp_path = tempfile.mkstemp(suffix='.gz.part')
    os.close(tmp_fd)

    try:
        _download_with_resume(url, tmp_path)
        gz = gzip.open(tmp_path, 'rb')
        # Unlink ihned po otevreni - na Linuxu data zustavaji dostupna
        # pres file descriptor i po unlink (data se uvolni az pri zavreni fd)
        os.unlink(tmp_path)
        return gz
    except Exception:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
        raise


def find_valid_citace():
    """Nacte 006PravniAktMetadata a vrati mnozinu citaci aktualne platnych
    pravnich predpisu (typ PRAVPRED, neni zruseno, ucinnost nekonci v
    minulosti) spolu s jejich podtypem pro urceni doc_type."""
    stream = fetch_gunzip_stream("006PravniAktMetadata.json.gz")
    today = date.today().isoformat()
    valid = {}
    count = 0
    for item in ijson.items(stream, "položky.item"):
        count += 1
        if count % 10000 == 0:
            log(f"  ...prosel {count} zaznamu metadat")
        if item.get("cis-esb-typ-právní-akt-položka") != "PRAVPRED":
            continue
        if item.get("metadata-datum-zrušení"):
            continue
        ucinnost_do = item.get("metadata-datum-účinnosti-do")
        if ucinnost_do and ucinnost_do <= today:
            continue
        cit = item.get("akt-citace") or item.get("metadata-citace")
        if not cit:
            continue
        subtype = item.get("cis-esb-podtyp-právní-akt-položka")
        valid[cit] = {
            "doc_type": SUBTYPE_TO_DOCTYPE.get(subtype, "jiny_predpis"),
            "title": item.get("metadata-název") or item.get("akt-název-vyhlášený") or cit,
        }
    log(f"Nalezeno {len(valid)} aktualne platnych pravnich predpisu (z {count} zaznamu historie)")
    return valid

def find_acts(valid_citace):
    """Jeden prochod 002PravniAkt - pro kazdou platnou citaci najde IRI
    aktualniho zneni (potreba pro napojeni na 003/004)."""
    stream = fetch_gunzip_stream("002PravniAkt.json.gz")
    wanted = set(valid_citace.keys())
    found = {}
    count = 0
    for item in ijson.items(stream, "položky.item"):
        count += 1
        if count % 10000 == 0:
            log(f"  ...prosel {count} zaznamu katalogu aktu")
        cit = item.get("akt-citace")
        if cit in wanted:
            found[cit] = item
            wanted.discard(cit)
            if not wanted:
                break
    log(f"Sparovano {len(found)} z {len(valid_citace)} platnych predpisu s katalogem aktu")
    return found

def current_version_iri(act):
    posledni = act.get("právní-akt-znění-poslední") or {}
    return posledni.get("iri")

def scan_version_fragments(version_iris):
    """JEDEN prochod souborem 003 pro VSECHNY pozadovane verze najednou."""
    stream = fetch_gunzip_stream("003PravniAktZneniFragment.json.gz")
    sorted_versions = sorted(version_iris)  # Serazene verze pro binarni vyhledavani

    section_nodes = {v: [] for v in version_iris}
    all_fragments = {v: [] for v in version_iris}

    count = 0
    for item in ijson.items(stream, "položky.item"):
        count += 1
        if count % 1_000_000 == 0:
            log(f"  ...prosel {count} zaznamu 003")
        iri = item.get("iri", "")
        if not iri:
            continue

        # O(log n) binarni vyhledavani misto O(n) linearniho pruchodu
        idx = bisect.bisect_right(sorted_versions, iri) - 1
        if idx < 0:
            continue
        v = sorted_versions[idx]
        if not iri.startswith(v + "/"):
            continue

        # Nalezena shoda - zpracuj zaznam
        frag_ref = (item.get("pr\u00e1vn\u00ed-akt-fragment") or {}).get("fragment-id")
        hierarchie_hex = item.get("zn\u011bn\u00ed-fragment-hierarchie-hex") or ""
        if frag_ref is not None:
            all_fragments[v].append({
                "iri": iri,
                "fragment_id": frag_ref,
                "hierarchie_hex": hierarchie_hex,
            })
        cit = item.get("zn\u011bn\u00ed-fragment-citace")
        if cit and re.fullmatch(r"§\s*\d+[a-z]?", cit.strip()):
            section_nodes[v].append({
                "iri": iri,
                "citace": cit,
                "hierarchie_hex": hierarchie_hex,
            })

    for v in version_iris:
        section_nodes[v].sort(key=lambda n: n["hierarchie_hex"])
        all_fragments[v].sort(key=lambda f: f["hierarchie_hex"])

    return section_nodes, all_fragments
def group_descendants(section_nodes, all_fragments):
    by_section = {}
    for node in section_nodes:
        node_iri = node["iri"]
        prefix = node_iri + "/"
        descendants = [
            f["fragment_id"] for f in all_fragments
            if f["iri"] == node_iri or f["iri"].startswith(prefix)
        ]
        by_section[node_iri] = descendants
    return by_section

def fetch_fragment_texts(all_fragment_ids):
    wanted = set(all_fragment_ids)
    log(f"-> hledam text pro {len(wanted)} fragmentu ve 004PravniAktFragment (1 prochod)")
    stream = fetch_gunzip_stream("004PravniAktFragment.json.gz")
    texts = {}
    count = 0
    for item in ijson.items(stream, "položky.item"):
        count += 1
        if count % 1000000 == 0:
            log(f"  ...prosel {count} zaznamu 004")
        fid = item.get("fragment-id")
        if fid in wanted:
            t = item.get("fragment-text")
            if t:
                texts[fid] = t
            wanted.discard(fid)
            if not wanted:
                break
    return texts

def supabase_upsert(table, rows, on_conflict):
    if not rows:
        return []
    r = SESSION.post(
        f"{SUPABASE_URL}/rest/v1/{table}?on_conflict={on_conflict}",
        headers={
            "apikey": SERVICE_KEY,
            "Authorization": f"Bearer {SERVICE_KEY}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates,return=representation",
        },
        json=rows,
        timeout=120,
    )
    if not r.ok:
        log("Supabase chyba:", r.status_code, r.text[:500])
        r.raise_for_status()
    return r.json()

def get_or_create_source():
    rows = supabase_upsert(
        "sources",
        [{"code": "esbirka", "name": "e-Sbirka", "base_url": "https://opendata.eselpoint.gov.cz"}],
        on_conflict="code",
    )
    return rows[0]["id"]

def main():
    log("=== Sync e-Sbirka TEXT (faze A): start ===")
    source_id = get_or_create_source()

    valid_citace = _with_retry(find_valid_citace, label="Nacteni metadat (006)")
    if not valid_citace:
        log("Nic k zpracovani, konec.")
        return
    acts = _with_retry(lambda: find_acts(valid_citace), label="Nacteni katalogu aktu (002)")
    if not acts:
        log("Nic k zpracovani, konec.")
        return

    version_iri_by_citace = {}
    for citace, act in acts.items():
        vi = current_version_iri(act)
        if vi:
            version_iri_by_citace[citace] = vi
        else:
            log(f"! {citace}: nenalezena aktualni verze, preskakuji")

    version_iris = list(version_iri_by_citace.values())
    log(f"-> jeden prochod 003 pro {len(version_iris)} verzi soucasne")
    section_nodes_by_version, all_fragments_by_version = _with_retry(
        lambda: scan_version_fragments(version_iris), label="Skenovani fragmentu verzi (003)"
    )

    for v in section_nodes_by_version:
        total = len(section_nodes_by_version[v])
        if total > MAX_SECTIONS_PER_ACT:
            log(f"  ! verze {v}: {total} paragrafu presahuje pojistku {MAX_SECTIONS_PER_ACT}, oriznuto")
            section_nodes_by_version[v] = section_nodes_by_version[v][:MAX_SECTIONS_PER_ACT]

    by_section_by_version = {}
    all_needed_fragment_ids = set()
    for v in version_iris:
        by_section = group_descendants(section_nodes_by_version[v], all_fragments_by_version[v])
        by_section_by_version[v] = by_section
        for ids in by_section.values():
            all_needed_fragment_ids.update(ids)

    texts_by_id = _with_retry(
        lambda: fetch_fragment_texts(all_needed_fragment_ids),
        label="Nacteni textu fragmentu (004)"
    )

    done = 0
    for citace, version_iri in version_iri_by_citace.items():
        act = acts[citace]
        meta = valid_citace[citace]
        title = meta["title"]
        doc_url = ("https://e-sbirka.cz" + version_iri.split("esel-esb:eli/cz")[-1]
                   if "eli/cz" in version_iri else None)

        docs = supabase_upsert(
            "documents",
            [{
                "source_id": source_id,
                "external_id": citace,
                "doc_type": meta["doc_type"],
                "title": title,
                "issuer": "Sbírka zákonş",
                "url": doc_url,
                "status": "platny",
            }],
            on_conflict="source_id,external_id",
        )
        document_id = docs[0]["id"]

        section_nodes = section_nodes_by_version[version_iri]
        by_section = by_section_by_version[version_iri]

        chunk_rows = []
        for idx, node in enumerate(section_nodes):
            frag_ids = by_section.get(node["iri"], [])
            parts = [texts_by_id[fid] for fid in frag_ids if fid in texts_by_id]
            content = " ".join(parts).strip()
            if not content:
                continue
            chunk_rows.append({
                "document_id": document_id,
                "chunk_index": idx,
                "heading": node["citace"],
                "content": content,
                "embedding": None,
            })

        if chunk_rows:
            supabase_upsert("chunks", chunk_rows, on_conflict="document_id,chunk_index")

        done += 1
        if done % 200 == 0:
            log(f"  ...zpracovano {done}/{len(version_iri_by_citace)} predpisu")

    log(f"=== Sync e-Sbirka TEXT: hotovo, zpracovano {done} predpisu ===")

if __name__ == "__main__":
    main()
