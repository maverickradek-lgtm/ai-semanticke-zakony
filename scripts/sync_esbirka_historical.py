"""
FAZE A (historicka zneni): dobihajici import SYROVEHO TEXTU jiz
NEAKTUALNICH (ukoncenych) zneni ceskych pravnich predpisu za poslednich
nekolik let (CUTOFF_YEARS, vychozi 5 = od roku 2020) z otevrenych dat
e-Sbirky.

Na rozdil od sync_esbirka_text.py (ktery importuje jen AKTUALNI zneni
kazdeho predpisu) tento skript prochazi 001PravniAktZneni.json.gz -
soupis VSECH historickych zneni vsech predpisu - a doplnuje ta, ktera
jiz skoncila svou ucinnost (byla nahrazena novejsi novelou), ale byla
jeste ucinna nekdy od CUTOFF_DATE.

DULEZITE: vsechny zaznamy se ukladaji s "skip_embedding" = true, aby se
NEPLETLY do denni embeddingove fronty (get_pending_chunks_prioritized
explicitne preskakuje radky se skip_embedding=true). Embedding
historickych zneni se zapne rucne (hromadny UPDATE dokumentu) az bude
hotovy aktualni pravni rad. Samotne STAHOVANI textu zadnou Gemini
kvotu nevyuziva, takze muze bezet uz ted bez ohledu na stav
aktualniho embeddingu.

Zdroj: https://opendata.eselpoint.gov.cz/datove-sady-esbirka/
"""

import bisect
import gzip
import os
import re
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

MAX_SECTIONS_PER_ACT = int(os.environ.get("MAX_SECTIONS_PER_ACT", "3000"))
CUTOFF_DATE = os.environ.get("CUTOFF_DATE", "2020-01-01")
MAX_VERSIONS_PER_RUN = int(os.environ.get("MAX_VERSIONS_PER_RUN", "500"))

# cis-esb-podtyp-pravni-akt-polozka -> nas doc_type (stejna mapa jako v
# sync_esbirka_text.py, musi zustat konzistentni)
SUBTYPE_TO_DOCTYPE = {
    "ZAKON": "zakon",
    "ZAKONUST": "zakon",
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


def load_subtypes():
    """Nacte 006PravniAktMetadata (male, ~5.7 MB) a vrati mapu citace ->
    podtyp (ZAKON/VYHLASKA/...) pro VSECHNY predpisy bez ohledu na to,
    zda jsou jeste platne - historicke zneni potrebujeme i pro uz
    zrusene predpisy."""
    stream = fetch_gunzip_stream("006PravniAktMetadata.json.gz")
    subtypes = {}
    count = 0
    for item in ijson.items(stream, "poloÅ¾ky.item"):
        count += 1
        if count % 20000 == 0:
            log(f"  ...prosel {count} zaznamu metadat (subtypy)")
        if item.get("cis-esb-typ-prÃ¡vnÃ­-akt-poloÅ¾ka") != "PRAVPRED":
            continue
        cit = item.get("akt-citace") or item.get("metadata-citace")
        if not cit or cit in subtypes:
            continue
        subtype = item.get("cis-esb-podtyp-prÃ¡vnÃ­-akt-poloÅ¾ka")
        subtypes[cit] = subtype
    return subtypes


def find_historical_versions(subtypes):
    """Projde 001PravniAktZneni (soupis VSECH zneni vsech predpisu) a
    vrati seznam jiz UKONCENYCH zneni, ktera byla ucinna nekdy od
    CUTOFF_DATE. Aktualne platne zneni (datum-ucinnosti-do je null) se
    preskakuje - to importuje samostatny skript sync_esbirka_text.py."""
    stream = fetch_gunzip_stream("001PravniAktZneni.json.gz")
    found = []
    count = 0
    for item in ijson.items(stream, "poloÅ¾ky.item"):
        count += 1
        if count % 200000 == 0:
            log(f"  ...prosel {count} zaznamu 001, nalezeno {len(found)}")
        if item.get("typ") != "prÃ¡vnÃ­-akt-znÄnÃ­":
            continue
        do = item.get("znÄnÃ­-datum-ÃºÄinnosti-do")
        if not do:
            continue
        if do < CUTOFF_DATE:
            continue
        cit = item.get("akt-citace")
        iri = item.get("iri")
        if not cit or not iri:
            continue
        subtype = subtypes.get(cit)
        doc_type = SUBTYPE_TO_DOCTYPE.get(subtype)
        if not doc_type:
            continue
        found.append({
            "citace": cit,
            "title": item.get("akt-nÃ¡zev-vyhlÃ¡Å¡enÃ½") or cit,
            "version_iri": iri,
            "doc_type": doc_type,
            "valid_from": item.get("znÄnÃ­-datum-ÃºÄinnosti-od"),
            "valid_until": do,
        })
    return found


def scan_version_fragments(version_iris):
    """JEDEN prochod souborem 003 pro VSECHNY pozadovane verze najednou."""
    stream = fetch_gunzip_stream("003PravniAktZneniFragment.json.gz")
    sorted_versions = sorted(version_iris)  # Serazene verze pro binarni vyhledavani

    section_nodes = {v: [] for v in version_iris}
    all_fragments = {v: [] for v in version_iris}

    count = 0
    for item in ijson.items(stream, "poloÅ¾ky.item"):
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
        if cit and re.fullmatch(r"Â§\s*\d+[a-z]?", cit.strip()):
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
    for item in ijson.items(stream, "poloÅ¾ky.item"):
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
    for attempt in range(5):
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
        if r.status_code >= 500:
            wait = 10 * (attempt + 1)
            log(f"   supabase_upsert chyba {r.status_code}, cekam {wait}s a zkusim znovu...")
            time.sleep(wait)
            continue
        if not r.ok:
            log("Supabase chyba:", r.status_code, r.text[:500])
            r.raise_for_status()
        return r.json()
    log("   supabase_upsert selhalo po 5 pokusech (opakovane 5xx), vzdavam se...")
    raise RuntimeError(f"supabase_upsert({table}) selhalo po 5 pokusech na opakovane 5xx chyby")


def get_or_create_source():
    rows = supabase_upsert(
        "sources",
        [{"code": "esbirka", "name": "e-Sbirka", "base_url": "https://opendata.eselpoint.gov.cz"}],
        on_conflict="code",
    )
    return rows[0]["id"]


def get_existing_historical_iris(source_id):
    """Mnozina version_iri, ktera uz byla drive naimportovana jako
    historicka zneni (poznavame je podle version_iri, protoze
    external_id ma umely suffix odvozeny prave od version_iri)."""
    result = set()
    offset = 0
    page = 1000
    while True:
        r = SESSION.get(
            f"{SUPABASE_URL}/rest/v1/documents",
            headers={"apikey": SERVICE_KEY, "Authorization": f"Bearer {SERVICE_KEY}"},
            params={
                "select": "version_iri",
                "source_id": f"eq.{source_id}",
                "is_current": "eq.false",
                "limit": page,
                "offset": offset,
            },
            timeout=60,
        )
        r.raise_for_status()
        rows = r.json()
        for row in rows:
            if row.get("version_iri"):
                result.add(row["version_iri"])
        if len(rows) < page:
            break
        offset += page
    return result


def make_historical_external_id(citace, version_iri):
    return f"{citace}#hist-{version_iri[-40:]}"


def main():
    log("=== Sync e-Sbirka HISTORICKA zneni: start ===")
    log(f"Cutoff: zneni ucinna od {CUTOFF_DATE}")
    source_id = get_or_create_source()

    subtypes = _with_retry(load_subtypes, label="Nacteni metadat pro subtypy (006)")
    all_historical = _with_retry(lambda: find_historical_versions(subtypes), label="Prohledani vsech zneni (001)")
    log(f"Celkem nalezeno historickych zneni od {CUTOFF_DATE}: {len(all_historical)}")

    already_done = get_existing_historical_iris(source_id)
    todo = [h for h in all_historical if h["version_iri"] not in already_done]
    log(f"Jiz naimportovano drive: {len(already_done)}, zbyva: {len(todo)}")

    if not todo:
        log("Nic noveho k importu, konec.")
        return

    batch = todo[:MAX_VERSIONS_PER_RUN]
    log(f"V tomto behu zpracuji {len(batch)} z {len(todo)} zbyvajicich (MAX_VERSIONS_PER_RUN={MAX_VERSIONS_PER_RUN})")

    version_iris = [h["version_iri"] for h in batch]
    by_version = {h["version_iri"]: h for h in batch}

    log(f"-> jeden prochod 003 pro {len(version_iris)} historickych verzi")
    section_nodes_by_version, all_fragments_by_version = _with_retry(
        lambda: scan_version_fragments(version_iris), label="Skenovani fragmentu verzi (003)"
    )

    for v in section_nodes_by_version:
        total = len(section_nodes_by_version[v])
        if total > MAX_SECTIONS_PER_ACT:
            log(f"  ! verze {v}: {total} paragrafu presahuje pojistku, oriznuto na {MAX_SECTIONS_PER_ACT}")
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
    for version_iri, h in by_version.items():
        external_id = make_historical_external_id(h["citace"], version_iri)
        doc_url = ("https://e-sbirka.cz" + version_iri.split("esel-esb:eli/cz")[-1]
                   if "eli/cz" in version_iri else None)

        docs = supabase_upsert(
            "documents",
            [{
                "source_id": source_id,
                "external_id": external_id,
                "doc_type": h["doc_type"],
                "title": h["title"],
                "issuer": "SbÃ­rka zÃ¡konÅ¯",
                "url": doc_url,
                "status": "historicky",
                "version_iri": version_iri,
                "is_current": False,
                "valid_from": h["valid_from"],
                "valid_until": h["valid_until"],
                "skip_embedding": True,
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
        if done % 50 == 0:
            log(f"  ...zpracovano {done}/{len(batch)} historickych zneni")

    log(f"=== Sync e-Sbirka HISTORICKA zneni: hotovo, zpracovano {done} zneni, zbyva jeste {len(todo) - done} ===")

if __name__ == "__main__":
    main()
