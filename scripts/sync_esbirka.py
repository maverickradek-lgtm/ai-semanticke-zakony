"""
Denni synchronizace vybranych zakonu z e-Sbirka open data do nasi Supabase
databaze (dokumenty + vyhledatelne useky s embeddingy).

Faze 1: mala, rucne vybrana sada zakonu (viz TARGET_CITATIONS), spousteno
zatim jen rucne (workflow_dispatch), ne na cronu, dokud si neoverime spravnost
vysledku.

Vykonnostni poznamka: soubory 003 (~1.2 GB) a 004 (~500 MB) obsahuji VSECHNY
ceske pravni akty najednou (e-Sbirka nema per-zakon filtr). Kazdy se proto
stahuje a stream-prochazi PRAVE JEDNOU ZA CELY BEH (ne jednou na zakon) -
diky tomu pridani dalsich zakonu do TARGET_CITATIONS nezvysi pocet stahovani.

Zdroj dat: https://opendata.eselpoint.gov.cz/datove-sady-esbirka/
Zadna registrace/API klic neni potreba - jde o volne dostupna otevrena data.
"""

import gzip
import json
import os
import sys
import time

import ijson
import requests

BASE = "https://opendata.eselpoint.gov.cz/datove-sady-esbirka"
SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SERVICE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
ADMIN_USER_ID = os.environ["ADMIN_USER_ID"]  # whose saved Gemini key we reuse for indexing

# Faze 1: mala, zamerne vybrana startovaci sada (citace ve formatu e-Sbirky)
TARGET_CITATIONS = [
    "89/2012 Sb.",   # obcansky zakonik
]

# Bezpecnostni limit, dokud si neoverime spravnost - pak zvysit/odstranit
MAX_CHUNKS_PER_ACT = int(os.environ.get("MAX_CHUNKS_PER_ACT", "15"))

EMBED_MODEL = "gemini-embedding-001"
EMBED_DIM = 768

SESSION = requests.Session()


def log(*a):
    print(*a, flush=True)


def fetch_gunzip_stream(path):
    url = f"{BASE}/{path}"
    log(f"-> stahuji {url}")
    r = SESSION.get(url, stream=True, timeout=900)
    r.raise_for_status()
    return gzip.GzipFile(fileobj=r.raw)


def find_target_acts():
    stream = fetch_gunzip_stream("002PravniAkt.json.gz")
    wanted = set(TARGET_CITATIONS)
    found = {}
    for item in ijson.items(stream, "položky.item"):
        cit = item.get("akt-citace")
        if cit in wanted:
            found[cit] = item
            wanted.discard(cit)
            if not wanted:
                break
    return found


def current_version_iri(act):
    posledni = act.get("právní-akt-znění-poslední") or {}
    return posledni.get("iri")


def scan_version_fragments(version_iris):
    """JEDEN prochod souborem 003 pro VSECHNY pozadovane verze najednou.
    Vraci pro kazdou verzi: seznam vrcholovych uzlu (paragrafy, citace '§ ...')
    a seznam VSECH fragmentu (pro pozdejsi dohledani potomku v pameti,
    bez dalsiho stahovani)."""
    stream = fetch_gunzip_stream("003PravniAktZneniFragment.json.gz")
    prefixes = {v: v + "/" for v in version_iris}

    section_nodes = {v: [] for v in version_iris}   # jen uzly s '§ ...'
    all_fragments = {v: [] for v in version_iris}    # uplne vsechny (pro potomky)

    count = 0
    for item in ijson.items(stream, "položky.item"):
        iri = item.get("iri", "")
        for v, prefix in prefixes.items():
            if not iri.startswith(prefix):
                continue
            frag_ref = (item.get("právní-akt-fragment") or {}).get("fragment-id")
            hierarchie_hex = item.get("znění-fragment-hierarchie-hex") or ""
            if frag_ref is not None:
                all_fragments[v].append({
                    "iri": iri,
                    "fragment_id": frag_ref,
                    "hierarchie_hex": hierarchie_hex,
                })
            cit = item.get("znění-fragment-citace")
            if cit and cit.startswith("§"):
                section_nodes[v].append({
                    "iri": iri,
                    "citace": cit,
                    "hierarchie_hex": hierarchie_hex,
                })
            break
        count += 1
        if count % 500000 == 0:
            log(f"   ...prosel {count} zaznamu 003")

    for v in version_iris:
        section_nodes[v].sort(key=lambda n: n["hierarchie_hex"])
        all_fragments[v].sort(key=lambda f: f["hierarchie_hex"])

    return section_nodes, all_fragments


def group_descendants(section_nodes, all_fragments):
    """V pameti (bez dalsiho stahovani) seskupi fragmenty patrici pod kazdy
    vrcholovy uzel (paragraf) vcetne jeho samotneho."""
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
    for item in ijson.items(stream, "položky.item"):
        fid = item.get("fragment-id")
        if fid in wanted:
            t = item.get("fragment-text")
            if t:
                texts[fid] = t
            wanted.discard(fid)
            if not wanted:
                break
    return texts


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
        raise RuntimeError("Admin nema ulozeny Gemini klic - nutne vlozit v appce v Nastaveni.")
    return key


def embed_text(text, gemini_key, task_type):
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
        r.raise_for_status()
        values = r.json()["embedding"]["values"]
        norm = sum(v * v for v in values) ** 0.5
        if norm > 0:
            values = [v / norm for v in values]
        return values
    raise RuntimeError("Nepovedlo se ziskat embedding po 5 pokusech (rate limit).")


def supabase_upsert(table, rows, on_conflict):
    r = SESSION.post(
        f"{SUPABASE_URL}/rest/v1/{table}?on_conflict={on_conflict}",
        headers={
            "apikey": SERVICE_KEY,
            "Authorization": f"Bearer {SERVICE_KEY}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates,return=representation",
        },
        json=rows,
        timeout=60,
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
    log("=== Sync e-Sbirka: start ===")
    source_id = get_or_create_source()
    gemini_key = get_admin_gemini_key()

    acts = find_target_acts()
    log(f"Nalezeno {len(acts)} z {len(TARGET_CITATIONS)} pozadovanych zakonu")
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
    section_nodes_by_version, all_fragments_by_version = scan_version_fragments(version_iris)

    # Omez na bezpecnostni limit pred dotazovanim textu/embeddingu
    for v in section_nodes_by_version:
        total = len(section_nodes_by_version[v])
        section_nodes_by_version[v] = section_nodes_by_version[v][:MAX_CHUNKS_PER_ACT]
        log(f"   verze {v}: nalezeno {total} paragrafu, zpracuji prvnich {len(section_nodes_by_version[v])}")

    # V pameti seskupit potomky (bez dalsiho stahovani) + posbirat vsechna potrebna fragment-id
    by_section_by_version = {}
    all_needed_fragment_ids = set()
    for v in version_iris:
        by_section = group_descendants(section_nodes_by_version[v], all_fragments_by_version[v])
        by_section_by_version[v] = by_section
        for ids in by_section.values():
            all_needed_fragment_ids.update(ids)

    texts_by_id = fetch_fragment_texts(all_needed_fragment_ids)

    for citace, version_iri in version_iri_by_citace.items():
        act = acts[citace]
        title = act.get("akt-název-vyhlášený") or citace
        doc_url = ("https://e-sbirka.cz" + version_iri.split("esel-esb:eli/cz")[-1]
                   if "eli/cz" in version_iri else None)

        docs = supabase_upsert(
            "documents",
            [{
                "source_id": source_id,
                "external_id": citace,
                "doc_type": "zakon",
                "title": title,
                "issuer": "Sbírka zákonů",
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
                log(f"   ! {node['citace']}: prazdny text, preskakuji")
                continue
            log(f"   {node['citace']}: {content[:80]}...")
            embedding = embed_text(f"{title} {node['citace']}: {content}", gemini_key, "RETRIEVAL_DOCUMENT")
            chunk_rows.append({
                "document_id": document_id,
                "chunk_index": idx,
                "heading": node["citace"],
                "content": content,
                "embedding": embedding,
            })

        if chunk_rows:
            supabase_upsert("chunks", chunk_rows, on_conflict="document_id,chunk_index")
            log(f"   ulozeno {len(chunk_rows)} useku pro {citace}")

    log("=== Sync e-Sbirka: hotovo ===")


if __name__ == "__main__":
    main()
