"""
Denni synchronizace vybranych zakonu z e-Sbirka open data do nasi Supabase
databaze (dokumenty + vyhledatelne useky s embeddingy).

Faze 1: jen mala, rucne vybrana sada zakonu (viz TARGET_CITATIONS), spousteno
zatim jen rucne (workflow_dispatch), ne na cronu, dokud si neoverime spravnost
vysledku.

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

# Phase 1: small, deliberately curated starter set (citace format jako v e-Sbirce)
TARGET_CITATIONS = [
    "89/2012 Sb.",   # obcansky zakonik
]

# Safety cap while we verify correctness - raise once confirmed working
MAX_CHUNKS_PER_ACT = int(os.environ.get("MAX_CHUNKS_PER_ACT", "15"))

EMBED_MODEL = "gemini-embedding-001"
EMBED_DIM = 768

SESSION = requests.Session()


def log(*a):
    print(*a, flush=True)


def fetch_gunzip_stream(path):
    url = f"{BASE}/{path}"
    log(f"-> stahuji {url}")
    r = SESSION.get(url, stream=True, timeout=600)
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


def find_section_nodes(version_iri):
    """Vrati vrcholove uzly s citaci (napr. '§ 1') pro danou verzi zakona,
    serazene podle hierarchie."""
    stream = fetch_gunzip_stream("003PravniAktZneniFragment.json.gz")
    nodes = []
    prefix = version_iri + "/"
    for item in ijson.items(stream, "položky.item"):
        iri = item.get("iri", "")
        if not iri.startswith(prefix):
            continue
        cit = item.get("znění-fragment-citace")
        if not cit or not cit.startswith("§"):
            continue
        nodes.append({
            "iri": iri,
            "citace": cit,
            "url": item.get("znění-fragment-url"),
            "hierarchie_hex": item.get("znění-fragment-hierarchie-hex") or "",
        })
    nodes.sort(key=lambda n: n["hierarchie_hex"])
    return nodes


def find_descendant_fragment_ids(version_iri, section_nodes):
    """Pro kazdy vrcholovy uzel (paragraf) najde VSECHNY potomky (vcetne sebe)
    a jejich fragment-id + hierarchie-hex, aby se dal slozit cely text."""
    stream = fetch_gunzip_stream("003PravniAktZneniFragment.json.gz")
    section_iris = [n["iri"] for n in section_nodes]
    by_section = {iri: [] for iri in section_iris}
    prefix = version_iri + "/"
    for item in ijson.items(stream, "položky.item"):
        iri = item.get("iri", "")
        if not iri.startswith(prefix):
            continue
        for sec_iri in section_iris:
            if iri == sec_iri or iri.startswith(sec_iri + "/"):
                frag_ref = (item.get("právní-akt-fragment") or {}).get("fragment-id")
                if frag_ref is not None:
                    by_section[sec_iri].append({
                        "fragment_id": frag_ref,
                        "hierarchie_hex": item.get("znění-fragment-hierarchie-hex") or "",
                    })
                break
    for sec_iri in by_section:
        by_section[sec_iri].sort(key=lambda x: x["hierarchie_hex"])
    return by_section


def fetch_fragment_texts(all_fragment_ids):
    wanted = set(all_fragment_ids)
    log(f"-> hledam text pro {len(wanted)} fragmentu ve 004PravniAktFragment")
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

    for citace, act in acts.items():
        version_iri = current_version_iri(act)
        if not version_iri:
            log(f"! {citace}: nenalezena aktualni verze, preskakuji")
            continue
        log(f"--- {citace} ({act.get('akt-název-vyhlášený')}) verze {version_iri}")

        title = act.get("akt-název-vyhlášený") or citace
        doc_url = "https://e-sbirka.cz" + version_iri.split("esel-esb:eli/cz")[-1] if "eli/cz" in version_iri else None

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

        section_nodes = find_section_nodes(version_iri)
        log(f"   nalezeno {len(section_nodes)} paragrafu, zpracuji prvnich {MAX_CHUNKS_PER_ACT} (bezpecnostni limit)")
        section_nodes = section_nodes[:MAX_CHUNKS_PER_ACT]

        by_section = find_descendant_fragment_ids(version_iri, section_nodes)
        all_ids = [fid["fragment_id"] for ids in by_section.values() for fid in ids]
        texts_by_id = fetch_fragment_texts(all_ids)

        chunk_rows = []
        for idx, node in enumerate(section_nodes):
            frag_ids = [f["fragment_id"] for f in by_section.get(node["iri"], [])]
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
                "url": None,
            })

        if chunk_rows:
            for row in chunk_rows:
                row.pop("url", None)
            supabase_upsert("chunks", chunk_rows, on_conflict="document_id,chunk_index")
            log(f"   ulozeno {len(chunk_rows)} useku pro {citace}")

    log("=== Sync e-Sbirka: hotovo ===")


if __name__ == "__main__":
    main()
