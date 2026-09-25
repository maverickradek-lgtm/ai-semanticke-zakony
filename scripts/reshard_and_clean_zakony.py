"""
Reshardovani + 3fazove ocisteni zakonu (Radek 2026-09-25).

Cil: nahradit 4 puvodni Neon shardy (do1997 / 1998_2007 / 2008_2020 / 2021_dosud)
sadou NOVYCH, mensich shardu, ktere:
  1) maji ocisteny content (bez HTML znacek jako <var>, <a href=...>),
  2) maji spravne prepocitany embedding z ocisteneho textu,
  3) maji predpocitany sloupec search_tsv (rychle plnotextove hledani pres
     cz_light_stem - viz cz_light_stem()/cz_search_text() v Neon shardu
     curly-union-35917887, port Apache Lucene CzechStemmer, ASL 2.0),
  4) se pri plneni ADAPTIVNE deli na novy shard, jakmile by aktualni presahl
     bezpecnostni rozpocet SHARD_BUDGET_BYTES (Radek: vzdy nechat rezervu
     min. 50 MB pod 512 MB free-tier limitem Neonu).

DULEZITE (Radek 2026-09-25): puvodni 4 shardy se NEMAZOU a NEMENI, dokud
neni proces overeny. Aplikace (ai-query) dal ciste z puvodnich shardu az do
rucniho prepnuti na nove shardy - viz PHASE_CUTOVER_TODO na konci souboru.

Beh je resumable a casove rozpoctovany (stejny vzor jako embed_zakony_neon.py) -
da se spoustet opakovane (napr. cron), pokracuje tam, kde skoncil predchozi beh.
"""

import os
import re
import sys
import time
import json
import psycopg2
import requests

# ---------------------------------------------------------------------------
# Konfigurace
# ---------------------------------------------------------------------------

# Prioritni predpisy (Radek 2026-09-25 + 2026-09-25 doplneni) - tyto se maji
# zaembedovat JAKO PRVNI po dobehnuti migrace daneho predpisu do noveho shardu
# (embed_priority=1000), pak teprve nasleduje stavajici prioritizace
# (is_current desc, embed_priority desc, valid_until/valid_from desc - viz
# get_pending_chunks_prioritized() - beze zmeny).
# Format: (cislo_predpisu, rok_predpisu) - presnejsi a spolehlivejsi nez
# fuzzy shoda na nazvu (diakritika, ruzne varianty formulace nazvu apod.).
PRIORITY_PREDPISY = {
    (262, 2006),  # zakonik prace
    (89, 2012),   # obcansky zakonik
    (40, 2009),   # trestni zakonik
    (141, 1961),  # trestni rad
    (119, 2002),  # zakon o strelnych zbranich a strelivu
    (283, 2021),  # stavebni zakon (novy, ucinny od 2024)
    (183, 2006),  # stavebni zakon (stary, pro historicka zneni)
    (361, 2000),  # zakon o provozu na pozemnich komunikacich (silnicni provoz)
    (255, 2012),  # zakon o kontrole (kontrolni rad)
    (320, 2001),  # zakon o financni kontrole ve verejne sprave
    (231, 2025),  # zakon o rizeni a kontrole verejnych financi
    (416, 2004),  # vyhlaska k zakonu o financni kontrole
    (218, 2000),  # rozpoctova pravidla
    (250, 2000),  # rozpoctova pravidla uzemnich rozpoctu
    (420, 2004),  # zakon o prezkoumavani hospodareni USC
    (128, 2000),  # zakon o obcich (obecni zrizeni)
    (129, 2000),  # zakon o krajich (krajske zrizeni)
    (131, 2000),  # zakon o hlavnim meste Praze
    (412, 2021),  # vyhlaska o rozpoctove skladbe
    (433, 2024),  # vyhlaska o financnim vyporadani (aktualni)
    (367, 2015),  # vyhlaska o financnim vyporadani (predchozi)
    (560, 2006),  # vyhlaska o ucasti statniho rozpoctu na financovani programu reprodukce majetku
    (219, 2000),  # zakon o majetku CR
    (62, 2001),   # vyhlaska o hospodareni organizacnich slozek statu
    (134, 2016),  # zakon o zadavani verejnych zakazek
    (340, 2015),  # zakon o registru smluv
    (563, 1991),  # zakon o ucetnictvi
    (410, 2009),  # vyhlaska provadejici zakon o ucetnictvi (vybrane ucetni jednotky)
    (383, 2009),  # technicka vyhlaska o ucetnich zaznamech
    (270, 2010),  # vyhlaska o inventarizaci majetku a zavazku
    (220, 2013),  # vyhlaska o schvalovani ucetnich zaverek
    (280, 2009),  # danovy rad
    (586, 1992),  # zakon o danich z prijmu
    (235, 2004),  # zakon o dani z pridane hodnoty
    (499, 2004),  # zakon o archivnictvi a spisove sluzbe
}
PRIORITY_EMBED_PRIORITY = 1000


SOURCE_SHARDS = {
    # jmeno -> (Neon project_id, DB URL env var)
    "do1997": "green-star-89328754",
    "1998_2007": "young-brook-90913289",
    "2008_2020": "curly-union-35917887",
    "2021_dosud": "nameless-art-23533131",
}

NEON_API_KEY = os.environ["NEON_API_KEY"]
NEON_ORG_ID = os.environ.get("NEON_ORG_ID")  # org-round-meadow-10799977
NEON_API_BASE = "https://console.neon.tech/api/v2"

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SERVICE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
ADMIN_USER_ID = "2648f5db-bea6-4cac-b490-ad0ec59723df"

GEMINI_API_KEY_POOL = [k.strip() for k in os.environ.get("GEMINI_API_KEY_POOL", "").split(",") if k.strip()]
EMBED_MODEL = "gemini-embedding-001"
EMBED_DIM = 256

# Radek: v kazdem novem shardu nechat aspon 50 MB rezervu pod 512MB limitem.
SHARD_HARD_LIMIT_BYTES = 512 * 1024 * 1024
SHARD_SAFETY_MARGIN_BYTES = 50 * 1024 * 1024
SHARD_BUDGET_BYTES = SHARD_HARD_LIMIT_BYTES - SHARD_SAFETY_MARGIN_BYTES  # 462 MB

TIME_BUDGET_SECONDS = int(os.environ.get("TIME_BUDGET_SECONDS", "3000"))
START_TIME = time.time()

# Stavovy soubor (progres migrace) - ulozeny primo v Supabase jako jednoducha
# key/value tabulka `migration_state`, aby beh prezival mezi GitHub Actions
# spustenimi (efemerni runner nema trvaly disk).
STATE_KEY = "reshard_zakony_v1"


def log(*a):
    print(*a)
    sys.stdout.flush()


def time_left():
    return TIME_BUDGET_SECONDS - (time.time() - START_TIME)


# ---------------------------------------------------------------------------
# Cesky "light stemmer" (port Apache Lucene CzechStemmer, ASL 2.0) -
# MUSI byt bit-identicky s Postgres funkci cz_light_stem() pouzivanou pri
# dotazech v ai-query, jinak by se dotaz a index rozesly.
# ---------------------------------------------------------------------------

_CASE_5 = ("atech",)
_CASE_4 = ("ětem", "etem", "atům")
_CASE_3 = ("ech", "ich", "ích", "ého", "ěmi", "emi", "ému", "ěte", "ete", "ěti",
           "eti", "ího", "iho", "ími", "ímu", "imu", "ách", "ata", "aty", "ých",
           "ama", "ami", "ové", "ovi", "ými")
_CASE_2 = ("em", "es", "ém", "ím", "ům", "at", "ám", "os", "us", "ým", "mi", "ou")
_VOWELS_1 = set("aeiouůyáéíý ě".replace(" ", ""))
_POSSESSIVE_2 = ("ov", "in", "ův")


def cz_light_stem(word: str) -> str:
    s = word.lower()
    n = len(s)
    if n > 7 and s.endswith(_CASE_5):
        s = s[:-5]
    elif n > 6 and s.endswith(_CASE_4):
        s = s[:-4]
    elif n > 5 and s.endswith(_CASE_3):
        s = s[:-3]
    elif n > 4 and s.endswith(_CASE_2):
        s = s[:-2]
    elif n > 3 and s[-1] in _VOWELS_1:
        s = s[:-1]
    n = len(s)
    if n > 5 and s.endswith(_POSSESSIVE_2):
        s = s[:-2]
    if not s:
        return s
    n = len(s)
    if s.endswith("čt"):
        return s[:-2] + "ck"
    if s.endswith("št"):
        return s[:-2] + "sk"
    if s[-1] in ("c", "č"):
        return s[:-1] + "k"
    if s[-1] in ("z", "ž"):
        return s[:-1] + "h"
    if n > 1 and s[-2] == "e":
        return s[:-2] + s[-1]
    if n > 2 and s[-2] == "ů":
        return s[:-2] + "o" + s[-1]
    return s


_WORD_RE = re.compile(r"[^a-zá-ěí-ňó-žA-ZÁ-ĚÍ-ŇÓ-Ž0-9]+")
_TAG_RE = re.compile(r"<[^>]+>")


def clean_html(raw: str) -> str:
    """Odstrani HTML znacky (<var>, <a href=...>, atd.) - Fáze 1 cisteni.
    Nahrazuje znackou mezerou (ne prazdnym retezcem), aby se slova na obou
    stranach znacky neslepila dohromady."""
    if not raw:
        return raw or ""
    text = _TAG_RE.sub(" ", raw)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def cz_search_text(clean_text: str) -> str:
    """Stejna logika jako Postgres funkce cz_search_text() - vrati retezec
    stemovanych slov oddelenych mezerou, pro to_tsvector('simple', ...)."""
    tokens = [t for t in _WORD_RE.split(clean_text.lower()) if t]
    return " ".join(cz_light_stem(t) for t in tokens)


# ---------------------------------------------------------------------------
# Gemini embedding (znovupouziti logiky z embed_zakony_neon.py)
# ---------------------------------------------------------------------------

class RateLimitStop(Exception):
    pass


_consecutive_429 = {}
MAX_CONSECUTIVE_429 = 10


def embed_text(text, gemini_key, retries=3, track_key="default"):
    global _consecutive_429
    for attempt in range(retries):
        try:
            resp = requests.post(
                f"https://generativelanguage.googleapis.com/v1beta/models/{EMBED_MODEL}:embedContent",
                headers={"Content-Type": "application/json", "x-goog-api-key": gemini_key},
                json={
                    "content": {"parts": [{"text": (text or "")[:8000]}]},
                    "taskType": "RETRIEVAL_DOCUMENT",
                    "outputDimensionality": EMBED_DIM,
                },
                timeout=30,
            )
            if resp.status_code == 429:
                n = _consecutive_429.get(track_key, 0) + 1
                _consecutive_429[track_key] = n
                if n >= MAX_CONSECUTIVE_429:
                    raise RateLimitStop(f"{n}x 429 pro klic '{track_key}'")
                time.sleep(5 * (attempt + 1))
                continue
            resp.raise_for_status()
            vec = resp.json().get("embedding", {}).get("values")
            _consecutive_429[track_key] = 0
            return vec or None
        except requests.RequestException as e:
            if attempt == retries - 1:
                log(f"   WARN embed_text vyjimka: {e}")
                raise
            time.sleep(3 * (attempt + 1))
    return None


# ---------------------------------------------------------------------------
# Neon API - vytvoreni noveho shardu za behu (adaptivni deleni)
# ---------------------------------------------------------------------------

NEW_SHARD_SCHEMA_SQL = """
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS documents (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    source_id uuid NOT NULL,
    external_id text NOT NULL,
    doc_type text NOT NULL,
    title text NOT NULL,
    issuer text,
    decision_date date,
    effective_date date,
    url text,
    status text,
    content_hash text,
    fetched_at timestamptz NOT NULL DEFAULT now(),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    skip_embedding boolean NOT NULL DEFAULT false,
    embed_priority integer NOT NULL DEFAULT 0,
    version_iri text,
    valid_from date,
    valid_until date,
    superseded_by uuid,
    is_current boolean NOT NULL DEFAULT true,
    explains_document_id uuid,
    predpis_cislo integer,
    predpis_rok integer,
    has_pending_chunks boolean NOT NULL DEFAULT true
);

CREATE TABLE IF NOT EXISTS chunks (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    document_id uuid NOT NULL REFERENCES documents(id),
    chunk_index integer NOT NULL,
    heading text,
    content text,
    embedding vector(256),
    created_at timestamptz NOT NULL DEFAULT now(),
    search_tsv tsvector,
    UNIQUE(document_id, chunk_index)
);

CREATE INDEX IF NOT EXISTS chunks_document_idx ON chunks(document_id);
CREATE INDEX IF NOT EXISTS chunks_embedding_idx ON chunks
    USING hnsw (embedding vector_cosine_ops);
CREATE INDEX IF NOT EXISTS idx_chunks_search_tsv ON chunks USING gin(search_tsv);

-- stejna funkce jako v curly-union-35917887 (match_chunks) - beze zmeny
CREATE OR REPLACE FUNCTION match_chunks(
    query_embedding vector, match_count integer DEFAULT 8,
    min_similarity double precision DEFAULT 0.55, p_as_of date DEFAULT NULL
) RETURNS TABLE(chunk_id uuid, document_id uuid, heading text, content text,
    similarity double precision, doc_title text, doc_url text, doc_type text)
LANGUAGE plpgsql STABLE AS $f$
begin
    return query
    select c.id, c.document_id, c.heading, c.content,
        (1 - (c.embedding <=> query_embedding))::float, d.title, d.url, d.doc_type
    from chunks c join documents d on d.id = c.document_id
    where case when p_as_of is null then d.is_current = true
        else (d.valid_from is null or d.valid_from <= p_as_of)
        and (d.valid_until is null or p_as_of <= d.valid_until) end
        and (1 - (c.embedding <=> query_embedding)) >= min_similarity
    order by c.embedding <=> query_embedding limit match_count;
end;
$f$;
"""
# POZOR: cz_light_stem / cz_search_text funkce se do noveho shardu kopiruji
# samostatne (viz deploy_cz_functions_sql nize) - je to dost dlouhy SQL na
# to, aby byl v teto konstante duplikovany rucne; script ho natahuje ze
# souboru cz_functions.sql (viz REQUIRES). Pred prvnim behem zkopirovat
# definice cz_light_stem/cz_search_text z curly-union-35917887 (uz overene
# a nasazene v teto session) do souboru scripts/sql/cz_functions.sql.


def neon_api(method, path, **kwargs):
    resp = requests.request(
        method, f"{NEON_API_BASE}{path}",
        headers={"Authorization": f"Bearer {NEON_API_KEY}", "Content-Type": "application/json"},
        timeout=60, **kwargs,
    )
    resp.raise_for_status()
    return resp.json()


def create_new_shard(name_suffix: str):
    """Vytvori novy Neon projekt + schema, vrati (project_id, db_url)."""
    body = {
        "project": {
            "name": f"ai-semanticke-zakony-{name_suffix}",
            "region_id": "aws-eu-central-1",
            "pg_version": 18,
        }
    }
    if NEON_ORG_ID:
        body["project"]["org_id"] = NEON_ORG_ID
    data = neon_api("POST", "/projects", json=body)
    project_id = data["project"]["id"]
    conn_uri = data["connection_uris"][0]["connection_uri"]
    log(f"   Vytvoren novy Neon shard '{name_suffix}' (project_id={project_id})")

    conn = psycopg2.connect(conn_uri, connect_timeout=15)
    with conn.cursor() as cur:
        cur.execute(NEW_SHARD_SCHEMA_SQL)
    conn.commit()
    conn.close()
    log(f"   Schema pripravena v '{name_suffix}'")
    return project_id, conn_uri


def get_shard_size_bytes(conn) -> int:
    with conn.cursor() as cur:
        cur.execute("select pg_database_size(current_database())")
        return cur.fetchone()[0]


# ---------------------------------------------------------------------------
# TODO / PHASE_CUTOVER_TODO - co je potreba udelat RUCNE po dobehnuti migrace,
# nez se stare shardy smazou (Radek 2026-09-25: nemazat, dokud neni overeno):
#
#  1. Overit v novych shardech: pocty dokumentu/chunku souhlasi s puvodnimi,
#     nahodny vzorek obsahu je spravne ocisteny (bez HTML), embedding a
#     search_tsv jsou vyplnene u vsech radku.
#  2. V ai-query pridat docasne DEBUG-only cteni z novych shardu vedle
#     stavajicich (stejny vzor jako body.debug v teto session), porovnat
#     kvalitu odpovedi na sade testovacich dotazu.
#  3. Az bude jiste, ze nove shardy funguji spravne a rychleji, prepnout
#     ai-query natvrdo na nove shardy (nove NEON_*_DB_URL secrets).
#  4. Teprve POTE smazat puvodni 4 shardy (do1997/1998_2007/2008_2020/
#     2021_dosud) a uvolnit jejich Neon projekty.
#  5. Znovu zapnout planovac v embed-zakony-neon.yml (nebo jeho naslednika
#     pro nove shardy), az bude bezici migrace u konce.
# ---------------------------------------------------------------------------


def main():
    log("Tento skript je navrzen jako kostra/zaklad pro reshardovani + cisteni.")
    log("Pred prvnim ostrym behem je potreba jeste:")
    log(" 1) zkopirovat definice cz_light_stem/cz_search_text do scripts/sql/cz_functions.sql")
    log(" 2) nastavit NEON_API_KEY (Neon 'Personal API key' s pravem vytvaret projekty)"
    log(" 3) rozhodnout poradi zpracovani dokumentu (navrhuji: chronologicky podle")
    log("    valid_from/valid_until, is_current dokumenty nakonec/samostatne)")
    log("Viz komentare v souboru pro dalsi kroky.")


if __name__ == "__main__":
    main()
