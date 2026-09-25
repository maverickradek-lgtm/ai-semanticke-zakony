"""
Reshardovani + 3fazove ocisteni zakonu (Radek 2026-09-25).

Cil: nahradit 4 puvodni Neon shardy (do1997 / 1998_2007 / 2008_2020 / 2021_dosud)
sadou NOVYCH, mensich shardu, ktere:
  1) maji ocisteny content (bez HTML znacek jako <var>, <a href=...>),
  2) maji predpocitany sloupec search_tsv (rychle plnotextove hledani pres
     cz_light_stem - port Apache Lucene CzechStemmer, ASL 2.0, overeno
     v teto session - viz cz_light_stem()/cz_search_text() nize),
  3) se pri plneni ADAPTIVNE deli na novy shard, jakmile by aktualni presahl
     bezpecnostni rozpocet SHARD_BUDGET_BYTES (Radek: vzdy nechat rezervu
     min. 50 MB pod 512 MB free-tier limitem Neonu).

DULEZITE (Radek 2026-09-25):
  - Puvodni 4 shardy se NEMAZOU a NEMENI, dokud neni proces overeny.
    Aplikace (ai-query) dal ciste z puvodnich shardu az do rucniho prepnuti
    na nove shardy - viz PHASE_CUTOVER_TODO na konci souboru. Konkretne:
    kazdy PREDPIS (dokument) zustava dostupny ve SVE PUVODNI (spatne
    zaembedovane) podobe, dokud neni ve svem NOVEM shardu skutecne
    zaembedovan (embedding NOT NULL) - migrace samotna (kopie+cisteni)
    tohle jeste negarantuje, teprve embedding.
  - Nove shardy zaklada Radek RUCNE pres Neon nastroj (ne tento skript -
    zadny NEON_API_KEY tedy neni potreba). Skript pracuje s pevnym
    seznamem jiz vytvorenych cilovych shardu (TARGET_SHARDS) a kdyz mu
    dojde misto v poslednim z nich, ZASTAVI SE a jasne o tom napise do
    logu - neni to chyba, je to signal "zaloz dalsi shard a pridej ho
    do TARGET_SHARDS".
  - Poradi je DVOJI a ZAMERNE ROZDILNE:
      * MIGRACE (cisteni+kopie, tento skript) jde CHRONOLOGICKY od
        nejstarsich predpisu k nejnovejsim - viz fetch_source_documents().
      * EMBEDDING (samostatny navazujici skript/beh, viz PRIORITY_PREDPISY)
        jde nejdriv podle prioritniho seznamu klicovych zakonu, pak teprve
        podle stavajici prioritizace (is_current desc, embed_priority desc,
        valid_until/valid_from desc - get_pending_chunks_prioritized(),
        beze zmeny). Tento skript proto pri migraci nastavuje embed_priority
        vysoko (PRIORITY_EMBED_PRIORITY) pro dokumenty z PRIORITY_PREDPISY,
        aby to navazujici embedovaci beh respektoval automaticky.

Beh je resumable a casove rozpoctovany (stejny vzor jako embed_zakony_neon.py) -
da se spoustet opakovane (napr. cron), pokracuje tam, kde skoncil predchozi beh
(sleduje se pres uz-migrovane document_id primo v cilovych shardech).
"""

import os
import re
import sys
import time
import psycopg2
import psycopg2.extras
import requests

# ---------------------------------------------------------------------------
# Konfigurace
# ---------------------------------------------------------------------------

# Prioritni predpisy (Radek 2026-09-25 + doplneni tyz den) - tyto se maji
# zaembedovat JAKO PRVNI, jakmile jsou v novem shardu (embed_priority=1000),
# pak teprve nasleduje stavajici prioritizace. Format: (cislo, rok) Sb. -
# presnejsi a spolehlivejsi nez fuzzy shoda na nazvu.
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

# Zdrojove (puvodni, NEMENENE) shardy - migrace z nich jen CTE, nikdy nezapisuje.
SOURCE_SHARDS = [
    # (jmeno, Neon project_id) - poradi je jen informativni, skutecne poradi
    # zpracovani urcuje fetch_source_documents() (chronologicky pres VSECHNY).
    ("do1997", "green-star-89328754"),
    ("1998_2007", "young-brook-90913289"),
    ("2008_2020", "curly-union-35917887"),
    ("2021_dosud", "nameless-art-23533131"),
]

# Cilove (nove, CISTE) shardy - Radek je zaklada RUCNE pres Neon nastroj;
# skript do nich zapisuje v tomto poradi, dokud se nezaplni (viz
# SHARD_BUDGET_BYTES), pak pokracuje dalsim v seznamu. Kdyz dojdou, skript
# se zastavi s jasnou hlaskou - NENI to auto-provisioning (viz DULEZITE
# na zacatku souboru).
#   env var s connection stringem se ocekava jako NEON_RESHARD_<KEY>_DB_URL
TARGET_SHARDS = [
    # (jmeno, env var s DB URL)
    ("reshard-01", "NEON_RESHARD_01_DB_URL"),  # delicate-brook-34508314
]

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")

GEMINI_API_KEY_POOL = [k.strip() for k in os.environ.get("GEMINI_API_KEY_POOL", "").split(",") if k.strip()]
EMBED_MODEL = "gemini-embedding-001"
EMBED_DIM = 256

# Radek: v kazdem novem shardu nechat aspon 50 MB rezervu pod 512MB limitem.
SHARD_HARD_LIMIT_BYTES = 512 * 1024 * 1024
SHARD_SAFETY_MARGIN_BYTES = 50 * 1024 * 1024
SHARD_BUDGET_BYTES = SHARD_HARD_LIMIT_BYTES - SHARD_SAFETY_MARGIN_BYTES  # 462 MB

DOCS_PER_BATCH = int(os.environ.get("DOCS_PER_BATCH", "25"))
TIME_BUDGET_SECONDS = int(os.environ.get("TIME_BUDGET_SECONDS", "3000"))
START_TIME = time.time()


def log(*a):
    print(*a)
    sys.stdout.flush()


def time_left():
    return TIME_BUDGET_SECONDS - (time.time() - START_TIME)


# ---------------------------------------------------------------------------
# Cesky "light stemmer" (port Apache Lucene CzechStemmer, ASL 2.0) -
# MUSI byt bit-identicky s Postgres funkci cz_light_stem() pouzivanou v
# ai-query, jinak by se dotaz a index rozesly. Overeno v teto session proti
# realnym datum (napr. "byt"/"bytu"/"bytech"/"bytu" -> "byt").
# ---------------------------------------------------------------------------

_CASE_5 = ("atech",)
_CASE_4 = ("ětem", "etem", "atům")
_CASE_3 = ("ech", "ich", "ích", "ého", "ěmi", "emi", "ému", "ěte", "ete", "ěti",
           "eti", "ího", "iho", "ími", "ímu", "imu", "ách", "ata", "aty", "ých",
           "ama", "ami", "ové", "ovi", "ými")
_CASE_2 = ("em", "es", "ém", "ím", "ům", "at", "ám", "os", "us", "ým", "mi", "ou")
_VOWELS_1 = set("aeiouůyáéíýě")
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
_WS_RE = re.compile(r"\s+")


def clean_html(raw: str) -> str:
    """Odstrani HTML znacky (<var>, <a href=...>, atd.) - Faze 1 cisteni.
    Nahrazuje znackou mezerou (ne prazdnym retezcem), aby se slova na obou
    stranach znacky neslepila dohromady. Overeno v teto session na realnem
    dokumentu (zakon 143/1919 Sb.)."""
    if not raw:
        return raw or ""
    text = _TAG_RE.sub(" ", raw)
    text = _WS_RE.sub(" ", text).strip()
    return text


def cz_search_text(clean_text: str) -> str:
    """Stejna logika jako Postgres funkce cz_search_text() v cilovych
    shardech - vrati retezec stemovanych slov oddelenych mezerou, pro
    to_tsvector('simple', ...)."""
    tokens = [t for t in _WORD_RE.split(clean_text.lower()) if t]
    return " ".join(cz_light_stem(t) for t in tokens)


# ---------------------------------------------------------------------------
# DB pripojeni
# ---------------------------------------------------------------------------

def connect_source(project_id_unused, name):
    """Pripojeni ke zdrojovemu shardu - ocekava env var
    NEON_ZAKONY_<NAME_UPPER>_DB_URL (stejne jmeno jako pouziva stavajici
    embed_zakony_neon.py, aby slo znovupouzit uz existujici GitHub secrets)."""
    env_key = f"NEON_ZAKONY_{name.upper()}_DB_URL"
    url = os.environ[env_key]
    return psycopg2.connect(url, connect_timeout=15)


def connect_target(env_key, retries=3):
    url = os.environ[env_key]
    last_err = None
    for attempt in range(retries):
        try:
            return psycopg2.connect(url, connect_timeout=15)
        except Exception as e:
            last_err = e
            log(f"   WARN pripojeni k cilovemu shardu selhalo (pokus {attempt+1}/{retries}): {e}")
            time.sleep(3)
    raise last_err


def get_shard_size_bytes(conn) -> int:
    with conn.cursor() as cur:
        cur.execute("select pg_database_size(current_database())")
        return cur.fetchone()[0]


def ensure_migration_tracking(conn):
    """V cilovem shardu potrebujeme vedet, ktere document_id uz maji svuj
    puvodni protejsek zmigrovany (idempotence pri opakovanych behich) -
    document.id se pri migraci ZACHOVAVA (kopiruje 1:1), takze staci
    kontrolovat existenci."""
    with conn.cursor() as cur:
        cur.execute("select id from documents")
        return {row[0] for row in cur.fetchall()}


# ---------------------------------------------------------------------------
# Cteni ze zdroje - CHRONOLOGICKY (nejstarsi napred), napric vsemi 4 shardy
# ---------------------------------------------------------------------------

def fetch_source_documents(conn, already_migrated_ids, limit):
    """Vraci az `limit` dosud nezmigrovanych dokumentu z jednoho zdrojoveho
    shardu, serazenych chronologicky (nejstarsi napred podle valid_from,
    pak valid_until, pak created_at jako tie-breaker)."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            select id, source_id, external_id, doc_type, title, issuer,
                decision_date, effective_date, url, status, content_hash,
                valid_from, valid_until, is_current, predpis_cislo, predpis_rok
            from documents
            order by coalesce(valid_from, valid_until, '1900-01-01'::date) asc,
                valid_until asc nulls last, created_at asc
            limit %s
            """,
            (limit * 3,),  # nabereme vic, protoze cast uz muze byt migrovana
        )
        rows = cur.fetchall()
    out = [r for r in rows if r["id"] not in already_migrated_ids]
    return out[:limit]


def fetch_chunks_for_document(conn, document_id):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "select id, chunk_index, heading, content from chunks "
            "where document_id = %s order by chunk_index",
            (document_id,),
        )
        return cur.fetchall()


# ---------------------------------------------------------------------------
# Zapis do cile - vycisteny content + search_tsv, embedding NULL (doplni ho
# az navazujici embedovaci beh - viz DULEZITE na zacatku souboru)
# ---------------------------------------------------------------------------

def write_document_and_chunks(target_conn, doc, chunks):
    priority_key = (doc.get("predpis_cislo"), doc.get("predpis_rok"))
    embed_priority = PRIORITY_EMBED_PRIORITY if priority_key in PRIORITY_PREDPISY else 0

    with target_conn.cursor() as cur:
        cur.execute(
            """
            insert into documents
                (id, source_id, external_id, doc_type, title, issuer,
                 decision_date, effective_date, url, status, content_hash,
                 valid_from, valid_until, is_current, predpis_cislo,
                 predpis_rok, embed_priority)
            values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            on conflict (id) do nothing
            """,
            (
                doc["id"], doc["source_id"], doc["external_id"], doc["doc_type"],
                doc["title"], doc["issuer"], doc["decision_date"], doc["effective_date"],
                doc["url"], doc["status"], "__resharded_cleaned_v1__",
                doc["valid_from"], doc["valid_until"], doc["is_current"],
                doc["predpis_cislo"], doc["predpis_rok"], embed_priority,
            ),
        )
        for c in chunks:
            cleaned = clean_html(c["content"])
            search_text = cz_search_text(cleaned)
            cur.execute(
                """
                insert into chunks (id, document_id, chunk_index, heading, content, search_tsv)
                values (%s,%s,%s,%s,%s, to_tsvector('simple', %s))
                on conflict (id) do nothing
                """,
                (c["id"], doc["id"], c["chunk_index"], c["heading"], cleaned, search_text),
            )
    target_conn.commit()


# ---------------------------------------------------------------------------
# Hlavni beh
# ---------------------------------------------------------------------------

def main():
    log("Reshardovani + cisteni zakonu - start.")
    log(f"Rozpocet na shard: {SHARD_BUDGET_BYTES / 1024 / 1024:.0f} MB "
        f"(limit {SHARD_HARD_LIMIT_BYTES/1024/1024:.0f} MB - rezerva "
        f"{SHARD_SAFETY_MARGIN_BYTES/1024/1024:.0f} MB).")

    # Najdi prvni cilovy shard, ktery jeste ma volne misto.
    active_target = None
    active_conn = None
    for name, env_key in TARGET_SHARDS:
        if env_key not in os.environ:
            log(f"   [{name}] přeskočeno - env var {env_key} neni nastavena.")
            continue
        conn = connect_target(env_key)
        size = get_shard_size_bytes(conn)
        if size < SHARD_BUDGET_BYTES:
            active_target, active_conn = name, conn
            log(f"   Aktivni cilovy shard: '{name}' ({size/1024/1024:.1f} MB / "
                f"{SHARD_BUDGET_BYTES/1024/1024:.0f} MB rozpoctu).")
            break
        else:
            log(f"   [{name}] je jiz naplneny ({size/1024/1024:.1f} MB) - "
                f"prechazim na dalsi cilovy shard v seznamu.")
            conn.close()

    if active_conn is None:
        log("STOP: Vsechny shardy v TARGET_SHARDS jsou plne (nebo seznam je")
        log("prazdny/nekonfigurovany). Toto NENI chyba skriptu - je potreba")
        log("rucne zalozit dalsi cilovy Neon shard (stejna schema jako")
        log("delicate-brook-34508314) a pridat ho do TARGET_SHARDS v tomto")
        log("souboru (+ odpovidajici GitHub secret s connection stringem).")
        return

    already_migrated = ensure_migration_tracking(active_conn)
    log(f"   V '{active_target}' uz je {len(already_migrated)} dokumentu.")

    total_migrated_docs = 0
    total_migrated_chunks = 0

    for src_name, src_project_id in SOURCE_SHARDS:
        if time_left() <= 30:
            break
        env_key = f"NEON_ZAKONY_{src_name.upper()}_DB_URL"
        if env_key not in os.environ:
            log(f"   [{src_name}] přeskočeno - env var {env_key} neni nastavena.")
            continue

        src_conn = connect_source(src_project_id, src_name)
        try:
            while time_left() > 30:
                docs = fetch_source_documents(src_conn, already_migrated, DOCS_PER_BATCH)
                if not docs:
                    break  # tenhle zdrojovy shard je (pro ted) hotovy

                cur_size = get_shard_size_bytes(active_conn)
                if cur_size >= SHARD_BUDGET_BYTES:
                    log(f"   Cilovy shard '{active_target}' dosahl rozpoctu "
                        f"({cur_size/1024/1024:.0f} MB) - konci beh, zalozit dalsi shard.")
                    src_conn.close()
                    active_conn.close()
                    log(f"CELKEM tento beh: {total_migrated_docs} dokumentu, "
                        f"{total_migrated_chunks} chunku.")
                    return

                for doc in docs:
                    chunks = fetch_chunks_for_document(src_conn, doc["id"])
                    write_document_and_chunks(active_conn, doc, chunks)
                    already_migrated.add(doc["id"])
                    total_migrated_docs += 1
                    total_migrated_chunks += len(chunks)

                log(f"   [{src_name}] zmigrovano +{len(docs)} dokumentu "
                    f"(bezici celkem: {total_migrated_docs} dok. / "
                    f"{total_migrated_chunks} chunku, cil '{active_target}' "
                    f"~{get_shard_size_bytes(active_conn)/1024/1024:.0f} MB).")

                if time_left() <= 30:
                    break
        finally:
            src_conn.close()

    active_conn.close()
    log(f"Hotovo (nebo dosazen casovy rozpocet). CELKEM tento beh: "
        f"{total_migrated_docs} dokumentu, {total_migrated_chunks} chunku.")


# ---------------------------------------------------------------------------
# PHASE_CUTOVER_TODO - co je potreba udelat RUCNE po dobehnuti migrace+
# embeddingu, nez se stare shardy smazou (Radek 2026-09-25: nemazat, dokud
# neni overeno):
#
#  1. Samostatny navazujici skript (uprava embed_zakony_neon.py pro nove
#     shardy misto puvodnich 4) - pouziva uz existujici
#     get_pending_chunks_prioritized() v kazdem novem shardu, ktera
#     automaticky respektuje embed_priority nastaveny touto migraci.
#  2. Overit v novych shardech: pocty dokumentu/chunku souhlasi s puvodnimi,
#     nahodny vzorek obsahu je spravne ocisteny (bez HTML), embedding a
#     search_tsv jsou vyplnene u vsech radku.
#  3. V ai-query pridat docasne DEBUG-only cteni z novych shardu vedle
#     stavajicich (stejny vzor jako body.debug pouzity v teto session),
#     porovnat kvalitu odpovedi na sade testovacich dotazu.
#  4. Az bude jiste, ze nove shardy funguji spravne a rychleji: v ai-query
#     PRO KAZDY DOKUMENT zvlast pouzit novy shard MISTO stareho, pokud v
#     novem shardu ma dany dokument uz vyplneny embedding (jinak dal stary -
#     to je presne to "zachovani starych zaembedovanych predpisu", o ktere
#     Radek 2026-09-25 vyslovne zadal).
#  5. Teprve POTE, az VSECHNY dokumenty maji noveho zaembedovaneho
#     nastupce, smazat puvodni 4 shardy (do1997/1998_2007/2008_2020/
#     2021_dosud) a uvolnit jejich Neon projekty.
#  6. Znovu zapnout planovac v embed-zakony-neon.yml (nebo jeho naslednika
#     pro nove shardy), az bude bezici migrace u konce.
# ---------------------------------------------------------------------------


if __name__ == "__main__":
    main()
