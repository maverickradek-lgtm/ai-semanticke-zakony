"""
Sync MF metodiky (Centralni harmonizacni jednotka) do samostatne Neon databaze.

Duvod samostatne databaze: metodicke dokumenty od statnich uradu/komor (mimo
e-Sbirku) jsou novy druh zdroje, ktery chceme postupne rozsirovat (Celni
sprava, Financni sprava, komory...) - aby se neopakoval problem s
dochazejicim mistem v hlavni 500MB Supabase databazi. Kazdy novy zdroj
tohoto typu muze dostat vlastni Neon projekt zdarma. Tato databaze sama o
sobe je navrzena obecne (sloupce "source"/"series"), takze do ni muzou
pribyt i dalsi MF serie (ne jen CHJ) bez zmeny schematu.

Na rozdil od e-Sbirka/UOHS/NS pipeline (ktere pisi pres Supabase PostgREST)
tahle jede pres primo Postgres spojeni (Neon nema vestaveny REST API), a
protoze zadna sdilena embedovaci fronta pro tuto databazi neexistuje,
embedding se dela rovnou v tomto skriptu (FAZE A+B dohromady, ne oddelene
jako u ostatnich zdroju).

Zdroj dat: mf.gov.cz/cs/kontrola-a-regulace/rizeni-a-kontrola-verejnych-financi/metodicke-materialy-chj/{rok}
- staticke HTML seznamy po letech, kazda polozka ma detail stranku s jednim
PDF odkazem (overeno rucne 2026-08-07).

MAX_NEW_DOCS_PER_RUN a TIME_BUDGET_SECONDS omezuji beh (stejny vzorec jako u
ostatnich sync skriptu) - skript preskakuje uz existujici zaznamy podle
external_id (slug z URL), takze dalsi beh (mesicni cron) pokracuje tam, kde
skoncil predchozi. Prvni beh projede zpetne az do START_YEAR (zaklad pro
zpetne dohnani historie), dalsi mesicni behy uz jen doplni nove polozky.

2026-11-27: odkaz na "PDF", ktery vraci HTML (ne PDF) nebo useknuty/poskozeny
soubor uz neshodi cely beh (je to chyba zdroje, ne skriptu) - dokument se jen
preskoci s varovanim a zkusi se znovu pri dalsim behu. Skutecne chyby
(sit, HTTP, DB, embedding) nadale koncí nenulovym exit kodem.
"""

import os
import re
import sys
import time
from datetime import datetime
from io import BytesIO
from docx import Document

import requests
from bs4 import BeautifulSoup
from pypdf import PdfReader
import psycopg2

# F-10 (audit 2026-09-24): sdileny stav pro rozliseni "zdroj nema nic
# noveho" od "behem behu doslo ke skutecne chybe" - druhy pripad ma za
# nasledek nenulovy exit kod z main().
_STATE = {"had_errors": False}

def db_connect(url, timeout=15):
    """Pripoji se k Neonu se 4 pokusy - NAS self-hosted runner ma obcas
    docasny DNS vypadek (Temporary failure in name resolution), jednorazovy
    pokus bez retry pak shodi cely beh zbytecne."""
    last_err = None
    for attempt in range(4):
        try:
            return psycopg2.connect(url, connect_timeout=timeout, keepalives=1, keepalives_idle=30, keepalives_interval=10, keepalives_count=3)
        except Exception as e:
            last_err = e
            print("db_connect selhalo (pokus " + str(attempt + 1) + "/4): " + str(e), flush=True)
            time.sleep(3)
    raise last_err

NEON_DB_URL = os.environ["NEON_MF_DB_URL"]
SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SERVICE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
ADMIN_USER_ID = "2648f5db-bea6-4cac-b490-ad0ec59723df"
GEMINI_API_KEY_OVERRIDE = os.environ.get("GEMINI_API_KEY_OVERRIDE")

EMBED_MODEL = "gemini-embedding-001"
EMBED_DIM = 256
MAX_CHARS_PER_CHUNK = 3000
MAX_NEW_DOCS_PER_RUN = int(os.environ.get("MAX_NEW_DOCS_PER_RUN", "40"))
TIME_BUDGET_SECONDS = int(os.environ.get("TIME_BUDGET_SECONDS", "3000"))
START_YEAR = int(os.environ.get("START_YEAR", "2015"))

BASE_URL = "https://mf.gov.cz"
LIST_URL_TMPL = (
    BASE_URL
    + "/cs/kontrola-a-regulace/rizeni-a-kontrola-verejnych-financi/metodicke-materialy-chj/{year}"
)

DETAIL_RE = re.compile(r"/metodicke-materialy-chj/\d{4}/[a-z0-9-]+", re.I)

START_TIME = time.time()


# Zaznamy, u kterych MF odkazuje na mrtvy/externi soubor - nahrazeno aktualnim
# zdrojem (slug detail stranky -> nazev + primy odkaz na PDF). Pridano 2026-11-27:
# stary odkaz na IPPF 2017 (IIA) vede na uvodni stranku; od 1/2025 plati
# Globalni standardy interniho auditu (vyhlaseny v Sb. pod c. 419/2025).
FILE_OVERRIDES = {
    "mezinarodni-standardy-pro-profesni-praxi-24374": {
        "title": "Globální standardy interního auditu (IIA 2024, český překlad) – nahrazují Mezinárodní standardy pro profesní praxi interního auditu",
        "pdf_url": "https://theiia.org/globalassets/site/standards/editable-versions/global-internal-audit-standards-czech.pdf",
    },
}
# Mezinarodni auditorske standardy (ISA) prijate Komorou auditoru CR - MF je
# jen zminuje, jednotliva PDF jsou na kacr.cz (prehled:
# https://www.kacr.cz/aktualni-auditorske-standardy). Kazdy standard = samostatny dokument.
KACR_PAGE = "https://www.kacr.cz/aktualni-auditorske-standardy"
_K = "https://www.kacr.cz/s-doc/1/"
EXTRA_DOCS = [
    ("kacr-isa-200", "ISA 200 – Obecné cíle nezávislého auditora a provádění auditu v souladu s mezinárodními auditorskými standardy", _K + "1000272/638766965211200000/ISA 200cz_12_2024.pdf"),
    ("kacr-isa-210", "ISA 210 – Sjednávání podmínek auditních zakázek", _K + "1000273/638766965218600000/ISA 210cz_12_2024.pdf"),
    ("kacr-isa-220r", "ISA 220 (revidovaný) – Řízení kvality auditu účetní závěrky", _K + "1000274/638766965243500000/ISA 220Rcz_12_2024.pdf"),
    ("kacr-isa-230", "ISA 230 – Dokumentace auditu", _K + "1000275/638766965257600000/ISA 230cz_12_2024.pdf"),
    ("kacr-isa-240", "ISA 240 – Postupy auditorů související s podvody při auditu účetní závěrky", _K + "1000276/638766965273033333/ISA 240cz_12_2024.pdf"),
    ("kacr-isa-250r", "ISA 250 (revidovaný) – Přihlížení k právním předpisům při auditu účetní závěrky", _K + "1000277/638766965282133333/ISA 250Rcz_12_2024.pdf"),
    ("kacr-isa-260r", "ISA 260 (revidovaný) – Předávání informací osobám pověřeným správou a řízením účetní jednotky", _K + "1000278/638766965293166667/ISA 260Rcz_PIE_12_2024.pdf"),
    ("kacr-isa-265", "ISA 265 – Předávání informací o nedostatcích ve vnitřním kontrolním systému", _K + "1000279/638766965298333333/ISA 265cz_12_2024.pdf"),
    ("kacr-isa-300", "ISA 300 – Plánování auditu účetní závěrky", _K + "1000280/638766965306766667/ISA 300cz_12_2024.pdf"),
    ("kacr-isa-315r", "ISA 315 (revidovaný) – Identifikace a vyhodnocení rizik materiální nesprávnosti", _K + "1000281/638766965336700000/ISA 315Rcz_12_2024.pdf"),
    ("kacr-isa-320", "ISA 320 – Materialita při plánování a provádění auditu", _K + "1000282/638766965341633333/ISA 320cz_12_2024.pdf"),
    ("kacr-isa-330", "ISA 330 – Reakce auditora na vyhodnocená rizika", _K + "1000283/638941273188666667/ISA 330cz_12_2024.pdf"),
    ("kacr-isa-402", "ISA 402 – Zvažované skutečnosti týkající se účetní jednotky využívající služeb servisní organizace", _K + "1000284/638766965379000000/ISA 402cz_12_2024.pdf"),
    ("kacr-isa-450", "ISA 450 – Vyhodnocení nesprávností zjištěných během auditu", _K + "1000285/638766965388300000/ISA 450cz_12_2024.pdf"),
    ("kacr-isa-500", "ISA 500 – Důkazní informace", _K + "1000286/638766965398933333/ISA 500cz_12_2024.pdf"),
    ("kacr-isa-501", "ISA 501 – Důkazní informace – specifické aspekty vybraných položek", _K + "1000287/638766965409233333/ISA 501cz_12_2024.pdf"),
    ("kacr-isa-505", "ISA 505 – Externí konfirmace", _K + "1000288/638766965423500000/ISA 505cz_12_2024.pdf"),
    ("kacr-isa-510", "ISA 510 – První auditní zakázka – počáteční zůstatky", _K + "1000289/638766965432300000/ISA 510cz_12_2024.pdf"),
    ("kacr-isa-520", "ISA 520 – Analytické postupy", _K + "1000290/638766965438566667/ISA 520cz_12_2024.pdf"),
    ("kacr-isa-530", "ISA 530 – Výběr vzorků", _K + "1000291/638766965443666667/ISA 530cz_12_2024.pdf"),
    ("kacr-isa-540r", "ISA 540 (revidovaný) – Audit účetních odhadů a souvisejících vysvětlujících a popisných informací", _K + "1000292/638766965461800000/ISA 540Rcz_12_2024.pdf"),
    ("kacr-isa-550", "ISA 550 – Spřízněné strany", _K + "1000293/638766965475000000/ISA 550cz_12_2024.pdf"),
    ("kacr-isa-560", "ISA 560 – Události po datu účetní závěrky", _K + "1000294/638766965483400000/ISA 560cz_12_2024.pdf"),
    ("kacr-isa-570r", "ISA 570 (revidovaný) – Nepřetržité trvání podniku", _K + "1000295/638766965501033333/ISA 570Rcz_12_2024.pdf"),
    ("kacr-isa-580", "ISA 580 – Písemná prohlášení", _K + "1000296/638766965509166667/ISA 580cz_12_2024.pdf"),
    ("kacr-isa-600r", "ISA 600 (revidovaný) – Specifické aspekty auditů účetních závěrek skupiny", _K + "1000297/638766965531433333/ISA 600R_cz_12_2024.pdf"),
    ("kacr-isa-610r", "ISA 610 (revidovaný) – Využití práce interních auditorů", _K + "1000298/638766965540900000/ISA 610Rcz_12_2024.pdf"),
    ("kacr-isa-620", "ISA 620 – Využití práce auditorova experta", _K + "1000299/638766965550800000/ISA 620cz_12_2024.pdf"),
    ("kacr-isa-700r", "ISA 700 (revidovaný) – Utvoření názoru na účetní závěrku a sestavení zprávy nezávislého auditora", _K + "1000300/638767636678700000/ISA 700Rcz_PIE_12_2024.pdf"),
    ("kacr-isa-700r-ad", "Aplikační doložka KA ČR k ISA 700R – Požadavky právních předpisů na zprávu auditora (účinnost od 10. 11. 2026)", _K + "1000871/639305884371033333/AD k ISA 700_po Sněmu 2026_na web.pdf"),
    ("kacr-isa-701", "ISA 701 – Informování o hlavních záležitostech auditu ve zprávě nezávislého auditora", _K + "1000301/638766965624933333/ISA 701cz_10_2024.pdf"),
    ("kacr-isa-705r", "ISA 705 (revidovaný) – Modifikace výroku ve zprávě nezávislého auditora", _K + "1000302/638766965633500000/ISA 705Rcz_12_2024.pdf"),
    ("kacr-isa-706r", "ISA 706 (revidovaný) – Odstavce zprávy nezávislého auditora obsahující zdůraznění skutečnosti a jiné skutečnosti", _K + "1000303/638766965642700000/ISA 706Rcz_12_2024.pdf"),
    ("kacr-isa-710", "ISA 710 – Srovnávací informace – srovnávací údaje a srovnávací účetní závěrka", _K + "1000304/638766965647933333/ISA 710cz_IAS1_12_2024.pdf"),
    ("kacr-isa-720r", "ISA 720 (revidovaný) – Povinnosti auditora týkající se ostatních informací", _K + "1000689/639168118460033333/ISA 720Rcz_06_2026_amend ISSA 5000.pdf"),
    ("kacr-isa-720-ad", "Aplikační doložka KA ČR k ISA 720 (revidovanému) – účinnost od 12. 11. 2024", _K + "1000125/638688983567933333/AD ISA 720-final-opr.pdf"),
    ("kacr-isa-800r", "ISA 800 (revidovaný) – Zvláštní aspekty – audity účetních závěrek sestavených v souladu s rámcem pro zvláštní účely", _K + "1000306/638766965681866667/ISA 800Rcz_12_2024.pdf"),
    ("kacr-isa-805r", "ISA 805 (revidovaný) – Zvláštní aspekty – audity jednotlivých účetních výkazů a specifických prvků, účtů nebo položek", _K + "1000690/639168118464633333/ISA 805Rcz_06_2026_amend ISSA 5000.pdf"),
    ("kacr-isa-810r", "ISA 810 (revidovaný) – Zakázky na vypracování zprávy o agregované účetní závěrce", _K + "1000308/638766965713366667/ISA 810Rcz_12_2024.pdf"),
]
BROWSER_HEADERS = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"}


class BadSourceFile(Exception):
    """Soubor na zdroji neni platny PDF/DOCX (HTML stranka, useknuty soubor...)."""


def time_left():
    return TIME_BUDGET_SECONDS - (time.time() - START_TIME)


def sb_headers():
    return {
        "apikey": SERVICE_KEY,
        "Authorization": f"Bearer {SERVICE_KEY}",
        "Content-Type": "application/json",
    }


def get_admin_gemini_key():
    if GEMINI_API_KEY_OVERRIDE:
        return GEMINI_API_KEY_OVERRIDE
    last_err = None
    for attempt in range(4):
        try:
            r = requests.post(
                f"{SUPABASE_URL}/rest/v1/rpc/get_user_gemini_key",
                headers=sb_headers(),
                json={"p_user_id": ADMIN_USER_ID},
                timeout=30,
            )
            r.raise_for_status()
            key = r.json()
            if not key:
                raise RuntimeError("Admin Gemini key not available")
            return key
        except Exception as e:
            last_err = e
            print("get_admin_gemini_key selhalo (pokus " + str(attempt + 1) + "/4): " + str(e), flush=True)
            time.sleep(3)
    raise last_err


def ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute("create extension if not exists pgcrypto;")
        cur.execute("create extension if not exists vector;")
        cur.execute(
            """
            create table if not exists documents (
                id uuid primary key default gen_random_uuid(),
                source text not null default 'mf_chj',
                series text,
                external_id text not null,
                title text not null,
                url text not null,
                pdf_url text,
                published_date date,
        is_current boolean not null default true,
                fetched_at timestamptz default now(),
                created_at timestamptz default now(),
                unique(source, external_id)
            );
            """
        )
        cur.execute(
            """
            create table if not exists chunks (
                id uuid primary key default gen_random_uuid(),
                document_id uuid not null references documents(id) on delete cascade,
                chunk_index int not null,
                heading text,
                content text,
                embedding vector(256),
                created_at timestamptz default now()
            );
            """
        )
        cur.execute("create index if not exists chunks_document_id_idx on chunks(document_id);")
        cur.execute(
            "alter table documents add column if not exists is_current boolean not null default true;"
        )
    conn.commit()


def fetch_existing_external_ids(conn, source="mf_chj"):
    with conn.cursor() as cur:
        cur.execute("select external_id from documents where source = %s;", (source,))
        return {row[0] for row in cur.fetchall()}


def slug_from_href(href):
    m = DETAIL_RE.search(href)
    if not m:
        return None
    return m.group(0).split("/")[-1]


def list_year(year):
    resp = requests.get(LIST_URL_TMPL.format(year=year), timeout=60)
    if resp.status_code == 404:
        return []
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    items = []
    seen = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if not DETAIL_RE.search(href):
            continue
        full = href if href.startswith("http") else BASE_URL + href
        slug = slug_from_href(full)
        if not slug or slug in seen:
            continue
        seen.add(slug)
        title = a.get_text(strip=True)
        if not title:
            continue
        items.append({"url": full, "slug": slug, "title": title})
    return items


CZECH_MONTHS = {
    "ledna": 1, "unora": 2, "brezna": 3, "dubna": 4,
    "kvetna": 5, "cervna": 6, "cervence": 7,
    "srpna": 8, "zari": 9, "rijna": 10, "listopadu": 11,
    "prosince": 12,
}


def _strip_diacritics(s):
    table = str.maketrans("áčďéěíňóřšťúůýž", "acdeeinorstuuyz")
    return s.lower().translate(table)


def parse_czech_date(text):
    m = re.search("(\\d{1,2})\\.?\\s*(\\d{1,2}|[a-zA-Zá-žÁ-Ž]+)\\.?\\s*(\\d{4})", text)
    if not m:
        return None
    day, month_raw, year = m.groups()
    month_raw_l = _strip_diacritics(month_raw.strip("."))
    if month_raw_l.isdigit():
        month = int(month_raw_l)
    else:
        month = CZECH_MONTHS.get(month_raw_l)
    if not month:
        return None
    try:
        return datetime(int(year), month, int(day)).date()
    except ValueError:
        return None


def fetch_detail(url, title=None):
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    article = soup.find("article") or soup
    # NOTE: attachment links (PDF and DOCX) live OUTSIDE <article> on
    # mf.gov.cz detail pages (in a .b-download quick-box and a "Dokumenty ke
    # stazeni" archive list), so we must search the whole page, not just the
    # article tag.
    def _collect(ext_regex):
        out = []
        for a in soup.find_all("a", href=True):
            if re.search(ext_regex, a["href"], re.I):
                href = a["href"] if a["href"].startswith("http") else BASE_URL + a["href"]
                out.append((href, a.get_text(" ", strip=True)))
        return out

    def _pick(candidates):
        if not candidates:
            return None
        ident = None
        if title:
            m = re.search("č\\.?\\s*(\\d+[a-z]?)\\s*/\\s*(\\d{4})", title, re.I)
            if m:
                ident = f"{m.group(1)}/{m.group(2)}"
        if ident:
            for href, text in candidates:
                if ident in text.replace(" ", ""):
                    return href
        # fall back to the last match on the page: observed pattern is
        # superseded/older versions listed first, current version last
        return candidates[-1][0]

    pdf_link = _pick(_collect(r"\.pdf($|\?)"))
    docx_link = _pick(_collect(r"\.docx($|\?)"))
    if pdf_link:
        file_url, file_kind = pdf_link, "pdf"
    elif docx_link:
        file_url, file_kind = docx_link, "docx"
    else:
        file_url, file_kind = None, None
    date_el_text = article.get_text(" ", strip=True)
    published = parse_czech_date(date_el_text)
    # supersession note, e.g. title says "puvodne stanovisko CHJ c. 5/2018"
    replaces_ident = None
    if title:
        m2 = re.search("p[uů]vodn[ěe][^0-9]{0,40}č\\.?\\s*(\\d+[a-z]?)\\s*/\\s*(\\d{4})", title, re.I)
        if m2:
            replaces_ident = f"{m2.group(1)}/{m2.group(2)}"
    return {
        "pdf_url": file_url,
        "file_kind": file_kind,
        "published_date": published,
        "replaces_ident": replaces_ident,
    }


def extract_pdf_text(pdf_bytes):
    if not pdf_bytes.lstrip()[:5] == b"%PDF-":
        raise BadSourceFile("odpoved neni PDF (zacina " + repr(pdf_bytes[:8]) + ")")
    try:
        reader = PdfReader(BytesIO(pdf_bytes))
        parts = []
        for page in reader.pages:
            t = page.extract_text() or ""
            parts.append(t)
    except Exception as e:
        raise BadSourceFile("PDF nelze precist: " + str(e))
    text = "\n".join(parts)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def extract_docx_text(docx_bytes):
    if docx_bytes[:2] != b"PK":
        raise BadSourceFile("odpoved neni DOCX (zacina " + repr(docx_bytes[:8]) + ")")
    try:
        doc = Document(BytesIO(docx_bytes))
        parts = [p.text for p in doc.paragraphs if p.text and p.text.strip()]
        for table in doc.tables:
            for row in table.rows:
                for cell in row.cells:
                    if cell.text and cell.text.strip():
                        parts.append(cell.text.strip())
    except Exception as e:
        raise BadSourceFile("DOCX nelze precist: " + str(e))
    return "\n".join(parts).strip()


def split_into_chunks(text, max_chars=MAX_CHARS_PER_CHUNK):
    paragraphs = []
    for p in text.split("\n\n"):
        p = p.strip()
        # odstavec delsi nez limit (napr. PDF bez prazdnych radku) rozdelit
        # po radcich a pripadne natvrdo na hranici mezery
        while len(p) > max_chars:
            cut = p.rfind("\n", 0, max_chars)
            if cut < max_chars // 2:
                cut = p.rfind(" ", 0, max_chars)
            if cut < max_chars // 2:
                cut = max_chars
            paragraphs.append(p[:cut].strip())
            p = p[cut:].strip()
        if p:
            paragraphs.append(p)
    chunks = []
    current = []
    current_len = 0
    for p in paragraphs:
        if current_len + len(p) > max_chars and current:
            chunks.append("\n\n".join(current))
            current = []
            current_len = 0
        current.append(p)
        current_len += len(p)
    if current:
        chunks.append("\n\n".join(current))
    return chunks or ([text] if text else [])


def embed_text(text, gemini_key, retries=3):
    for attempt in range(retries):
        try:
            resp = requests.post(
                f"https://generativelanguage.googleapis.com/v1beta/models/{EMBED_MODEL}:embedContent",
                headers={"Content-Type": "application/json", "x-goog-api-key": gemini_key},
                json={
                    "content": {"parts": [{"text": text[:8000]}]},
                    "taskType": "RETRIEVAL_DOCUMENT",
                    "outputDimensionality": EMBED_DIM,
                },
                timeout=30,
            )
            if resp.status_code == 429:
                time.sleep(5 * (attempt + 1))
                continue
            resp.raise_for_status()
            vec = resp.json().get("embedding", {}).get("values")
            if not vec:
                return None
            return vec
        except requests.RequestException:
            if attempt == retries - 1:
                raise
            time.sleep(3 * (attempt + 1))
    return None


def import_new_documents(conn):
    existing = fetch_existing_external_ids(conn)
    current_year = datetime.now().year
    new_count = 0
    for year in range(current_year, START_YEAR - 1, -1):
        if time_left() <= 0 or new_count >= MAX_NEW_DOCS_PER_RUN:
            break
        try:
            items = list_year(year)
        except requests.RequestException as e:
            print(f"WARN: list_year({year}) failed: {e}")
            continue
        for item in items:
            if time_left() <= 0 or new_count >= MAX_NEW_DOCS_PER_RUN:
                break
            if item["slug"] in existing:
                continue
            try:
                detail = fetch_detail(item["url"], title=item.get("title"))
            except requests.RequestException as e:
                print(f"WARN: fetch_detail({item['url']}) failed: {e}")
                continue
            ov = FILE_OVERRIDES.get(item["slug"])
            if ov:
                print(f"OVERRIDE: {item['title']} -> {ov['pdf_url']}")
                detail["pdf_url"] = ov["pdf_url"]
                detail["file_kind"] = "pdf"
                item = dict(item, title=ov["title"])
            if not detail["pdf_url"]:
                print(f"SKIP (no pdf/docx): {item['title']}")
                existing.add(item["slug"])
                continue
            try:
                file_resp = requests.get(detail["pdf_url"], timeout=120, headers=BROWSER_HEADERS if ov else None)
                file_resp.raise_for_status()
                if detail.get("file_kind") == "docx":
                    text = extract_docx_text(file_resp.content)
                else:
                    text = extract_pdf_text(file_resp.content)
            except BadSourceFile as e:
                # chyba na strane zdroje (HTML misto PDF, poskozeny soubor) - ne chyba
                # skriptu; preskocit, pri dalsim behu se zkusi znovu
                print(f"SKIP (neplatny soubor na zdroji): {item['title']} -> {e}")
                existing.add(item["slug"])
                continue
            except Exception as e:
                print(f"WARN: file fetch/extract failed for {item['title']}: {e}")
                _STATE["had_errors"] = True
                continue
            if not text:
                print(f"SKIP (empty text): {item['title']}")
                existing.add(item["slug"])
                continue
            own_is_current = True
            m_own = re.search("č\\.?\\s*(\\d+[a-z]?)\\s*/\\s*(\\d{4})", item["title"], re.I)
            if m_own:
                own_num, own_year = m_own.group(1), m_own.group(2)
                pat_own = "p[uů]vodn[ěe][^0-9]{0,40}č\\.?\\s*" + re.escape(own_num) + "\\s*/\\s*" + re.escape(own_year)
                with conn.cursor() as cur0:
                    cur0.execute(
                        "select 1 from documents where source = %s and title ~* %s limit 1",
                        ("mf_chj", pat_own),
                    )
                    if cur0.fetchone():
                        own_is_current = False
                        print(f"SUPERSEDED-ON-IMPORT: {item['title']} already replaced by a newer document")
            chunk_texts = split_into_chunks(text)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    insert into documents (source, series, external_id, title, url, pdf_url, published_date, is_current)
                    values (%s, %s, %s, %s, %s, %s, %s, %s)
                    on conflict (source, external_id) do nothing
                    returning id;
                    """,
                    (
                        "mf_chj",
                        "chj_metodika",
                        item["slug"],
                        item["title"],
                        item["url"],
                        detail["pdf_url"],
                        detail["published_date"],
                        own_is_current,
                    ),
                )
                row = cur.fetchone()
                if not row:
                    conn.commit()
                    continue
                doc_id = row[0]
                if detail.get("replaces_ident"):
                    print(f"SUPERSEDES: {item['title']} -> marking c. {detail['replaces_ident']} as historical")
                    old_num, old_year = detail["replaces_ident"].split("/")
                    pat = "č\\.?\\s*" + re.escape(old_num) + "\\s*/\\s*" + re.escape(old_year)
                    cur.execute(
                        """
                        update documents set is_current = false
                        where source = %s and is_current = true and id != %s
                          and title ~* %s
                        """,
                        ("mf_chj", doc_id, pat),
                    )
                for i, ctext in enumerate(chunk_texts):
                    heading = item["title"] if i == 0 else f"{item['title']} (pokracovani {i + 1})"
                    cur.execute(
                        """
                        insert into chunks (document_id, chunk_index, heading, content)
                        values (%s, %s, %s, %s);
                        """,
                        (doc_id, i, heading, ctext),
                    )
            conn.commit()
            existing.add(item["slug"])
            new_count += 1
            print(f"IMPORTED: {item['title']} ({len(chunk_texts)} chunks)")
    print(f"Import done: {new_count} new documents")
    return new_count


def import_extra_documents(conn):
    """Import jednotlivych PDF z EXTRA_DOCS (ISA od Komory auditoru)."""
    existing = fetch_existing_external_ids(conn)
    new_count = 0
    for slug, title, pdf_url in EXTRA_DOCS:
        if slug in existing:
            continue
        if time_left() <= 0 or new_count >= MAX_NEW_DOCS_PER_RUN:
            break
        try:
            r = requests.get(pdf_url, timeout=120, headers=BROWSER_HEADERS)
            r.raise_for_status()
            text = extract_pdf_text(r.content)
        except BadSourceFile as e:
            print(f"SKIP (neplatny soubor na zdroji): {title} -> {e}")
            continue
        except Exception as e:
            print(f"WARN: EXTRA fetch/extract failed for {title}: {e}")
            _STATE["had_errors"] = True
            continue
        if not text:
            print(f"SKIP (empty text): {title}")
            continue
        chunk_texts = split_into_chunks(text)
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into documents (source, series, external_id, title, url, pdf_url, published_date, is_current)
                values (%s, %s, %s, %s, %s, %s, %s, true)
                on conflict (source, external_id) do nothing
                returning id;
                """,
                ("mf_chj", "isa_kacr", slug, title, KACR_PAGE, pdf_url, None),
            )
            row = cur.fetchone()
            if not row:
                conn.commit()
                continue
            for i, ctext in enumerate(chunk_texts):
                heading = title if i == 0 else f"{title} (pokracovani {i + 1})"
                cur.execute(
                    "insert into chunks (document_id, chunk_index, heading, content) values (%s, %s, %s, %s);",
                    (row[0], i, heading, ctext),
                )
        conn.commit()
        existing.add(slug)
        new_count += 1
        print(f"IMPORTED (KA CR): {title} ({len(chunk_texts)} chunks)")
    print(f"Import KA CR done: {new_count} new documents")
    return new_count


def ensure_conn(conn):
    if conn is not None and conn.closed == 0:
        try:
            with conn.cursor() as cur:
                cur.execute("select 1")
            return conn
        except Exception:
            try:
                conn.close()
            except Exception:
                pass
    new_conn = db_connect(NEON_DB_URL)
    print("(znovu navazano spojeni)")
    return new_conn


def embed_pending(conn, gemini_key):
    embedded = 0
    while time_left() > 30:
        conn = ensure_conn(conn)
        with conn.cursor() as cur:
            cur.execute(
                "select id, content from chunks where embedding is null order by created_at asc limit 20;"
            )
            rows = cur.fetchall()
        if not rows:
            break
        for chunk_id, content in rows:
            if time_left() <= 0:
                break
            try:
                vec = embed_text(content, gemini_key)
            except Exception as e:
                print(f"WARN: embed failed for {chunk_id}: {e}")
                _STATE["had_errors"] = True
                continue
            if not vec:
                continue
            vec_str = "[" + ",".join(f"{x:.8f}" for x in vec) + "]"
            conn = ensure_conn(conn)
            with conn.cursor() as cur:
                cur.execute(
                    "update chunks set embedding = %s::vector where id = %s;",
                    (vec_str, chunk_id),
                )
            conn.commit()
            embedded += 1
    print(f"Embedding done: {embedded} chunks")
    return embedded


def main():
    conn = db_connect(NEON_DB_URL)
    try:
        ensure_schema(conn)
        import_new_documents(conn)
        import_extra_documents(conn)
        gemini_key = get_admin_gemini_key()
        embed_pending(conn, gemini_key)
    finally:
        conn.close()

    if _STATE["had_errors"]:
        print("=== SELHANI: behem behu doslo k alespon jedne skutecne chybe, viz log vyse ===")
        sys.exit(1)


if __name__ == "__main__":
    main()
