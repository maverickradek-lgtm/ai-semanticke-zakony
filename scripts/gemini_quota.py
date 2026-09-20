"""
Obecny, znovupouzitelny system pro sledovani a omezovani denniho vyuziti
Gemini API klicu napric libovolnymi skripty/pipeline v tomto repu.

Google od 12/2025 nepublikuje presne RPD limity v dokumentaci (jen odkazuje
na Google AI Studio pro konkretni projekt), takze presne cislo "X requestu/
den" nelze spolehlive predem znat. Misto hadani si strop pro kazdy klic
MERIME SAMI: prvni HTTP 429 daneho dne = pozorovany strop pro ten den,
vyhlazeny prumerem s predchozim pozorovanim. Od dalsiho vyuziti stejneho
klice se pak pouziti drzi pod zadanym procentem (cap_fraction) tohoto
pozorovaneho stropu - dokud klic poprve nenarazi na skutecny 429, zadny
strop se nevynucuje (system se teprve "uci").

Pouziti (v libovolnem skriptu, ktery uz ma SUPABASE_URL a
SUPABASE_SERVICE_ROLE_KEY, coz je temer kazdy skript v tomto repu):

    import gemini_quota

    cap = 0.6  # napr. max 60 % pozorovaneho denniho stropu pro tento klic
    if not gemini_quota.check_quota(SUPABASE_URL, SERVICE_KEY, "GEMINI_API_KEY_4", cap):
        # tento klic uz dnes vycerpal svuj povoleny podil - pouzit jiny klic
        # nebo preskocit
        ...
    else:
        resp = requests.post(...)
        if resp.status_code == 429:
            gemini_quota.report_429(SUPABASE_URL, SERVICE_KEY, "GEMINI_API_KEY_4")

Databazova vrstva (Supabase projekt dmnenjykqhxkipxulhxr, migrace
"gemini_key_quota_tracking"): tabulky gemini_key_usage (denni pocitadlo)
a gemini_key_limits (perzistentni odhad stropu), pristupne jen pres
service_role (RLS zapnute, zadne policy) - proto se sem musi predavat
SUPABASE_SERVICE_ROLE_KEY, ne anon klic.

key_label by mel byt stabilni identifikator klice (napr. nazev GitHub
secretu jako "GEMINI_API_KEY_4"), NE samotna hodnota klice - hodnoty se
casem rotuji, label zustava stejny, takze si system spravne pamatuje
historii i po vymene klice za novy se stejnym ucelem.
"""
import requests


def _headers(service_key):
    return {
        "apikey": service_key,
        "Authorization": f"Bearer {service_key}",
        "Content-Type": "application/json",
    }


def check_quota(supabase_url, service_key, key_label, cap_fraction=None, retries=2):
    """Atomicky pripocte jedno volani pro (key_label, dnesni den) a vrati
    True, pokud se smi provest, nebo False, pokud uz dnesni pouziti
    presahlo cap_fraction pozorovaneho denniho stropu pro tento klic
    (volajici by pak NEMEL skutecne volani na Gemini API provest).

    cap_fraction=None => zadny strop se nevynucuje, volani se jen pocita
    (hodi se pro klice, ktere chceme zatim jen mereni, bez omezovani).

    Pri selhani tohoto sledovaciho volani (napr. vypadek Supabase) se
    vraci True - jde o ochranny mechanismus navic, ne kritickou cestu,
    takze radeji necháme embedding bezet dal, nez abychom kvuli
    bookkeepingu zbytecne blokovali praci.
    """
    url = f"{supabase_url.rstrip('/')}/rest/v1/rpc/gemini_quota_record_call"
    last_err = None
    for _ in range(retries):
        try:
            r = requests.post(
                url,
                headers=_headers(service_key),
                json={"p_key_label": key_label},
                timeout=15,
            )
            r.raise_for_status()
            rows = r.json()
            if not rows:
                return True
            row = rows[0]
            count = row.get("request_count")
            limit = row.get("estimated_daily_limit")
            if cap_fraction is None or limit is None or count is None:
                return True
            return count <= cap_fraction * limit
        except Exception as e:
            last_err = e
    print(f"   WARN gemini_quota.check_quota selhalo pro '{key_label}' ({last_err}) - pokracuji bez omezeni")
    return True


def report_429(supabase_url, service_key, key_label, retries=2):
    """Nahlasi HTTP 429 pro dany klic - aktualizuje pozorovany denni strop
    (vyhlazeno prumerem s predchozim odhadem, aby jeden nahodny vypadek
    strop trvale nesrazil). Volat hned po prijeti 429 z Gemini API pro
    tento klic."""
    url = f"{supabase_url.rstrip('/')}/rest/v1/rpc/gemini_quota_record_429"
    last_err = None
    for _ in range(retries):
        try:
            r = requests.post(
                url,
                headers=_headers(service_key),
                json={"p_key_label": key_label},
                timeout=15,
            )
            r.raise_for_status()
            return True
        except Exception as e:
            last_err = e
    print(f"   WARN gemini_quota.report_429 selhalo pro '{key_label}' ({last_err})")
    return False
