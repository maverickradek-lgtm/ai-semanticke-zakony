"""
DOCASNY diagnosticky skript (Radek 2026-09-22): zjistuje, jak e-Sbirka
oznacuje textove useky u NOVELIZACNICH zakonu (Cl. I, Cl. II...), protoze
soucasny parser v sync_esbirka_text.py hleda jen citace typu paragraf
(regex na "§ cislo") a novely tak vychazeji s 0 chunky, i kdyz realny
text v e-Sbirce existuje (viz 72/2025 Sb.).

Vypise VSECHNY zaznamy ze souboru 003PravniAktZneniFragment.json.gz, jejichz
iri zacina na zadany version_iri prefix - bez ohledu na to, jestli citace
sedi na regex pro paragrafy. Diky tomu uvidime skutecny format citace u
Cl.-uzlu a budeme moci regex/logiku spravne rozsirit.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
import sync_esbirka_text as fetcher

TARGET_VERSION_IRIS = [
    "esel-esb:eli/cz/sb/2025/72/2025-04-04",  # priklad od Radka
]


def main():
    stream = fetcher.fetch_gunzip_stream("003PravniAktZneniFragment.json.gz")
    import ijson

    count = 0
    matched = 0
    for item in ijson.items(stream, "položky.item"):
        count += 1
        if count % 2_000_000 == 0:
            print(f"...prosel {count} zaznamu", flush=True)
        iri = item.get("iri", "")
        if not iri:
            continue
        for v in TARGET_VERSION_IRIS:
            if iri == v or iri.startswith(v + "/"):
                cit = item.get("znění-fragment-citace")
                hh = item.get("znění-fragment-hierarchie-hex")
                frag = (item.get("právní-akt-fragment") or {}).get("fragment-id")
                print(f"MATCH iri={iri!r} citace={cit!r} hier={hh!r} fragment_id={frag!r}", flush=True)
                matched += 1
                break

    print(f"=== Hotovo. Proslo {count} zaznamu, nalezeno {matched} pro sledovane verze. ===", flush=True)


if __name__ == "__main__":
    main()
