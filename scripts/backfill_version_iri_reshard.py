"""
JEDNORAZOVE: doplni documents.version_iri v reshardech u dokumentu e-Sbirky
(reshard_and_clean_zakony.py ho pri kopii nepreneslo), aby novy
sync_esbirka_text_reshard.py poznal, ktere predpisy se od posledniho behu
zmenily.

Pravidlo: dokument v reshardu dostane version_iri ze stejneho dokumentu
(stejne id) ve starych shardech, POKUD se stary dokument od zkopirovani do
reshardu nezmenil (stary updated_at <= updated_at v reshardu). Zmenene
dokumenty (stary je novejsi) se nechaji s version_iri NULL - novy sync je
pak prepise aktualnim zneni z e-Sbirky.

DRY_RUN=true (vychozi) jen vypise pocty, nic nezapisuje.
"""

import os
import sys

from reshard_and_clean_zakony import TARGET_SHARDS, connect_target, connect_source

SOURCE_ID = "5804ffaa-c5c6-4f35-b5c7-48da040ed457"
OLD_SHARDS = ["do1997", "1998_2007", "2008_2020", "2021_dosud"]
DRY_RUN = os.environ.get("DRY_RUN", "true").lower() != "false"


def log(*a):
    print(*a)
    sys.stdout.flush()


def main():
    log("Backfill version_iri v reshardech, DRY_RUN=" + str(DRY_RUN))
    old = {}
    for name in OLD_SHARDS:
        if not os.environ.get("NEON_ZAKONY_" + name.upper() + "_DB_URL"):
            log("  [" + name + "] preskoceno - chybi env")
            continue
        conn = connect_source(None, name)
        with conn.cursor() as cur:
            cur.execute(
                "select id, version_iri, updated_at from documents "
                "where source_id = %s and version_iri is not null",
                (SOURCE_ID,),
            )
            for i, v, u in cur.fetchall():
                old[str(i)] = (v, u)
        conn.close()
        log("  [" + name + "] nacteno, celkem znamych dokumentu: " + str(len(old)))

    total_set = 0
    total_stale = 0
    total_missing = 0
    for name, env_key in TARGET_SHARDS:
        if not os.environ.get(env_key):
            continue
        conn = connect_target(env_key, retries=8)
        with conn.cursor() as cur:
            cur.execute(
                "select id, updated_at from documents where source_id = %s and version_iri is null",
                (SOURCE_ID,),
            )
            rows = cur.fetchall()
        upd = []
        stale = 0
        missing = 0
        for i, u in rows:
            o = old.get(str(i))
            if o is None:
                missing += 1
            elif o[1] <= u:
                upd.append((o[0], i))
            else:
                stale += 1
        if upd and not DRY_RUN:
            with conn.cursor() as cur:
                cur.executemany("update documents set version_iri = %s where id = %s", upd)
            conn.commit()
        conn.close()
        log("  [" + name + "] bez version_iri: " + str(len(rows)) + ", doplneno: " + str(len(upd))
            + ", zmeneno po kopii (ponechano NULL): " + str(stale) + ", bez protejsku ve starych: " + str(missing))
        total_set += len(upd)
        total_stale += stale
        total_missing += missing
    log("Hotovo. Doplneno " + str(total_set) + ", ke zmene/obnove " + str(total_stale) + ", bez protejsku " + str(total_missing))


if __name__ == "__main__":
    main()
