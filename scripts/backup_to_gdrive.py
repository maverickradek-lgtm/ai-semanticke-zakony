#!/usr/bin/env python3
"""
Zalohovaci skript ParagrAlf.

Spusti pg_dump nad vsemi pripojenymi databazemi (hlavni Supabase, Supabase
judikatura, vsech 14 Neon projektu) a nahraje komprimovane zalohy do sdilene
slozky na Google Disku pres OAuth (vlastni Google ucet, aby se pocitalo do
jeho ulozneho prostoru - servisni ucet zadny vlastni prostor nema a nahrani
by selhalo s chybou storageQuotaExceeded). Po uspesnem nahrani smaze
zalohy starsi nez posledni KEEP_LAST_N behu (retence).

Zadna databaze se timto skriptem nemodifikuje - pg_dump je pouze cteci
operace. Pripojovaci retezce se ctou vyhradne z environment promennych
(GitHub Actions secrets), nikdy nejsou v tomto souboru napevno.
"""
import datetime
import os
import subprocess
import sys
from pathlib import Path

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# ID slozky "ParagrAlf - zalohy databazi" na Google Disku (vlastni ji
# Radkuv Google ucet, OAuth token ma k ni pristup jako on sam).
DRIVE_FOLDER_ID = "1yk4JzSE4kWsWrSWEUzJkT57U5a5IQM0g"

# Kolik poslednich behu zalohovani se ma na Disku drzet. Pri kadenci
# jednou za ~14 dni odpovida KEEP_LAST_N = 4 retenci zhruba 2 mesice.
KEEP_LAST_N = 4

# (nazev pro soubor, jmeno env promenne s connection stringem)
DATABASES = [
    ("supabase_main", "SUPABASE_MAIN_DB_URL"),
    ("supabase_judikatura", "SUPABASE_JUDIKATURA_DB_URL"),
    ("neon_zakony_do1997", "NEON_ZAKONY_DO1997_DB_URL"),
    ("neon_zakony_1998_2007", "NEON_ZAKONY_1998_2007_DB_URL"),
    ("neon_zakony_2008_2020", "NEON_ZAKONY_2008_2020_DB_URL"),
    ("neon_zakony_2021_dosud", "NEON_ZAKONY_2021_DOSUD_DB_URL"),
    ("neon_celni", "NEON_CELNI_DB_URL"),
    ("neon_eru", "NEON_ERU_DB_URL"),
    ("neon_fs", "NEON_FS_DB_URL"),
    ("neon_mf", "NEON_MF_DB_URL"),
    ("neon_mmr", "NEON_MMR_DB_URL"),
    ("neon_mv", "NEON_MV_DB_URL"),
    ("neon_uohs", "NEON_UOHS_DB_URL"),
    ("neon_uoou", "NEON_UOOU_DB_URL"),
    ("neon_us", "NEON_US_DB_URL"),
    ("neon_ifrs", "NEON_IFRS_DB_URL"),
]


def log(msg: str) -> None:
    ts = datetime.datetime.utcnow().isoformat(timespec="seconds")
    print(f"[{ts}] {msg}", flush=True)


def dump_database(name: str, db_url: str, out_dir: Path) -> Path | None:
    """Spusti pg_dump v custom (komprimovanem) formatu. Cteci operace,
    nic v databazi nemeni."""
    out_path = out_dir / f"{name}.dump"
    cmd = [
        "pg_dump", db_url,
        "-Fc",  # custom format, uz komprimovany, obnovitelny pg_restore
        "--no-owner", "--no-privileges",
        "-f", str(out_path),
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=1800)
    except subprocess.TimeoutExpired:
        log(f"CHYBA {name}: pg_dump prekrocil casovy limit 30 min")
        return None
    if result.returncode != 0:
        stderr_tail = result.stderr[-1500:] if result.stderr else "(bez detailu)"
        log(f"CHYBA pri zalohovani {name}: {stderr_tail}")
        return None
    if not out_path.exists() or out_path.stat().st_size == 0:
        log(f"CHYBA {name}: vystupni soubor je prazdny")
        return None
    size_mb = out_path.stat().st_size / 1024 / 1024
    log(f"OK {name}: {size_mb:.1f} MB")
    return out_path


def get_drive_service():
    """OAuth prihlaseni jako Radkuv vlastni Google ucet (ne servisni ucet -
    ten nema vlastni ulozny prostor a nahravani souboru by selhalo).
    Refresh token byl ziskan jednorazove pres OAuth Playground a nema
    expiraci (OAuth klient je v produkcnim rezimu)."""
    creds = Credentials(
        token=None,
        refresh_token=os.environ["GDRIVE_OAUTH_REFRESH_TOKEN"],
        token_uri="https://oauth2.googleapis.com/token",
        client_id=os.environ["GDRIVE_OAUTH_CLIENT_ID"],
        client_secret=os.environ["GDRIVE_OAUTH_CLIENT_SECRET"],
        scopes=["https://www.googleapis.com/auth/drive"],
    )
    creds.refresh(Request())
    return build("drive", "v3", credentials=creds)


def upload_run_folder(service, local_dir: Path, run_name: str) -> str:
    folder_metadata = {
        "name": run_name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [DRIVE_FOLDER_ID],
    }
    folder = service.files().create(body=folder_metadata, fields="id").execute()
    folder_id = folder["id"]
    for f in sorted(local_dir.glob("*.dump")):
        media = MediaFileUpload(str(f), mimetype="application/octet-stream", resumable=True)
        file_metadata = {"name": f.name, "parents": [folder_id]}
        service.files().create(body=file_metadata, media_body=media, fields="id").execute()
        log(f"Nahrano na Disk: {f.name}")
    return folder_id


def prune_old_backups(service, keep_last_n: int) -> None:
    resp = service.files().list(
        q=(
            f"'{DRIVE_FOLDER_ID}' in parents "
            "and mimeType = 'application/vnd.google-apps.folder' "
            "and trashed = false"
        ),
        fields="files(id, name, createdTime)",
        orderBy="createdTime desc",
        pageSize=100,
    ).execute()
    folders = resp.get("files", [])
    to_delete = folders[keep_last_n:]
    for f in to_delete:
        log(f"Mazu starou zalohu (retence {keep_last_n}): {f['name']}")
        service.files().delete(fileId=f["id"]).execute()


def main() -> None:
    run_name = datetime.datetime.utcnow().strftime("%Y-%m-%d")
    work_dir = Path("/tmp/paragralf-backup") / run_name
    work_dir.mkdir(parents=True, exist_ok=True)

    ok, failed = [], []
    for name, env_var in DATABASES:
        db_url = os.environ.get(env_var)
        if not db_url:
            log(f"PRESKAKUJI {name}: chybi secret {env_var}")
            failed.append(name)
            continue
        result = dump_database(name, db_url, work_dir)
        (ok if result else failed).append(name)

    if not ok:
        log("Zadna databaze se nepodarila zazalohovat, koncim s chybou bez nahravani.")
        sys.exit(1)

    log(f"Nahravam {len(ok)} zaloh na Google Disk...")
    service = get_drive_service()
    upload_run_folder(service, work_dir, run_name)
    prune_old_backups(service, KEEP_LAST_N)

    log(f"Hotovo. Uspesne: {len(ok)} ({', '.join(ok)}).")
    if failed:
        log(f"Selhalo/preskoceno: {len(failed)} ({', '.join(failed)}).")
        sys.exit(1)


if __name__ == "__main__":
    main()
