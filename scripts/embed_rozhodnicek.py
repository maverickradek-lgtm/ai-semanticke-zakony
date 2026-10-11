#!/usr/bin/env python3
"""Rozhodnicek: dopocita embeddingy (gemini-embedding-001, 256 dim) pro chunky bez embeddingu.

Env:
  NEON_ROZHODNICEK_DB_URL
  SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY   (pojistka kvoty, scripts/gemini_quota.py)
  GEMINI_API_KEY_POOL           klice oddelene carkou
  GEMINI_API_KEY_POOL_LABELS    nazvy secretu (stejne poradi) - pro sledovani kvoty
  CAP_FRACTION                  max. podil pozorovaneho denniho stropu na klic (vychozi 0.5)
  TIME_BUDGET_SECONDS           (vychozi 10000)
"""
import os
import sys
import time

import psycopg2
import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import gemini_quota  # noqa: E402

DB_URL = os.environ["NEON_ROZHODNICEK_DB_URL"]
SB_URL = os.environ.get("SUPABASE_URL", "")
SB_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")
KEYS = [k.strip() for k in os.environ.get("GEMINI_API_KEY_POOL", "").split(",")]
LABELS = [k.strip() for k in os.environ.get("GEMINI_API_KEY_POOL_LABELS", "").split(",")]
CAP = float(os.environ.get("CAP_FRACTION", "0.5"))
BUDGET = int(os.environ.get("TIME_BUDGET_SECONDS", "10000"))
DIM = 256
BATCH = 20
START = time.time()
URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-embedding-001:batchEmbedContents"

pool = [(k, LABELS[i] if i < len(LABELS) else "KEY_%d" % i) for i, k in enumerate(KEYS) if k]
dead = set()


def log(*a):
    print(*a, flush=True)


def pick_key():
    for k, label in pool:
        if label in dead:
            continue
        if SB_URL and not gemini_quota.check_quota(SB_URL, SB_KEY, label, CAP):
            dead.add(label)
            log("  klic %s dosahl dnesniho podilu, vyrazen" % label)
            continue
        return k, label
    return None, None


def embed_batch(texts):
    """vraci seznam vektoru, nebo None kdyz uz neni kam sahnout (kvota)"""
    for _ in range(len(pool) * 2 + 1):
        key, label = pick_key()
        if key is None:
            return None
        body = {"requests": [{
            "model": "models/gemini-embedding-001",
            "content": {"parts": [{"text": t[:8000]}]},
            "taskType": "RETRIEVAL_DOCUMENT",
            "outputDimensionality": DIM,
        } for t in texts]}
        try:
            r = requests.post(URL, headers={"x-goog-api-key": key}, json=body, timeout=90)
        except requests.RequestException as e:
            log("  sitova chyba: " + str(e)[:100])
            time.sleep(5)
            continue
        if r.status_code == 429:
            log("  429 u klice " + label)
            if SB_URL:
                gemini_quota.report_429(SB_URL, SB_KEY, label)
            dead.add(label)
            continue
        if not r.ok:
            log("  Gemini chyba %s: %s" % (r.status_code, r.text[:200]))
            time.sleep(5)
            continue
        out = []
        for e in r.json()["embeddings"]:
            v = e["values"]
            n = sum(x * x for x in v) ** 0.5
            out.append([x / n for x in v] if n > 0 else v)
        return out
    return None


def main():
    if not pool:
        log("Chybi Gemini klice")
        sys.exit(1)
    conn = psycopg2.connect(DB_URL, keepalives=1, keepalives_idle=30, keepalives_interval=10, keepalives_count=5)
    done = 0
    while time.time() - START < BUDGET:
        cur = conn.cursor()
        cur.execute("select id, content from chunks where embedding is null order by id limit %s", (BATCH,))
        rows = cur.fetchall()
        cur.close()
        if not rows:
            log("Vsechny chunky maji embedding.")
            break
        vecs = embed_batch([r[1] for r in rows])
        if vecs is None or len(vecs) != len(rows):
            log("Kvota vycerpana nebo chyba - koncim.")
            break
        cur = conn.cursor()
        for (cid, _), v in zip(rows, vecs):
            cur.execute("update chunks set embedding = %s::vector where id = %s",
                        ("[" + ",".join("%.6f" % x for x in v) + "]", cid))
        conn.commit()
        cur.close()
        done += len(rows)
        if done % 200 == 0:
            log("  ... embedovano %d (%d s)" % (done, int(time.time() - START)))
        time.sleep(0.5)
    cur = conn.cursor()
    cur.execute("select count(*) filter (where embedding is null), count(*) from chunks")
    left, total = cur.fetchone()
    log("=== Hotovo: embedovano %d, zbyva %d z %d ===" % (done, left, total))
    conn.close()


if __name__ == "__main__":
    main()
