#!/usr/bin/env python3
"""Data pull for the 'Utilization' tab of the prototype (hours-only view).

ALL physicians, last 60 days at day grain, so the tab's doctor + date filters
work client-side without re-querying:
- doctors: id -> name (indexed; rows below reference the index)
- configs: provider-specific slot durations per type x program
- blocks:  bookable block minutes per doctor x IST date (the denominator)
- caps:    per doctor x date x type: offline/online capability from block maps
- appts:   doctor x dt x type(SC/RPT) x channel(mode) x program(SH/MH) x outcome
           outcome: c=completed, n=no-show (MISSED or updated after start),
           r=rescheduled (before start), x=cancelled (before start)

Usage: python3 fetch_utilization_tab.py
Auth: AWS profile `redshift-data` (SSO).
"""
import json
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

PROFILE = "redshift-data"
CLUSTER = "warehouse"
DATABASE = "allo_prod"
HERE = Path(__file__).resolve().parent


def aws(*args):
    cmd = ["aws", "--profile", PROFILE, "--output", "json", *args]
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0:
        sys.stderr.write(f"ERROR: {res.stderr}\n")
        if any(k in res.stderr.lower() for k in ("sso", "credential", "token")):
            sys.stderr.write("Hint: run `aws sso login --profile redshift-data`.\n")
        sys.exit(1)
    return json.loads(res.stdout) if res.stdout.strip() else {}


def run_query(sql, label):
    sys.stderr.write(f"[{label}] executing...\n")
    stmt = aws("redshift-data", "execute-statement",
               "--cluster-identifier", CLUSTER, "--database", DATABASE, "--sql", sql)
    sid = stmt["Id"]
    for _ in range(450):
        time.sleep(2)
        desc = aws("redshift-data", "describe-statement", "--id", sid)
        if desc["Status"] == "FINISHED":
            break
        if desc["Status"] in ("FAILED", "ABORTED"):
            sys.stderr.write(f"ERROR: {label} {desc['Status']}: {desc.get('Error')}\n")
            sys.exit(1)
    else:
        sys.stderr.write(f"ERROR: {label} timed out\n")
        sys.exit(1)
    rows, token = [], None
    while True:
        args = ["redshift-data", "get-statement-result", "--id", sid]
        if token:
            args += ["--next-token", token]
        result = aws(*args)
        for rec in result["Records"]:
            rows.append([None if c.get("isNull") else list(c.values())[0] for c in rec])
        token = result.get("NextToken")
        if not token:
            break
    return rows


DOC_FILTER = """p.is_physician=1 AND p.deleted_at IS NULL"""

Q_BLOCKS = f"""SELECT b.provider_id, p.name, CAST(DATEADD(minute,330,b.start_time) AS DATE) AS dt,
  SUM(DATEDIFF(minute, b.start_time, b.end_time)) AS mins
FROM allo_consultations.appointment_blocks b
JOIN allo_persons.providers p ON b.provider_id=p.id AND {DOC_FILTER}
WHERE b.deleted_at IS NULL AND b.is_bookable=1
  AND DATEADD(minute,330,b.start_time) >= DATEADD(day,-60,CURRENT_DATE)
GROUP BY 1,2,3 ORDER BY 2,3"""

Q_CONFIGS = f"""SELECT c.provider_id, t.code, COALESCE(c.program,'any') AS program,
  MAX(c.duration_mins) AS mins
FROM allo_consultations.consultation_type_configs c
JOIN allo_consultations.types t ON c.type_id=t.id
JOIN allo_persons.providers p ON c.provider_id=p.id AND {DOC_FILTER}
WHERE c.deleted_at IS NULL AND t.code IN ('SC','FU','RR','PQ')
GROUP BY 1,2,3"""

# shrinkage = non-bookable blocks (is_bookable=0) that fall inside a bookable
# window, i.e. roster time the doctor gave back. Intervals are de-duplicated and
# clipped to the bookable window before summing, and capped per day so shrinkage
# can never exceed the block itself.
Q_SHRINK = f"""WITH bk AS (
  SELECT DISTINCT b.provider_id, b.start_time, b.end_time,
         CAST(DATEADD(minute,330,b.start_time) AS DATE) AS dt
  FROM allo_consultations.appointment_blocks b
  JOIN allo_persons.providers p ON b.provider_id=p.id AND {DOC_FILTER}
  WHERE b.deleted_at IS NULL AND b.is_bookable=1
    AND DATEADD(minute,330,b.start_time) >= DATEADD(day,-60,CURRENT_DATE)
),
nb AS (
  SELECT DISTINCT b.provider_id, b.start_time, b.end_time
  FROM allo_consultations.appointment_blocks b
  JOIN allo_persons.providers p ON b.provider_id=p.id AND {DOC_FILTER}
  WHERE b.deleted_at IS NULL AND b.is_bookable=0
    AND DATEADD(minute,330,b.end_time) >= DATEADD(day,-60,CURRENT_DATE)
)
SELECT provider_id, dt, SUM(overlap_mins) AS mins FROM (
  SELECT bk.provider_id, bk.dt, bk.start_time, bk.end_time,
         SUM(GREATEST(0, DATEDIFF(minute,
             GREATEST(bk.start_time, nb.start_time),
             LEAST(bk.end_time, nb.end_time)))) AS raw_mins,
         LEAST(SUM(GREATEST(0, DATEDIFF(minute,
             GREATEST(bk.start_time, nb.start_time),
             LEAST(bk.end_time, nb.end_time)))),
               DATEDIFF(minute, bk.start_time, bk.end_time)) AS overlap_mins
  FROM bk JOIN nb
    ON nb.provider_id = bk.provider_id
   AND nb.start_time < bk.end_time AND nb.end_time > bk.start_time
  GROUP BY 1,2,3,4
) x GROUP BY 1,2"""

Q_CAP = f"""SELECT b.provider_id, CAST(DATEADD(minute,330,b.start_time) AS DATE) AS dt,
  CASE WHEN t.code='SC' THEN 'SC' ELSE 'RPT' END AS typ,
  MAX(CASE WHEN abtm.offline_location_id IS NOT NULL THEN 1 ELSE 0 END) AS has_off,
  MAX(CASE WHEN abtm.online_location_id IS NOT NULL THEN 1 ELSE 0 END) AS has_on
FROM allo_consultations.appointment_blocks b
JOIN allo_persons.providers p ON b.provider_id=p.id AND {DOC_FILTER}
JOIN allo_consultations.appointment_block_type_maps abtm
  ON abtm.appointment_block_id=b.id AND abtm.deleted_at IS NULL
JOIN allo_consultations.types t ON abtm.consultation_type_id=t.id AND t.code IN ('SC','FU','RR','PQ')
WHERE b.deleted_at IS NULL AND b.is_bookable=1
  AND DATEADD(minute,330,b.start_time) >= DATEADD(day,-60,CURRENT_DATE)
GROUP BY 1,2,3"""

Q_APPTS = f"""SELECT a.provider_id, CAST(DATEADD(minute,330,a.start_time) AS DATE) AS dt,
       CASE WHEN t.code='SC' THEN 'SC' ELSE 'RPT' END AS typ,
       CASE WHEN a.mode='offline' THEN 'Offline' ELSE 'Online' END AS channel,
       CASE WHEN a.program='mental_health' THEN 'MH' ELSE 'SH' END AS program,
       CASE WHEN a.status='COMPLETED' THEN 'c'
            WHEN a.status='MISSED' OR a.updated_at > a.start_time THEN 'n'
            WHEN a.status='RESCHEDULED' THEN 'r'
            WHEN a.status='CANCELLED' THEN 'x' END AS outcome,
       COUNT(*) AS n
FROM allo_consultations.appointments a
JOIN allo_persons.providers p ON a.provider_id=p.id AND {DOC_FILTER}
JOIN allo_consultations.types t ON a.type_id=t.id
WHERE a.deleted_at IS NULL
  AND t.code IN ('SC','FU','RR','PQ')
  AND a.status IN ('COMPLETED','MISSED','RESCHEDULED','CANCELLED')
  AND DATEADD(minute,330,a.start_time) >= DATEADD(day,-60,CURRENT_DATE)
GROUP BY 1,2,3,4,5,6"""


def main():
    block_rows = run_query(Q_BLOCKS, "blocks")
    cfg_rows = run_query(Q_CONFIGS, "configs")
    cap_rows = run_query(Q_CAP, "capability")
    appt_rows = run_query(Q_APPTS, "appointments")

    docs, idx = [], {}
    def di(pid, name=None):
        if pid not in idx:
            idx[pid] = len(docs)
            docs.append(name or pid)
        elif name and docs[idx[pid]] == pid:
            docs[idx[pid]] = name
        return idx[pid]

    blocks = [[di(r[0], r[1]), str(r[2])[:10], r[3]] for r in block_rows]
    shrink = [[di(r[0]), str(r[1])[:10], r[2]] for r in run_query(Q_SHRINK, "shrinkage") if r[0] in idx]
    configs = [[di(r[0]), r[1], r[2], r[3]] for r in cfg_rows if r[0] in idx]
    caps = [[di(r[0]), str(r[1])[:10], r[2], r[3], r[4]] for r in cap_rows if r[0] in idx]
    appts = [[di(r[0]), str(r[1])[:10], r[2], r[3], r[4], r[5], r[6]]
             for r in appt_rows if r[0] in idx and r[5]]

    payload = {
        "updated": datetime.now().strftime("%Y-%m-%d %H:%M IST"),
        "doctors": docs,
        "blocks": blocks,   # [docIdx, dt, mins]  (gross bookable block time)
        "shrink": shrink,   # [docIdx, dt, mins]  (non-bookable time inside those blocks)
        "configs": configs, # [docIdx, typeCode, program, mins]
        "caps": caps,       # [docIdx, dt, typ, has_off, has_on]
        "appts": appts,     # [docIdx, dt, typ, channel, program, outcome, n]
    }
    out = HERE / "data_utilz.js"
    out.write_text("window.UTILZ_DATA = " + json.dumps(payload, separators=(",", ":")) + ";\n")
    sys.stderr.write(f"[done] {len(docs)} doctors, {len(blocks)} block-days, {len(appts)} appt rows -> {out}\n")
    print(str(out))


if __name__ == "__main__":
    main()
