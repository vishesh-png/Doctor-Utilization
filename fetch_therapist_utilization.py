#!/usr/bin/env python3
"""Data pull for the 'Therapist Utilization' tab of the prototype (hours-only).

Mirrors fetch_utilization_tab.py exactly, with two differences:
  - providers are therapists (is_therapist=1), not physicians
  - there is only one consultation type (TH = Therapy), so the metric grid is
    channel x program -> Offline SH / Offline MH / Online SH / Online MH,
    instead of the doctors' type x channel x program

ALL therapists, last 60 days at day grain, so the tab's therapist + date
filters work client-side without re-querying:
- therapists: id -> name (indexed; rows below reference the index)
- configs: per-therapist therapy slot duration per program
- blocks:  bookable block minutes per therapist x IST date (the denominator)
- shrink:  non-bookable time clipped inside those bookable blocks
- caps:    per therapist x date: offline/online capability from block maps
- appts:   therapist x dt x channel x program(SH/MH) x outcome
- untag:   the slice of `appts` whose program tag is NULL (counted as SH, but
           reported so the tab can disclose how much of the split is inferred)

Outcome (same rule the Booking Analysis tab uses): a booking moved or cancelled
BEFORE the slot released the time (r/x); one touched only after the slot had
started never freed it, so it is a no-show (n) whatever the status column says.

Usage: python3 fetch_therapist_utilization.py
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
DAYS = 60


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
    sys.stderr.write(f"[{label}] {len(rows)} rows\n")
    return rows


THER = "p.is_therapist=1 AND p.deleted_at IS NULL"
TH_CODE = "'TH'"

Q_BLOCKS = f"""SELECT b.provider_id, p.name, CAST(DATEADD(minute,330,b.start_time) AS DATE) AS dt,
  SUM(DATEDIFF(minute, b.start_time, b.end_time)) AS mins
FROM allo_consultations.appointment_blocks b
JOIN allo_persons.providers p ON b.provider_id=p.id AND {THER}
WHERE b.deleted_at IS NULL AND b.is_bookable=1
  AND DATEADD(minute,330,b.start_time) >= DATEADD(day,-{DAYS},CURRENT_DATE)
GROUP BY 1,2,3 ORDER BY 2,3"""

# duration_mins=0 rows exist for a few therapists (unconfigured variants); they
# would silently zero out the hours, so drop them and let the client fall back.
Q_CONFIGS = f"""SELECT c.provider_id, COALESCE(c.program,'any') AS program,
  MAX(c.duration_mins) AS mins
FROM allo_consultations.consultation_type_configs c
JOIN allo_consultations.types t ON c.type_id=t.id AND t.code={TH_CODE}
JOIN allo_persons.providers p ON c.provider_id=p.id AND {THER}
WHERE c.deleted_at IS NULL AND c.duration_mins > 0
GROUP BY 1,2"""

# shrinkage = non-bookable blocks (is_bookable=0) that fall inside a bookable
# window, i.e. roster time the therapist gave back. Intervals are de-duplicated
# and clipped to the bookable window, and capped per block so shrinkage can
# never exceed the block itself.
Q_SHRINK = f"""WITH bk AS (
  SELECT DISTINCT b.provider_id, b.start_time, b.end_time,
         CAST(DATEADD(minute,330,b.start_time) AS DATE) AS dt
  FROM allo_consultations.appointment_blocks b
  JOIN allo_persons.providers p ON b.provider_id=p.id AND {THER}
  WHERE b.deleted_at IS NULL AND b.is_bookable=1
    AND DATEADD(minute,330,b.start_time) >= DATEADD(day,-{DAYS},CURRENT_DATE)
),
nb AS (
  SELECT DISTINCT b.provider_id, b.start_time, b.end_time
  FROM allo_consultations.appointment_blocks b
  JOIN allo_persons.providers p ON b.provider_id=p.id AND {THER}
  WHERE b.deleted_at IS NULL AND b.is_bookable=0
    AND DATEADD(minute,330,b.end_time) >= DATEADD(day,-{DAYS},CURRENT_DATE)
)
SELECT provider_id, dt, SUM(overlap_mins) AS mins FROM (
  SELECT bk.provider_id, bk.dt, bk.start_time, bk.end_time,
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
  MAX(CASE WHEN abtm.offline_location_id IS NOT NULL THEN 1 ELSE 0 END) AS has_off,
  MAX(CASE WHEN abtm.online_location_id IS NOT NULL THEN 1 ELSE 0 END) AS has_on
FROM allo_consultations.appointment_blocks b
JOIN allo_persons.providers p ON b.provider_id=p.id AND {THER}
JOIN allo_consultations.appointment_block_type_maps abtm
  ON abtm.appointment_block_id=b.id AND abtm.deleted_at IS NULL
JOIN allo_consultations.types t ON abtm.consultation_type_id=t.id AND t.code={TH_CODE}
WHERE b.deleted_at IS NULL AND b.is_bookable=1
  AND DATEADD(minute,330,b.start_time) >= DATEADD(day,-{DAYS},CURRENT_DATE)
GROUP BY 1,2"""

MOVED = "COALESCE(a.rescheduled_at, a.updated_at)"
OUTCOME = f"""CASE WHEN a.status='COMPLETED' THEN 'c'
       WHEN a.status='MISSED' THEN 'n'
       WHEN a.status='RESCHEDULED' AND {MOVED} < a.start_time THEN 'r'
       WHEN a.status='CANCELLED'   AND {MOVED} < a.start_time THEN 'x'
       ELSE 'n' END"""

Q_APPTS = f"""SELECT a.provider_id, CAST(DATEADD(minute,330,a.start_time) AS DATE) AS dt,
       CASE WHEN a.mode='offline' THEN 'Offline' ELSE 'Online' END AS channel,
       CASE WHEN a.program='mental_health' THEN 'MH' ELSE 'SH' END AS program,
       CASE WHEN a.program IS NULL THEN 1 ELSE 0 END AS untagged,
       {OUTCOME} AS outcome,
       COUNT(*) AS n
FROM allo_consultations.appointments a
JOIN allo_persons.providers p ON a.provider_id=p.id AND {THER}
JOIN allo_consultations.types t ON a.type_id=t.id AND t.code={TH_CODE}
WHERE a.deleted_at IS NULL
  AND a.status IN ('COMPLETED','MISSED','RESCHEDULED','CANCELLED')
  AND DATEADD(minute,330,a.start_time) >= DATEADD(day,-{DAYS},CURRENT_DATE)
GROUP BY 1,2,3,4,5,6"""


def main():
    block_rows = run_query(Q_BLOCKS, "blocks")
    cfg_rows = run_query(Q_CONFIGS, "configs")
    cap_rows = run_query(Q_CAP, "capability")
    appt_rows = run_query(Q_APPTS, "appointments")

    ths, idx = [], {}
    def ti(pid, name=None):
        if pid not in idx:
            idx[pid] = len(ths)
            ths.append(name or pid)
        elif name and ths[idx[pid]] == pid:
            ths[idx[pid]] = name
        return idx[pid]

    blocks = [[ti(r[0], r[1]), str(r[2])[:10], r[3]] for r in block_rows]
    shrink = [[ti(r[0]), str(r[1])[:10], r[2]]
              for r in run_query(Q_SHRINK, "shrinkage") if r[0] in idx]
    configs = [[ti(r[0]), r[1], r[2]] for r in cfg_rows if r[0] in idx]
    caps = [[ti(r[0]), str(r[1])[:10], r[2], r[3]] for r in cap_rows if r[0] in idx]
    appts, untag = [], []
    for r in appt_rows:
        if r[0] not in idx or not r[5]:
            continue
        row = [ti(r[0]), str(r[1])[:10], r[2], r[3], r[5], r[6]]
        appts.append(row)
        if int(r[4]) == 1:
            untag.append([row[0], row[1], row[2], row[4], row[5]])

    payload = {
        "updated": datetime.now().strftime("%Y-%m-%d %H:%M IST"),
        "days": DAYS,
        "therapists": ths,
        "blocks": blocks,   # [thIdx, dt, mins]  (gross bookable block time)
        "shrink": shrink,   # [thIdx, dt, mins]  (non-bookable time inside those blocks)
        "configs": configs, # [thIdx, program, mins]
        "caps": caps,       # [thIdx, dt, has_off, has_on]
        "appts": appts,     # [thIdx, dt, channel, program, outcome, n]
        "untag": untag,     # [thIdx, dt, channel, outcome, n]  (subset of appts, program was NULL)
    }
    out = HERE / "data_ther_utilz.js"
    out.write_text("window.THERUTILZ_DATA = " + json.dumps(payload, separators=(",", ":")) + ";\n")
    sys.stderr.write(f"[done] {len(ths)} therapists, {len(blocks)} block-days, "
                     f"{len(appts)} appt rows ({len(untag)} untagged) -> {out}\n")
    print(str(out))


if __name__ == "__main__":
    main()
