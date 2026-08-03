#!/usr/bin/env python3
"""Data pull for the 'Overbooking' tab — booking-window risk model.

Rolling 90 days, all physicians. Buckets follow the business spec:
  0 Same day · 1 1 day before · 2 2 days · 3 3-4 days · 4 5-7 days
  5 8-14 days · 6 15+ days
Bookings created after the slot started are folded into "Same day" (walk-ins).

Outcomes: c completed · n no-show (MISSED, or touched after start without
completing) · r rescheduled before start · x cancelled before start.

The extra signals the overbooking model needs:
  late  : the release (reschedule/cancel) happened < 24h before the slot, i.e.
          too late to be re-sold in practice
  refill: another appointment was later created on the exact same provider+slot,
          i.e. the released time genuinely came back into use

Produces data_overbook.js:
  doctors : [name]
  cohort  : [doc, typ, channel, program, bucket, outcome, late, n]
  refill  : [doc, typ, channel, program, bucket, released, refilled]
  dow     : [doc, dow(0=Sun), outcome, n]
  hour    : [doc, hourIST, outcome, n]

Usage: python3 fetch_overbooking.py
Auth: AWS profile `redshift-data` (SSO).
"""
import json
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

PROFILE, CLUSTER, DATABASE = "redshift-data", "warehouse", "allo_prod"
HERE = Path(__file__).resolve().parent
DAYS = 90


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


DOC = "p.is_physician=1 AND p.deleted_at IS NULL"
WIN = f"DATEADD(minute,330,ap.start_time) >= DATEADD(day,-{DAYS},CURRENT_DATE)"
LEADD = """DATEDIFF(day, CAST(DATEADD(minute,330,ap.created_at) AS DATE),
                        CAST(DATEADD(minute,330,ap.start_time) AS DATE))"""
BUCKET = f"""CASE WHEN {LEADD} <= 0 THEN 0
                  WHEN {LEADD} = 1 THEN 1
                  WHEN {LEADD} = 2 THEN 2
                  WHEN {LEADD} <= 4 THEN 3
                  WHEN {LEADD} <= 7 THEN 4
                  WHEN {LEADD} <= 14 THEN 5
                  ELSE 6 END"""
OUTCOME = """CASE WHEN ap.status='COMPLETED' THEN 'c'
             WHEN ap.status='MISSED' OR ap.updated_at > ap.start_time THEN 'n'
             WHEN ap.status='RESCHEDULED' THEN 'r'
             WHEN ap.status='CANCELLED' THEN 'x' END"""
LATE = """CASE WHEN ap.status IN ('RESCHEDULED','CANCELLED')
           AND DATEDIFF(hour, COALESCE(ap.rescheduled_at, ap.updated_at), ap.start_time) < 24
          THEN 1 ELSE 0 END"""
TYP = "CASE WHEN t.code='SC' THEN 'SC' ELSE 'RPT' END"
CHAN = "CASE WHEN ap.mode='offline' THEN 'Offline' ELSE 'Online' END"
PROG = "CASE WHEN ap.program='mental_health' THEN 'MH' ELSE 'SH' END"
BASE = f"""FROM allo_consultations.appointments ap
JOIN allo_persons.providers p ON ap.provider_id=p.id AND {DOC}
JOIN allo_consultations.types t ON ap.type_id=t.id AND t.code IN ('SC','FU','RR','PQ')
WHERE ap.deleted_at IS NULL AND {WIN}
  AND ap.status IN ('COMPLETED','MISSED','RESCHEDULED','CANCELLED')"""

Q_COHORT = f"""SELECT ap.provider_id, p.name, {TYP} AS typ, {CHAN} AS channel, {PROG} AS program,
  {BUCKET} AS bucket, {OUTCOME} AS outcome, {LATE} AS late, COUNT(*) AS n
{BASE}
GROUP BY 1,2,3,4,5,6,7,8"""

Q_REFILL = f"""WITH rel AS (
  SELECT ap.id, ap.provider_id, ap.start_time,
         COALESCE(ap.rescheduled_at, ap.updated_at) AS moved_at,
         {TYP} AS typ, {CHAN} AS channel, {PROG} AS program, {BUCKET} AS bucket
  {BASE} AND ap.status IN ('RESCHEDULED','CANCELLED')
)
SELECT rel.provider_id, rel.typ, rel.channel, rel.program, rel.bucket,
  COUNT(*) AS released,
  SUM(CASE WHEN f.n_new > 0 THEN 1 ELSE 0 END) AS refilled
FROM rel
LEFT JOIN (
  SELECT r2.id, COUNT(c.id) AS n_new
  FROM rel r2
  JOIN allo_consultations.appointments c
    ON c.provider_id = r2.provider_id AND c.start_time = r2.start_time
   AND c.id <> r2.id AND c.deleted_at IS NULL
   AND c.created_at > r2.moved_at
   AND c.status IN ('COMPLETED','MISSED')
  GROUP BY 1
) f ON f.id = rel.id
GROUP BY 1,2,3,4,5"""

# reschedule timing matrix: WHEN it was booked x WHEN it was moved (notice before the slot)
NOTICE = """DATEDIFF(day, CAST(DATEADD(minute,330,COALESCE(ap.rescheduled_at, ap.updated_at)) AS DATE),
                          CAST(DATEADD(minute,330,ap.start_time) AS DATE))"""
Q_RESMX = f"""SELECT ap.provider_id, {TYP} AS typ, {CHAN} AS channel, {PROG} AS program,
  {BUCKET} AS book_bucket,
  CASE WHEN {NOTICE} < 0 THEN 0            -- moved after the slot had already passed
       WHEN {NOTICE} = 0 THEN 1            -- same day as the slot
       WHEN {NOTICE} = 1 THEN 2
       WHEN {NOTICE} = 2 THEN 3
       WHEN {NOTICE} <= 4 THEN 4
       WHEN {NOTICE} <= 7 THEN 5
       ELSE 6 END AS notice_bucket,
  COUNT(*) AS n
{BASE} AND ap.status='RESCHEDULED'
GROUP BY 1,2,3,4,5,6"""

Q_DOW = f"""SELECT ap.provider_id, EXTRACT(DOW FROM DATEADD(minute,330,ap.start_time)) AS dow,
  {OUTCOME} AS outcome, COUNT(*) AS n
{BASE} GROUP BY 1,2,3"""

Q_HOUR = f"""SELECT ap.provider_id, EXTRACT(HOUR FROM DATEADD(minute,330,ap.start_time)) AS hr,
  {OUTCOME} AS outcome, COUNT(*) AS n
{BASE} GROUP BY 1,2,3"""


def main():
    coh = run_query(Q_COHORT, "cohort risk")
    docs, idx = [], {}
    def di(pid, name=None):
        if pid not in idx:
            idx[pid] = len(docs); docs.append(name or pid)
        elif name and docs[idx[pid]] == pid:
            docs[idx[pid]] = name
        return idx[pid]
    cohort = [[di(r[0], r[1]), r[2], r[3], r[4], int(r[5]), r[6], int(r[7]), r[8]] for r in coh]
    refill = [[di(r[0]), r[1], r[2], r[3], int(r[4]), r[5], r[6]]
              for r in run_query(Q_REFILL, "release refill") if r[0] in idx]
    resmx = [[di(r[0]), r[1], r[2], r[3], int(r[4]), int(r[5]), r[6]]
             for r in run_query(Q_RESMX, "reschedule timing matrix") if r[0] in idx]
    dow = [[di(r[0]), int(r[1]), r[2], r[3]] for r in run_query(Q_DOW, "weekday") if r[0] in idx]
    hour = [[di(r[0]), int(r[1]), r[2], r[3]] for r in run_query(Q_HOUR, "hour") if r[0] in idx]

    payload = {"updated": datetime.now().strftime("%Y-%m-%d %H:%M IST"), "days": DAYS,
               "doctors": docs, "cohort": cohort, "refill": refill, "resmx": resmx, "dow": dow, "hour": hour}
    out = HERE / "data_overbook.js"
    out.write_text("window.OB_DATA = " + json.dumps(payload, separators=(",", ":")) + ";\n")
    sys.stderr.write(f"[done] {len(docs)} doctors -> {out}\n")
    print(str(out))


if __name__ == "__main__":
    main()
