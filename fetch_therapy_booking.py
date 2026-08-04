#!/usr/bin/env python3
"""Data pull for the 'Therapy Booking Analysis' tab — booking-window behaviour.

The therapy twin of fetch_overbooking.py. Same rolling 90-day window, same
buckets, same outcome rule; the only structural difference is that therapy is a
single consultation type, so a cohort is channel x program rather than
type x channel x program:
  Offline SH · Online SH · Offline MH · Online MH

Buckets (how far ahead the appointment was booked):
  0 Same day · 1 1 day · 2 2 days · 3 3-4 days · 4 5-7 days · 5 8-14 days · 6 15+
Bookings created after the slot started are folded into "Same day" (walk-ins).

Outcomes: c completed · n no-show · r rescheduled before start · x cancelled
before start. A booking touched only AFTER the slot had started never released
the time, so it is a no-show whatever the status column says — the same rule the
doctor Booking Analysis tab uses, which is what keeps the three tables
reconciling with each other.

Produces data_therbook.js:
  therapists : [name]
  cohort     : [th, channel, program, bucket, outcome, late, n]
  resmx      : [th, channel, program, book_bucket, notice_bucket, n]

Usage: python3 fetch_therapy_booking.py
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


THER = "p.is_therapist=1 AND p.deleted_at IS NULL"
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
MOVED = "COALESCE(ap.rescheduled_at, ap.updated_at)"
OUTCOME = f"""CASE WHEN ap.status='COMPLETED' THEN 'c'
             WHEN ap.status='MISSED' THEN 'n'
             WHEN ap.status='RESCHEDULED' AND {MOVED} < ap.start_time THEN 'r'
             WHEN ap.status='CANCELLED'   AND {MOVED} < ap.start_time THEN 'x'
             ELSE 'n' END"""
LATE = f"""CASE WHEN ap.status IN ('RESCHEDULED','CANCELLED')
           AND {MOVED} < ap.start_time
           AND DATEDIFF(hour, {MOVED}, ap.start_time) < 24
          THEN 1 ELSE 0 END"""
CHAN = "CASE WHEN ap.mode='offline' THEN 'Offline' ELSE 'Online' END"
PROG = "CASE WHEN ap.program='mental_health' THEN 'MH' ELSE 'SH' END"
BASE = f"""FROM allo_consultations.appointments ap
JOIN allo_persons.providers p ON ap.provider_id=p.id AND {THER}
JOIN allo_consultations.types t ON ap.type_id=t.id AND t.code='TH'
WHERE ap.deleted_at IS NULL AND {WIN}
  AND ap.status IN ('COMPLETED','MISSED','RESCHEDULED','CANCELLED')"""

Q_COHORT = f"""SELECT ap.provider_id, p.name, {CHAN} AS channel, {PROG} AS program,
  {BUCKET} AS bucket, {OUTCOME} AS outcome, {LATE} AS late, COUNT(*) AS n
{BASE}
GROUP BY 1,2,3,4,5,6,7"""

# reschedule timing matrix: WHEN it was booked x how much NOTICE the move gave.
# Only moves made before the slot are reschedules; the rest are no-shows above.
NOTICE = f"""DATEDIFF(day, CAST(DATEADD(minute,330,{MOVED}) AS DATE),
                           CAST(DATEADD(minute,330,ap.start_time) AS DATE))"""
Q_RESMX = f"""SELECT ap.provider_id, {CHAN} AS channel, {PROG} AS program,
  {BUCKET} AS book_bucket,
  CASE WHEN {NOTICE} = 0 THEN 1            -- same day as the slot
       WHEN {NOTICE} = 1 THEN 2
       WHEN {NOTICE} = 2 THEN 3
       WHEN {NOTICE} <= 4 THEN 4
       WHEN {NOTICE} <= 7 THEN 5
       ELSE 6 END AS notice_bucket,
  COUNT(*) AS n
{BASE} AND ap.status='RESCHEDULED' AND {MOVED} < ap.start_time
GROUP BY 1,2,3,4,5"""


def main():
    coh = run_query(Q_COHORT, "cohort")
    ths, idx = [], {}
    def ti(pid, name=None):
        if pid not in idx:
            idx[pid] = len(ths); ths.append(name or pid)
        elif name and ths[idx[pid]] == pid:
            ths[idx[pid]] = name
        return idx[pid]
    cohort = [[ti(r[0], r[1]), r[2], r[3], int(r[4]), r[5], int(r[6]), r[7]] for r in coh]
    resmx = [[ti(r[0]), r[1], r[2], int(r[3]), int(r[4]), r[5]]
             for r in run_query(Q_RESMX, "reschedule timing matrix") if r[0] in idx]

    payload = {"updated": datetime.now().strftime("%Y-%m-%d %H:%M IST"), "days": DAYS,
               "therapists": ths, "cohort": cohort, "resmx": resmx}
    out = HERE / "data_therbook.js"
    out.write_text("window.THERBOOK_DATA = " + json.dumps(payload, separators=(",", ":")) + ";\n")
    sys.stderr.write(f"[done] {len(ths)} therapists, {len(cohort)} cohort rows, "
                     f"{len(resmx)} matrix rows -> {out}\n")
    print(str(out))


if __name__ == "__main__":
    main()
