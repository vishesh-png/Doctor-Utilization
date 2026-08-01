#!/usr/bin/env python3
"""Data pull for the 'Booking Analysis' tab (booking-window deep dive).

Goal: understand WHEN bookings arrive relative to the slot, so the roster can be
shaped to push gross utilization toward 100%.

Rolling 60 days, all physicians, aggregated server-side. Produces data_booking.js:
  doctors  : [name]  (rows below reference the index)
  lead     : [doc, typ, channel, program, leadBucket, outcome, n]
  hour     : [doc, slotHourIST, outcome, n]           demand + outcome by hour
  supply   : [doc, slotHourIST, blockMins]            roster capacity by hour
  dow      : [doc, dow(0=Mon), outcome, n]            demand by weekday
  supplyDow: [doc, dow, blockMins]                    capacity by weekday
  created  : [doc, createdHourIST, sameDay, n]        when patients actually book
  resched  : [doc, typ, channel, program, quality, rebookGap, newOutcome, slotRefilled, n]

Lead buckets (created_at -> slot start):
  0 late/walk-in (booked after start) · 1 same day <2h · 2 same day 2h+
  3 next day · 4 2-3 days · 5 4-7 days · 6 8-14 days · 7 15+ days
Outcomes: c completed · n no-show (MISSED or touched after start) · r rescheduled · x cancelled
Reschedule quality: bad = moved on the slot's own day, good = moved earlier.

Usage: python3 fetch_booking_analysis.py
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


DOC = "p.is_physician=1 AND p.deleted_at IS NULL"
WIN = f"DATEADD(minute,330,ap.start_time) >= DATEADD(day,-{DAYS},CURRENT_DATE)"
OUTCOME = """CASE WHEN ap.status='COMPLETED' THEN 'c'
            WHEN ap.status='MISSED' OR ap.updated_at > ap.start_time THEN 'n'
            WHEN ap.status='RESCHEDULED' THEN 'r'
            WHEN ap.status='CANCELLED' THEN 'x' END"""
TYP = "CASE WHEN t.code='SC' THEN 'SC' ELSE 'RPT' END"
CHAN = "CASE WHEN ap.mode='offline' THEN 'Offline' ELSE 'Online' END"
PROG = "CASE WHEN ap.program='mental_health' THEN 'MH' ELSE 'SH' END"
BASE = f"""FROM allo_consultations.appointments ap
JOIN allo_persons.providers p ON ap.provider_id=p.id AND {DOC}
JOIN allo_consultations.types t ON ap.type_id=t.id AND t.code IN ('SC','FU','RR','PQ')
WHERE ap.deleted_at IS NULL AND {WIN}
  AND ap.status IN ('COMPLETED','MISSED','RESCHEDULED','CANCELLED')"""

Q_LEAD = f"""SELECT ap.provider_id, p.name, {TYP} AS typ, {CHAN} AS channel, {PROG} AS program,
  CASE WHEN DATEDIFF(minute, ap.created_at, ap.start_time) < 0 THEN 0
       WHEN DATEDIFF(day, CAST(DATEADD(minute,330,ap.created_at) AS DATE),
                          CAST(DATEADD(minute,330,ap.start_time) AS DATE)) = 0
            AND DATEDIFF(minute, ap.created_at, ap.start_time) < 120 THEN 1
       WHEN DATEDIFF(day, CAST(DATEADD(minute,330,ap.created_at) AS DATE),
                          CAST(DATEADD(minute,330,ap.start_time) AS DATE)) = 0 THEN 2
       WHEN DATEDIFF(day, CAST(DATEADD(minute,330,ap.created_at) AS DATE),
                          CAST(DATEADD(minute,330,ap.start_time) AS DATE)) = 1 THEN 3
       WHEN DATEDIFF(day, CAST(DATEADD(minute,330,ap.created_at) AS DATE),
                          CAST(DATEADD(minute,330,ap.start_time) AS DATE)) <= 3 THEN 4
       WHEN DATEDIFF(day, CAST(DATEADD(minute,330,ap.created_at) AS DATE),
                          CAST(DATEADD(minute,330,ap.start_time) AS DATE)) <= 7 THEN 5
       WHEN DATEDIFF(day, CAST(DATEADD(minute,330,ap.created_at) AS DATE),
                          CAST(DATEADD(minute,330,ap.start_time) AS DATE)) <= 14 THEN 6
       ELSE 7 END AS bucket,
  {OUTCOME} AS outcome, COUNT(*) AS n
{BASE}
GROUP BY 1,2,3,4,5,6,7"""

Q_HOUR = f"""SELECT ap.provider_id, EXTRACT(HOUR FROM DATEADD(minute,330,ap.start_time)) AS hr,
  {OUTCOME} AS outcome, COUNT(*) AS n
{BASE}
GROUP BY 1,2,3"""

Q_DOW = f"""SELECT ap.provider_id,
  EXTRACT(DOW FROM DATEADD(minute,330,ap.start_time)) AS dow,
  {OUTCOME} AS outcome, COUNT(*) AS n
{BASE}
GROUP BY 1,2,3"""

Q_CREATED = f"""SELECT ap.provider_id, EXTRACT(HOUR FROM DATEADD(minute,330,ap.created_at)) AS hr,
  CASE WHEN CAST(DATEADD(minute,330,ap.created_at) AS DATE)
          = CAST(DATEADD(minute,330,ap.start_time) AS DATE) THEN 1 ELSE 0 END AS same_day,
  COUNT(*) AS n
{BASE}
GROUP BY 1,2,3"""

# roster capacity exploded to hour-of-day / weekday (blocks never cross midnight IST)
NUMS = """WITH nums AS (SELECT ROW_NUMBER() OVER () - 1 AS h
  FROM (SELECT 1 FROM allo_consultations.appointment_blocks LIMIT 24) z)"""
Q_SUPPLY = f"""{NUMS}
SELECT b.provider_id, n.h AS hr,
  SUM(GREATEST(0, DATEDIFF(minute,
    GREATEST(DATEADD(minute,330,b.start_time),
             DATEADD(hour, n.h, CAST(CAST(DATEADD(minute,330,b.start_time) AS DATE) AS TIMESTAMP))),
    LEAST(DATEADD(minute,330,b.end_time),
          DATEADD(hour, n.h+1, CAST(CAST(DATEADD(minute,330,b.start_time) AS DATE) AS TIMESTAMP)))))) AS mins
FROM allo_consultations.appointment_blocks b
JOIN allo_persons.providers p ON b.provider_id=p.id AND {DOC}
CROSS JOIN nums n
WHERE b.deleted_at IS NULL AND b.is_bookable=1
  AND DATEADD(minute,330,b.start_time) >= DATEADD(day,-{DAYS},CURRENT_DATE)
GROUP BY 1,2 HAVING SUM(GREATEST(0, DATEDIFF(minute,
    GREATEST(DATEADD(minute,330,b.start_time),
             DATEADD(hour, n.h, CAST(CAST(DATEADD(minute,330,b.start_time) AS DATE) AS TIMESTAMP))),
    LEAST(DATEADD(minute,330,b.end_time),
          DATEADD(hour, n.h+1, CAST(CAST(DATEADD(minute,330,b.start_time) AS DATE) AS TIMESTAMP)))))) > 0"""

Q_SUPPLY_DOW = f"""SELECT b.provider_id,
  EXTRACT(DOW FROM DATEADD(minute,330,b.start_time)) AS dow,
  SUM(DATEDIFF(minute,b.start_time,b.end_time)) AS mins
FROM allo_consultations.appointment_blocks b
JOIN allo_persons.providers p ON b.provider_id=p.id AND {DOC}
WHERE b.deleted_at IS NULL AND b.is_bookable=1
  AND DATEADD(minute,330,b.start_time) >= DATEADD(day,-{DAYS},CURRENT_DATE)
GROUP BY 1,2"""

# reschedules: quality (same-day = bad), how fast the patient re-books, what the
# new booking did, and whether the freed slot was taken by anyone else
Q_RESCHED = f"""WITH r AS (
  SELECT ap.id, ap.provider_id, ap.patient_id, ap.start_time,
         COALESCE(ap.rescheduled_at, ap.updated_at) AS moved_at,
         {TYP} AS typ, {CHAN} AS channel, {PROG} AS program
  {BASE} AND ap.status='RESCHEDULED'
),
nxt AS (
  SELECT rr.id, b.start_time AS new_start, b.status AS new_status,
         ROW_NUMBER() OVER (PARTITION BY rr.id ORDER BY b.created_at) AS rn
  FROM r rr
  JOIN allo_consultations.appointments b
    ON b.patient_id = rr.patient_id AND b.deleted_at IS NULL AND b.id <> rr.id
   AND b.created_at BETWEEN DATEADD(minute,-5,rr.moved_at) AND DATEADD(minute,5,rr.moved_at)
),
refill AS (
  SELECT rr.id, COUNT(c.id) AS n_refill
  FROM r rr
  LEFT JOIN allo_consultations.appointments c
    ON c.provider_id = rr.provider_id AND c.start_time = rr.start_time
   AND c.id <> rr.id AND c.deleted_at IS NULL AND c.created_at > rr.moved_at
  GROUP BY 1
)
SELECT r.provider_id, r.typ, r.channel, r.program,
  CASE WHEN CAST(DATEADD(minute,330,r.moved_at) AS DATE)
          = CAST(DATEADD(minute,330,r.start_time) AS DATE) THEN 'bad' ELSE 'good' END AS quality,
  CASE WHEN n.new_start IS NULL THEN 'none'
       WHEN DATEDIFF(day, CAST(DATEADD(minute,330,r.start_time) AS DATE),
                          CAST(DATEADD(minute,330,n.new_start) AS DATE)) <= 0 THEN 'same_day'
       WHEN DATEDIFF(day, CAST(DATEADD(minute,330,r.start_time) AS DATE),
                          CAST(DATEADD(minute,330,n.new_start) AS DATE)) <= 3 THEN 'd1_3'
       WHEN DATEDIFF(day, CAST(DATEADD(minute,330,r.start_time) AS DATE),
                          CAST(DATEADD(minute,330,n.new_start) AS DATE)) <= 7 THEN 'd4_7'
       ELSE 'd8plus' END AS rebook_gap,
  COALESCE(CASE WHEN n.new_status='COMPLETED' THEN 'c'
                WHEN n.new_status='MISSED' THEN 'n'
                WHEN n.new_status='RESCHEDULED' THEN 'r'
                WHEN n.new_status='CANCELLED' THEN 'x'
                WHEN n.new_status IS NULL THEN 'none' ELSE 'o' END,'none') AS new_outcome,
  CASE WHEN f.n_refill > 0 THEN 1 ELSE 0 END AS slot_refilled,
  COUNT(*) AS n
FROM r
LEFT JOIN nxt n ON n.id=r.id AND n.rn=1
LEFT JOIN refill f ON f.id=r.id
GROUP BY 1,2,3,4,5,6,7,8"""


def main():
    lead = run_query(Q_LEAD, "lead-time cohorts")
    docs, idx = [], {}
    def di(pid, name=None):
        if pid not in idx:
            idx[pid] = len(docs); docs.append(name or pid)
        elif name and docs[idx[pid]] == pid:
            docs[idx[pid]] = name
        return idx[pid]
    lead_rows = [[di(r[0], r[1]), r[2], r[3], r[4], r[5], r[6], r[7]] for r in lead]

    hour = [[di(r[0]), int(r[1]), r[2], r[3]] for r in run_query(Q_HOUR, "demand by hour") if r[0] in idx]
    dow = [[di(r[0]), int(r[1]), r[2], r[3]] for r in run_query(Q_DOW, "demand by weekday") if r[0] in idx]
    created = [[di(r[0]), int(r[1]), r[2], r[3]] for r in run_query(Q_CREATED, "booking creation hour") if r[0] in idx]
    supply = [[di(r[0]), int(r[1]), r[2]] for r in run_query(Q_SUPPLY, "capacity by hour") if r[0] in idx]
    supply_dow = [[di(r[0]), int(r[1]), r[2]] for r in run_query(Q_SUPPLY_DOW, "capacity by weekday") if r[0] in idx]
    resched = [[di(r[0]), r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8]]
               for r in run_query(Q_RESCHED, "reschedule quality + recovery") if r[0] in idx]

    payload = {
        "updated": datetime.now().strftime("%Y-%m-%d %H:%M IST"),
        "days": DAYS,
        "doctors": docs,
        "lead": lead_rows,
        "hour": hour,
        "dow": dow,
        "created": created,
        "supply": supply,
        "supplyDow": supply_dow,
        "resched": resched,
    }
    out = HERE / "data_booking.js"
    out.write_text("window.BOOK_DATA = " + json.dumps(payload, separators=(",", ":")) + ";\n")
    sys.stderr.write(f"[done] {len(docs)} doctors -> {out}\n")
    print(str(out))


if __name__ == "__main__":
    main()
