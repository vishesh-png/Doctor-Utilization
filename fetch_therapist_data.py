#!/usr/bin/env python3
"""Refresh data_therapist.js for the Therapist Utilization tab.

Pulls therapist x day x channel (Offline/Online) utilization facts for TH
(Therapy) roster slots over the last 2 months, plus a per-therapist program
label (SH = sexual health, MH = mental health) derived from the program tag on
their therapy appointments (capacity itself carries no program tag, so program
is a therapist-level attribute).

Auth: AWS profile `redshift-data` (SSO). If expired: aws sso login --profile redshift-data
Usage: python3 fetch_therapist_data.py
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

TH = "fe5b19b4-5961-4036-bc5f-fb1009a27d64"

# TH slots have in_repeat_boundary=0 always; qualifying = booked (same-type) or still bookable.
SLOT_QUERY = f"""WITH raw_slots AS (
  SELECT DISTINCT rs.provider_id, rs.location_id, rs.start_time, rs.end_time,
    pro.name AS therapist,
    CASE WHEN abtm.offline_location_id IS NOT NULL THEN 'Offline' ELSE 'Online' END AS channel
  FROM allo_consultations.roster_slots rs
  JOIN allo_persons.providers pro ON rs.provider_id = pro.id AND pro.is_therapist = 1
  LEFT JOIN (SELECT DISTINCT *, COALESCE(offline_location_id, online_location_id) AS block_location_id
             FROM allo_consultations.appointment_block_type_maps WHERE deleted_at IS NULL) abtm
    ON rs.block_id = abtm.appointment_block_id
  WHERE abtm.block_location_id = rs.location_id
    AND rs.type_id = '{TH}'
    AND DATEADD(minute, 330, rs.start_time) >= DATEADD(month, -2, CURRENT_DATE)
    AND rs.overlaps_non_bookable_block = 0
    AND rs.is_realized = 1
    AND ((rs.is_booked = 1 AND rs.overlaps_other_booked_type = 0)
         OR rs.available_for_booking = 1)),
slot_data AS (
  SELECT provider_id, slot_dt, therapist, slot_start_ts, slot_end_ts, slot_duration, channel
  FROM (SELECT provider_id,
               CAST(DATEADD(minute,330,start_time) AS DATE) AS slot_dt,
               therapist,
               DATEADD(minute,330,start_time) AS slot_start_ts,
               DATEADD(minute,330,end_time) AS slot_end_ts,
               DATEDIFF(minute,start_time,end_time) AS slot_duration,
               channel,
               ROW_NUMBER() OVER (PARTITION BY provider_id, start_time
                                  ORDER BY CASE WHEN channel='Offline' THEN 0 ELSE 1 END,
                                           end_time, location_id) AS loc_rn
        FROM raw_slots) x WHERE loc_rn = 1),
appointment_data AS (
  SELECT app.id AS appt_id, app.provider_id,
         DATEADD(minute,330,app.start_time) AS appt_start_ts,
         CASE WHEN app.status='COMPLETED' THEN 'COMPLETED'
              WHEN app.updated_at > app.start_time THEN 'No Show' ELSE NULL END AS appt_final_status
  FROM allo_consultations.appointments app
  JOIN allo_persons.providers pro ON app.provider_id=pro.id AND pro.deleted_at IS NULL AND pro.is_therapist=1
  WHERE app.deleted_at IS NULL
    AND app.type_id = '{TH}'
    AND DATEADD(minute, 330, app.start_time) >= DATEADD(month, -2, CURRENT_DATE)),
sad AS (
  SELECT s.provider_id, s.slot_dt, s.therapist, s.channel,
         s.slot_start_ts, s.slot_duration,
         a.appt_id, a.appt_final_status,
         ROW_NUMBER() OVER (PARTITION BY a.appt_id
                            ORDER BY DATEDIFF(minute,s.slot_start_ts,a.appt_start_ts) ASC,
                                     s.slot_start_ts) AS rn
  FROM slot_data s
  LEFT JOIN appointment_data a
    ON s.provider_id=a.provider_id
   AND a.appt_final_status IS NOT NULL
   AND a.appt_start_ts >= s.slot_start_ts AND a.appt_start_ts < s.slot_end_ts),
ss AS (
  SELECT provider_id, slot_dt, therapist, channel, slot_start_ts, slot_duration,
         SUM(CASE WHEN rn=1 AND appt_final_status='COMPLETED' THEN 1 ELSE 0 END) AS nc,
         SUM(CASE WHEN rn=1 AND appt_final_status='No Show' THEN 1 ELSE 0 END) AS nns
  FROM sad GROUP BY 1,2,3,4,5,6)
SELECT slot_dt AS dt, therapist, channel,
  COUNT(*) AS slots,
  SUM(slot_duration) AS mins,
  SUM(CASE WHEN (nc+nns)>0 THEN 1 ELSE 0 END) AS gross_slots,
  SUM(CASE WHEN (nc+nns)>0 THEN slot_duration ELSE 0 END) AS gross_min,
  SUM(CASE WHEN nc>0 THEN 1 ELSE 0 END) AS net_slots,
  SUM(CASE WHEN nc>0 THEN slot_duration ELSE 0 END) AS net_min
FROM ss
GROUP BY 1,2,3
ORDER BY 1,2,3"""

PROGRAM_QUERY = f"""SELECT pro.name AS therapist,
  SUM(CASE WHEN app.program='mental_health' THEN 1 ELSE 0 END) AS mh_appts,
  SUM(CASE WHEN app.program='sexual_health' THEN 1 ELSE 0 END) AS sh_appts
FROM allo_consultations.appointments app
JOIN allo_persons.providers pro ON app.provider_id=pro.id AND pro.is_therapist=1
WHERE app.deleted_at IS NULL AND app.type_id='{TH}'
  AND DATEADD(minute, 330, app.start_time) >= DATEADD(month, -2, CURRENT_DATE)
GROUP BY 1"""


def aws(*args):
    cmd = ["aws", "--profile", PROFILE, "--output", "json", *args]
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0:
        sys.stderr.write(f"ERROR: {' '.join(cmd[:4])}...: {res.stderr}\n")
        low = res.stderr.lower()
        if "sso" in low or "credential" in low or "token" in low:
            sys.stderr.write("Hint: run `aws sso login --profile redshift-data` and retry.\n")
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
        st = desc["Status"]
        if st == "FINISHED":
            break
        if st in ("FAILED", "ABORTED"):
            sys.stderr.write(f"ERROR: query {label} {st}: {desc.get('Error')}\n")
            sys.exit(1)
    else:
        sys.stderr.write(f"ERROR: query {label} timed out\n")
        sys.exit(1)
    rows, token = [], None
    while True:
        args = ["redshift-data", "get-statement-result", "--id", sid]
        if token:
            args += ["--next-token", token]
        result = aws(*args)
        for rec in result["Records"]:
            row = []
            for i, cell in enumerate(rec):
                if cell.get("isNull"):
                    row.append(None)
                elif "stringValue" in cell:
                    v = cell["stringValue"]
                    row.append(v[:10] if i == 0 and label == "slots" else v)
                elif "longValue" in cell:
                    row.append(cell["longValue"])
                else:
                    row.append(list(cell.values())[0])
            rows.append(row)
        token = result.get("NextToken")
        if not token:
            break
    return rows


def main():
    slot_rows = run_query(SLOT_QUERY, "slots")
    prog_rows = run_query(PROGRAM_QUERY, "programs")
    prog = {}
    for name, mh, sh in prog_rows:
        prog[name] = "MH" if (mh or 0) > (sh or 0) else "SH"
    # rows: dt, therapist, channel, slots, mins, gross_slots, gross_min, net_slots, net_min
    out_rows = [[r[0], r[1], prog.get(r[1], "SH"), r[2], r[3], r[4], r[5], r[6], r[7], r[8]]
                for r in slot_rows]
    cols = ["dt", "therapist", "program", "channel",
            "slots", "mins", "gross_slots", "gross_min", "net_slots", "net_min"]
    payload = {
        "updated": datetime.now().strftime("%Y-%m-%d %H:%M IST"),
        "columns": cols,
        "rows": out_rows,
    }
    out = HERE / "data_therapist.js"
    out.write_text("window.THER_DATA = " + json.dumps(payload, separators=(",", ":")) + ";\n")
    sys.stderr.write(f"[done] {len(out_rows)} therapist-day-channel rows -> {out}\n")
    print(str(out))


if __name__ == "__main__":
    main()
