#!/usr/bin/env python3
"""Refresh data.js for the Doctor Utilization tracker.

Pulls doctor x day utilization facts (corrected roster-slots query: deduped
physical windows, type-matched appointment attribution) for the last 2 months
from Redshift and writes them into data.js next to index.html.

Auth: AWS profile `redshift-data` (SSO). If the session has expired, run:
        aws sso login --profile redshift-data
Usage: python3 fetch_data.py
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

SC = "cd02525c-1528-4047-a12c-1ad526c28c9a"
RPT = "871a9ff6-e076-4fef-9aee-14c566e67d71"
SPECIAL = "c7d8c9d2-f389-4e8f-a260-71110195b83f"


def qual(t):
    return f"""{t}.overlaps_non_bookable_block=0 AND {t}.is_realized=1
AND (({t}.is_booked=1 AND {t}.overlaps_other_booked_type=0)
  OR ({t}.available_for_booking=1 AND (({t}.type_id='{SC}' AND {t}.in_repeat_boundary=0)
                                    OR ({t}.type_id='{RPT}' AND {t}.in_repeat_boundary=1))))"""


QUERY = f"""WITH raw_slots AS (
  SELECT DISTINCT rs.provider_id, rs.location_id,
    CASE WHEN rs.type_id='{SC}' THEN 'SC' ELSE 'RPT' END AS slot_type,
    rs.start_time, rs.end_time,
    pro.name AS doctor_name, l.city, l.locality,
    CASE WHEN abtm.offline_location_id IS NOT NULL THEN 'Offline' ELSE 'Online' END AS channel
  FROM allo_consultations.roster_slots rs
  LEFT JOIN allo_persons.providers pro ON rs.provider_id = pro.id
  LEFT JOIN (SELECT DISTINCT *, COALESCE(offline_location_id, online_location_id) AS block_location_id
             FROM allo_consultations.appointment_block_type_maps WHERE deleted_at IS NULL) abtm
    ON rs.block_id = abtm.appointment_block_id
  LEFT JOIN allo_health.locations l ON abtm.block_location_id = l.id
  WHERE abtm.block_location_id = rs.location_id
    AND rs.type_id IN ('{SC}','{RPT}')
    AND DATEADD(minute, 330, rs.start_time) >= DATEADD(month, -2, CURRENT_DATE)
    AND {qual('rs')}
    AND (rs.location_id != '{SPECIAL}'
         OR NOT EXISTS (SELECT 1 FROM allo_consultations.roster_slots rs2
                        WHERE rs2.provider_id=rs.provider_id AND rs2.type_id=rs.type_id
                          AND rs2.start_time=rs.start_time AND rs2.location_id != '{SPECIAL}'
                          AND {qual('rs2')}))),
slot_data AS (
  SELECT provider_id, slot_dt, doctor_name, city, locality,
         slot_start_ts, slot_end_ts, slot_duration, slot_type, channel
  FROM (SELECT provider_id,
               CAST(DATEADD(minute,330,start_time) AS DATE) AS slot_dt,
               doctor_name, city, locality,
               DATEADD(minute,330,start_time) AS slot_start_ts,
               DATEADD(minute,330,end_time) AS slot_end_ts,
               DATEDIFF(minute,start_time,end_time) AS slot_duration,
               slot_type, channel,
               ROW_NUMBER() OVER (PARTITION BY provider_id, slot_type, start_time
                                  ORDER BY CASE WHEN channel='Offline' THEN 0 ELSE 1 END,
                                           end_time, location_id) AS loc_rn
        FROM raw_slots) x WHERE loc_rn = 1),
appointment_data AS (
  SELECT app.id AS appt_id, app.provider_id,
         DATEADD(minute,330,app.start_time) AS appt_start_ts,
         CASE WHEN typ.name='Screening Call' THEN 'SC' ELSE 'RPT' END AS appt_slot_type,
         CASE WHEN app.status='COMPLETED' THEN 'COMPLETED'
              WHEN app.updated_at > app.start_time THEN 'No Show' ELSE NULL END AS appt_final_status
  FROM allo_consultations.appointments app
  JOIN allo_persons.providers pro ON app.provider_id=pro.id AND pro.deleted_at IS NULL
  JOIN allo_health.locations loc ON app.location_id=loc.id AND loc.deleted_at IS NULL
  JOIN allo_consultations.types typ ON app.type_id=typ.id AND typ.deleted_at IS NULL
  WHERE app.deleted_at IS NULL
    AND typ.name IN ('Screening Call','Follow Up','Report Reading','Patient Queries')
    AND DATEADD(minute, 330, app.start_time) >= DATEADD(month, -2, CURRENT_DATE)),
sad AS (
  SELECT s.provider_id, s.slot_dt, s.doctor_name, s.city, s.locality,
         s.slot_type, s.channel, s.slot_start_ts, s.slot_end_ts, s.slot_duration,
         a.appt_id, a.appt_final_status,
         ROW_NUMBER() OVER (PARTITION BY a.appt_id
                            ORDER BY DATEDIFF(minute,s.slot_start_ts,a.appt_start_ts) ASC,
                                     CASE WHEN s.channel='Offline' THEN 0 ELSE 1 END,
                                     s.slot_end_ts, s.slot_start_ts) AS rn
  FROM slot_data s
  LEFT JOIN appointment_data a
    ON s.provider_id=a.provider_id AND s.slot_type=a.appt_slot_type
   AND a.appt_final_status IS NOT NULL
   AND a.appt_start_ts >= s.slot_start_ts AND a.appt_start_ts < s.slot_end_ts),
ss AS (
  SELECT provider_id, slot_dt, doctor_name, city, locality,
         slot_type, channel, slot_duration, slot_start_ts, slot_end_ts,
         SUM(CASE WHEN rn=1 AND appt_final_status='COMPLETED' THEN 1 ELSE 0 END) AS nc,
         SUM(CASE WHEN rn=1 AND appt_final_status='No Show' THEN 1 ELSE 0 END) AS nns
  FROM sad GROUP BY 1,2,3,4,5,6,7,8,9,10)
SELECT slot_dt AS dt, city, locality, doctor_name AS doctor,
  COUNT(CASE WHEN slot_type='SC' THEN 1 END) AS sc_slots,
  SUM(CASE WHEN slot_type='SC' THEN slot_duration ELSE 0 END) AS sc_min,
  SUM(CASE WHEN slot_type='SC' AND channel='Offline' AND (nc+nns)>0 THEN 1 ELSE 0 END) AS sc_off_gross_slots,
  SUM(CASE WHEN slot_type='SC' AND channel='Offline' AND (nc+nns)>0 THEN slot_duration ELSE 0 END) AS sc_off_gross_min,
  SUM(CASE WHEN slot_type='SC' AND channel='Offline' AND nc>0 THEN 1 ELSE 0 END) AS sc_off_net_slots,
  SUM(CASE WHEN slot_type='SC' AND channel='Offline' AND nc>0 THEN slot_duration ELSE 0 END) AS sc_off_net_min,
  SUM(CASE WHEN slot_type='SC' AND channel='Online' AND (nc+nns)>0 THEN 1 ELSE 0 END) AS sc_on_gross_slots,
  SUM(CASE WHEN slot_type='SC' AND channel='Online' AND (nc+nns)>0 THEN slot_duration ELSE 0 END) AS sc_on_gross_min,
  SUM(CASE WHEN slot_type='SC' AND channel='Online' AND nc>0 THEN 1 ELSE 0 END) AS sc_on_net_slots,
  SUM(CASE WHEN slot_type='SC' AND channel='Online' AND nc>0 THEN slot_duration ELSE 0 END) AS sc_on_net_min,
  COUNT(CASE WHEN slot_type='RPT' THEN 1 END) AS fu_slots,
  SUM(CASE WHEN slot_type='RPT' THEN slot_duration ELSE 0 END) AS fu_min,
  SUM(CASE WHEN slot_type='RPT' AND (nc+nns)>0 THEN 1 ELSE 0 END) AS fu_gross_slots,
  SUM(CASE WHEN slot_type='RPT' AND (nc+nns)>0 THEN slot_duration ELSE 0 END) AS fu_gross_min,
  SUM(CASE WHEN slot_type='RPT' AND nc>0 THEN 1 ELSE 0 END) AS fu_net_slots,
  SUM(CASE WHEN slot_type='RPT' AND nc>0 THEN slot_duration ELSE 0 END) AS fu_net_min
FROM ss
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4"""


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


def main():
    sys.stderr.write("[query] executing...\n")
    stmt = aws("redshift-data", "execute-statement",
               "--cluster-identifier", CLUSTER, "--database", DATABASE, "--sql", QUERY)
    sid = stmt["Id"]
    for _ in range(450):  # up to 15 min
        time.sleep(2)
        desc = aws("redshift-data", "describe-statement", "--id", sid)
        st = desc["Status"]
        if st == "FINISHED":
            break
        if st in ("FAILED", "ABORTED"):
            sys.stderr.write(f"ERROR: query {st}: {desc.get('Error')}\n")
            sys.exit(1)
    else:
        sys.stderr.write("ERROR: query timed out\n")
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
                    row.append(v[:10] if i == 0 else v)  # dt -> YYYY-MM-DD
                elif "longValue" in cell:
                    row.append(cell["longValue"])
                elif "doubleValue" in cell:
                    row.append(cell["doubleValue"])
                else:
                    row.append(list(cell.values())[0])
            rows.append(row)
        token = result.get("NextToken")
        if not token:
            break

    cols = ["dt", "city", "locality", "doctor",
            "sc_slots", "sc_min", "sc_off_gross_slots", "sc_off_gross_min",
            "sc_off_net_slots", "sc_off_net_min", "sc_on_gross_slots", "sc_on_gross_min",
            "sc_on_net_slots", "sc_on_net_min", "fu_slots", "fu_min",
            "fu_gross_slots", "fu_gross_min", "fu_net_slots", "fu_net_min"]
    payload = {
        "updated": datetime.now().strftime("%Y-%m-%d %H:%M IST"),
        "columns": cols,
        "rows": rows,
    }
    out = HERE / "data.js"
    out.write_text("window.UTIL_DATA = " + json.dumps(payload, separators=(",", ":")) + ";\n")
    sys.stderr.write(f"[done] {len(rows)} doctor-day rows -> {out}\n")
    print(str(out))


if __name__ == "__main__":
    main()
