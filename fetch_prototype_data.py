#!/usr/bin/env python3
"""Data pull for the Doctor Slot Utilization Tracker prototype (prototype.html).

Single doctor x single day. Produces data_prototype.js with:
- slots: type (SC/RPT) x channel (Offline/Online) x unit duration -> offered/booked counts
- appts: type x channel (mode) x program (sexual_health / mental_health / null) x status
- configs: the doctor's slot durations per type x program

Usage: python3 fetch_prototype_data.py [provider_id] [day YYYY-MM-DD]
Defaults: Dr. Sandhiya Loganathan, Thursday of last full week.
Auth: AWS profile `redshift-data` (SSO).
"""
import json
import subprocess
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

PROFILE = "redshift-data"
CLUSTER = "warehouse"
DATABASE = "allo_prod"
HERE = Path(__file__).resolve().parent

SC = "cd02525c-1528-4047-a12c-1ad526c28c9a"
RPT = "871a9ff6-e076-4fef-9aee-14c566e67d71"

PROVIDER = sys.argv[1] if len(sys.argv) > 1 else "34571382-e65f-429e-a6d8-20b4c1f22fa7"
if len(sys.argv) > 2:
    DAY = date.fromisoformat(sys.argv[2])
else:
    today = date.today()
    DAY = today - timedelta(days=today.weekday() + 7) + timedelta(days=3)  # last week's Thursday


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
    for _ in range(300):
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


Q_DOCTOR = f"SELECT name FROM allo_persons.providers WHERE id='{PROVIDER}'"

Q_CONFIGS = f"""SELECT t.code, COALESCE(c.program,'any') AS program, c.duration_mins
FROM allo_consultations.consultation_type_configs c
JOIN allo_consultations.types t ON c.type_id=t.id
WHERE c.deleted_at IS NULL AND t.code IN ('SC','FU','RR','PQ')
  AND c.provider_id='{PROVIDER}'
ORDER BY 1,2"""

# one row per physical window (dedup across locations); channel is 3-way and
# PER CONSULTATION TYPE from the block's type maps: a block can offer SC offline-only
# while offering repeats offline+online (e.g. Dr. Sandhiya's clinic days)
Q_SLOTS = f"""SELECT typ, channel, dur, COUNT(*) AS slots, SUM(booked) AS booked
FROM (
  SELECT typ, start_time, channel, dur, booked,
         ROW_NUMBER() OVER (PARTITION BY typ, start_time ORDER BY dur) AS rn
  FROM (
    SELECT DISTINCT
           CASE WHEN rs.type_id='{SC}' THEN 'SC' ELSE 'RPT' END AS typ,
           rs.start_time,
           CASE WHEN bc.has_off=1 AND bc.has_on=1 THEN 'Both'
                WHEN bc.has_off=1 THEN 'Offline' ELSE 'Online' END AS channel,
           DATEDIFF(minute,rs.start_time,rs.end_time) AS dur,
           rs.is_booked AS booked
    FROM allo_consultations.roster_slots rs
    JOIN (SELECT appointment_block_id, consultation_type_id,
                 MAX(CASE WHEN offline_location_id IS NOT NULL THEN 1 ELSE 0 END) AS has_off,
                 MAX(CASE WHEN online_location_id IS NOT NULL THEN 1 ELSE 0 END) AS has_on
          FROM allo_consultations.appointment_block_type_maps WHERE deleted_at IS NULL
          GROUP BY 1,2) bc ON rs.block_id=bc.appointment_block_id AND rs.type_id=bc.consultation_type_id
    JOIN (SELECT DISTINCT appointment_block_id, COALESCE(offline_location_id, online_location_id) AS bl
          FROM allo_consultations.appointment_block_type_maps WHERE deleted_at IS NULL) abtm
      ON rs.block_id=abtm.appointment_block_id AND abtm.bl=rs.location_id
    WHERE rs.provider_id='{PROVIDER}'
      AND rs.type_id IN ('{SC}','{RPT}')
      AND CAST(DATEADD(minute,330,rs.start_time) AS DATE) = '{DAY}'
      AND rs.overlaps_non_bookable_block=0 AND rs.is_realized=1
      AND ((rs.is_booked=1 AND rs.overlaps_other_booked_type=0)
        OR (rs.available_for_booking=1 AND ((rs.type_id='{SC}' AND rs.in_repeat_boundary=0)
                                         OR (rs.type_id='{RPT}' AND rs.in_repeat_boundary=1))))
  ) r
) w WHERE rn=1
GROUP BY 1,2,3 ORDER BY 1,2,3"""

Q_APPTS = f"""SELECT
       CASE WHEN t.code='SC' THEN 'SC' ELSE 'RPT' END AS typ,
       CASE WHEN a.mode='offline' THEN 'Offline' ELSE 'Online' END AS channel,
       COALESCE(a.program,'unattributed') AS program,
       a.status,
       CASE WHEN a.updated_at > a.start_time THEN 1 ELSE 0 END AS uas,
       COUNT(*) AS n
FROM allo_consultations.appointments a
JOIN allo_consultations.types t ON a.type_id=t.id
WHERE a.provider_id='{PROVIDER}' AND a.deleted_at IS NULL
  AND t.code IN ('SC','FU','RR','PQ')
  AND CAST(DATEADD(minute,330,a.start_time) AS DATE) = '{DAY}'
GROUP BY 1,2,3,4,5 ORDER BY 1,2,3,4,5"""


def main():
    doctor = run_query(Q_DOCTOR, "doctor")[0][0]
    configs = [{"type": r[0], "program": r[1], "mins": r[2]} for r in run_query(Q_CONFIGS, "configs")]
    slots = [{"typ": r[0], "channel": r[1], "dur": r[2], "slots": r[3], "booked": r[4]}
             for r in run_query(Q_SLOTS, "slots")]
    appts = [{"typ": r[0], "channel": r[1], "program": r[2], "status": r[3], "uas": r[4], "n": r[5]}
             for r in run_query(Q_APPTS, "appointments")]
    payload = {
        "updated": datetime.now().strftime("%Y-%m-%d %H:%M IST"),
        "doctor": doctor,
        "provider_id": PROVIDER,
        "day": str(DAY),
        "configs": configs,
        "slots": slots,
        "appts": appts,
    }
    out = HERE / "data_prototype.js"
    out.write_text("window.PROTO_DATA = " + json.dumps(payload, separators=(",", ":")) + ";\n")
    sys.stderr.write(f"[done] {doctor} {DAY} -> {out}\n")
    print(str(out))


if __name__ == "__main__":
    main()
