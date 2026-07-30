#!/usr/bin/env python3
"""Refresh data_weekly.js for the Weekly doctor deep-dive tracker (weekly.html).

Single doctor x single week: SC + repeat (FU/RR/PQ) slot supply, appointment
outcomes by program (SH / MH), and the doctor's configured slot durations per
type x program from consultation_type_configs.

Program attribution: program='mental_health' -> MH; 'sexual_health' or NULL -> SH
(verified: untagged appointments all have SH-config durations; MH bookings are
always tagged).

Usage: python3 fetch_weekly_data.py [provider_id] [week_start YYYY-MM-DD]
Defaults: Dr. Sandhiya Loganathan, last full Mon-Sun week.
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
    WEEK_START = date.fromisoformat(sys.argv[2])
else:
    today = date.today()
    WEEK_START = today - timedelta(days=today.weekday() + 7)  # last full week's Monday
WEEK_END = WEEK_START + timedelta(days=6)


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

Q_CONFIGS = f"""SELECT t.code, COALESCE(c.program,'any') AS program, c.duration_mins,
       CASE WHEN c.provider_id='{PROVIDER}' THEN 1 ELSE 0 END AS is_provider_override
FROM allo_consultations.consultation_type_configs c
JOIN allo_consultations.types t ON c.type_id=t.id
WHERE c.deleted_at IS NULL AND t.code IN ('SC','FU','RR','PQ')
  AND c.provider_id='{PROVIDER}'
ORDER BY 1,2"""

Q_APPTS = f"""SELECT t.code, COALESCE(a.program,'null') AS program, a.status,
       CAST(DATEADD(minute,330,a.start_time) AS DATE) AS dt,
       COUNT(*) AS n, SUM(DATEDIFF(minute,a.start_time,a.end_time)) AS mins
FROM allo_consultations.appointments a
JOIN allo_consultations.types t ON a.type_id=t.id
WHERE a.provider_id='{PROVIDER}' AND a.deleted_at IS NULL
  AND t.code IN ('SC','FU','RR','PQ')
  AND CAST(DATEADD(minute,330,a.start_time) AS DATE) BETWEEN '{WEEK_START}' AND '{WEEK_END}'
GROUP BY 1,2,3,4 ORDER BY 4,1,2,3"""

Q_SLOTS = f"""SELECT dt, typ, dur, COUNT(*) AS slots, SUM(booked) AS booked
FROM (
  SELECT CAST(DATEADD(minute,330,rs.start_time) AS DATE) AS dt,
         CASE WHEN rs.type_id='{SC}' THEN 'SC' ELSE 'RPT' END AS typ,
         rs.start_time,
         MAX(DATEDIFF(minute,rs.start_time,rs.end_time)) AS dur,
         MAX(rs.is_booked) AS booked
  FROM allo_consultations.roster_slots rs
  JOIN (SELECT DISTINCT appointment_block_id, COALESCE(offline_location_id, online_location_id) AS bl
        FROM allo_consultations.appointment_block_type_maps WHERE deleted_at IS NULL) m
    ON rs.block_id=m.appointment_block_id AND m.bl=rs.location_id
  WHERE rs.provider_id='{PROVIDER}'
    AND rs.type_id IN ('{SC}','{RPT}')
    AND CAST(DATEADD(minute,330,rs.start_time) AS DATE) BETWEEN '{WEEK_START}' AND '{WEEK_END}'
    AND rs.overlaps_non_bookable_block=0 AND rs.is_realized=1
    AND ((rs.is_booked=1 AND rs.overlaps_other_booked_type=0)
      OR (rs.available_for_booking=1 AND ((rs.type_id='{SC}' AND rs.in_repeat_boundary=0)
                                       OR (rs.type_id='{RPT}' AND rs.in_repeat_boundary=1))))
  GROUP BY 1,2,3
) x GROUP BY 1,2,3 ORDER BY 1,2,3"""

Q_LOC = f"""SELECT DISTINCT COALESCE(l.name,'?'), COALESCE(l.locality,''), COALESCE(l.city,'')
FROM allo_consultations.appointments a
JOIN allo_health.locations l ON a.location_id=l.id
WHERE a.provider_id='{PROVIDER}' AND a.deleted_at IS NULL
AND CAST(DATEADD(minute,330,a.start_time) AS DATE) BETWEEN '{WEEK_START}' AND '{WEEK_END}'"""


def main():
    doctor = run_query(Q_DOCTOR, "doctor")[0][0]
    configs = [{"type": r[0], "program": r[1], "mins": r[2]} for r in run_query(Q_CONFIGS, "configs")]
    appts = [{"type": r[0], "program": r[1], "status": r[2], "dt": str(r[3])[:10],
              "n": r[4], "mins": r[5]} for r in run_query(Q_APPTS, "appointments")]
    slots = [{"dt": str(r[0])[:10], "typ": r[1], "dur": r[2], "slots": r[3], "booked": r[4]}
             for r in run_query(Q_SLOTS, "slots")]
    locs = [" ".join(x for x in (r[0], r[1], r[2]) if x) for r in run_query(Q_LOC, "locations")]
    payload = {
        "updated": datetime.now().strftime("%Y-%m-%d %H:%M IST"),
        "doctor": doctor,
        "provider_id": PROVIDER,
        "locations": locs,
        "week_start": str(WEEK_START),
        "week_end": str(WEEK_END),
        "configs": configs,
        "appts": appts,
        "slots": slots,
    }
    out = HERE / "data_weekly.js"
    out.write_text("window.WEEKLY_DATA = " + json.dumps(payload, separators=(",", ":")) + ";\n")
    sys.stderr.write(f"[done] {doctor} week {WEEK_START}..{WEEK_END} -> {out}\n")
    print(str(out))


if __name__ == "__main__":
    main()
