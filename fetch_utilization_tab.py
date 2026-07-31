#!/usr/bin/env python3
"""Data pull for the 'Utilization' tab of the prototype (hours-only view).

Single doctor, last 60 days at day grain, so the tab's date filter works
client-side without re-querying:
- blocks: bookable block minutes per IST date (the "total hour" denominator)
- appts: dt x type(SC/RPT) x channel(mode) x program x status x updated-after-start
- configs: slot durations per type x program

Usage: python3 fetch_utilization_tab.py [provider_id]
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

PROVIDER = sys.argv[1] if len(sys.argv) > 1 else "34571382-e65f-429e-a6d8-20b4c1f22fa7"


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

Q_BLOCKS = f"""SELECT CAST(DATEADD(minute,330,b.start_time) AS DATE) AS dt,
  SUM(DATEDIFF(minute, b.start_time, b.end_time)) AS mins
FROM allo_consultations.appointment_blocks b
WHERE b.provider_id='{PROVIDER}' AND b.deleted_at IS NULL AND b.is_bookable=1
  AND DATEADD(minute,330,b.start_time) >= DATEADD(day,-60,CURRENT_DATE)
GROUP BY 1 ORDER BY 1"""

Q_APPTS = f"""SELECT CAST(DATEADD(minute,330,a.start_time) AS DATE) AS dt,
       CASE WHEN t.code='SC' THEN 'SC' ELSE 'RPT' END AS typ,
       CASE WHEN a.mode='offline' THEN 'Offline' ELSE 'Online' END AS channel,
       CASE WHEN a.program='mental_health' THEN 'MH' ELSE 'SH' END AS program,
       a.status,
       CASE WHEN a.updated_at > a.start_time THEN 1 ELSE 0 END AS uas,
       COUNT(*) AS n
FROM allo_consultations.appointments a
JOIN allo_consultations.types t ON a.type_id=t.id
WHERE a.provider_id='{PROVIDER}' AND a.deleted_at IS NULL
  AND t.code IN ('SC','FU','RR','PQ')
  AND DATEADD(minute,330,a.start_time) >= DATEADD(day,-60,CURRENT_DATE)
GROUP BY 1,2,3,4,5,6 ORDER BY 1,2,3,4,5,6"""


def main():
    doctor = run_query(Q_DOCTOR, "doctor")[0][0]
    configs = [{"type": r[0], "program": r[1], "mins": r[2]} for r in run_query(Q_CONFIGS, "configs")]
    blocks = [{"dt": str(r[0])[:10], "mins": r[1]} for r in run_query(Q_BLOCKS, "blocks")]
    appts = [{"dt": str(r[0])[:10], "typ": r[1], "channel": r[2], "program": r[3],
              "status": r[4], "uas": r[5], "n": r[6]} for r in run_query(Q_APPTS, "appointments")]
    payload = {
        "updated": datetime.now().strftime("%Y-%m-%d %H:%M IST"),
        "doctor": doctor,
        "provider_id": PROVIDER,
        "configs": configs,
        "blocks": blocks,
        "appts": appts,
    }
    out = HERE / "data_utilz.js"
    out.write_text("window.UTILZ_DATA = " + json.dumps(payload, separators=(",", ":")) + ";\n")
    sys.stderr.write(f"[done] {doctor}: {len(blocks)} block-days, {len(appts)} appt rows -> {out}\n")
    print(str(out))


if __name__ == "__main__":
    main()
