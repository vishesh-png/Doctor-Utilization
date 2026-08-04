#!/usr/bin/env python3
"""Refresh data_therapist.js for the Therapist Utilization tab.

Pulls therapist x day x channel (Offline/Online) utilization facts for TH
(Therapy) roster slots over the last 2 months, plus a per-therapist program
label (SH = sexual health, MH = mental health) derived from the program tag on
their therapy appointments (capacity itself carries no program tag, so program
is a therapist-level attribute).

IMPORTANT — parallel slot grids:
`roster_slots` materializes SEVERAL candidate grids over the SAME wall clock for
one block: one per program variant (rs.program NULL = SH, 'mental_health' = MH)
and per slot duration (30 / 40 / 45 min), each repeated at the block's offline
location and at the generic/online location. These tile the same time in
parallel — they are alternatives, NOT additive capacity. Counting them all (or
de-duplicating only on start_time) invents slots: e.g. Ms. Zaheen Saifi,
2026-08-01, one 11:00-13:40 block + one 13:40-14:20 block = 5 real 40-min slots,
but the naive grid gave 8 slots / 260 min.

Two guards, applied in order:
  1. SQL: keep only slots whose rs.program matches the program declared on the
     block's type map (NULL-safe). That picks one grid per block.
  2. Python (`tile`): greedy non-overlap sweep per therapist — sort by start,
     longest first, drop anything starting before the last kept slot ends.
     Catches the residual mixed-duration rows (~1.4%) that step 1 leaves.
Appointments are credited to the SURVIVING slots only, so numerator and
denominator always describe the same grid.

Auth: AWS profile `redshift-data` (SSO). If expired: aws sso login --profile redshift-data
Usage: python3 fetch_therapist_data.py
"""
import json
import subprocess
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path

PROFILE = "redshift-data"
CLUSTER = "warehouse"
DATABASE = "allo_prod"
HERE = Path(__file__).resolve().parent

TH = "fe5b19b4-5961-4036-bc5f-fb1009a27d64"

# One row per surviving candidate slot. TH slots have in_repeat_boundary=0
# always; qualifying = booked (same-type) or still bookable.
# channel is 3-way from the BLOCK's location mappings: Offline (offline-only),
# Online (online-only), Both (dual offline+online block).
SLOT_QUERY = f"""WITH blk AS (
  SELECT appointment_block_id, program,
         MAX(CASE WHEN offline_location_id IS NOT NULL THEN 1 ELSE 0 END) AS has_off,
         MAX(CASE WHEN online_location_id IS NOT NULL THEN 1 ELSE 0 END) AS has_on
  FROM allo_consultations.appointment_block_type_maps
  WHERE deleted_at IS NULL AND consultation_type_id = '{TH}'
  GROUP BY 1,2),
blk_loc AS (
  SELECT DISTINCT appointment_block_id,
         COALESCE(offline_location_id, online_location_id) AS block_location_id
  FROM allo_consultations.appointment_block_type_maps
  WHERE deleted_at IS NULL AND consultation_type_id = '{TH}'),
raw_slots AS (
  SELECT DISTINCT rs.provider_id, pro.name AS therapist,
    rs.start_time, rs.end_time,
    CASE WHEN b.has_off=1 AND b.has_on=1 THEN 'Both'
         WHEN b.has_off=1 THEN 'Offline' ELSE 'Online' END AS channel
  FROM allo_consultations.roster_slots rs
  JOIN allo_persons.providers pro ON rs.provider_id = pro.id AND pro.is_therapist = 1
  -- program-matched join: one grid per block, not the parallel SH/MH variants
  JOIN blk b ON rs.block_id = b.appointment_block_id
            AND COALESCE(b.program,'~') = COALESCE(rs.program,'~')
  JOIN blk_loc bl ON bl.appointment_block_id = rs.block_id
                 AND bl.block_location_id = rs.location_id
  WHERE rs.type_id = '{TH}'
    AND DATEADD(minute, 330, rs.start_time) >= DATEADD(month, -2, CURRENT_DATE)
    AND rs.overlaps_non_bookable_block = 0
    AND rs.is_realized = 1
    AND ((rs.is_booked = 1 AND rs.overlaps_other_booked_type = 0)
         OR rs.available_for_booking = 1))
SELECT provider_id, therapist, channel,
       DATEADD(minute,330,start_time) AS slot_start,
       DATEADD(minute,330,end_time)   AS slot_end,
       DATEDIFF(minute,start_time,end_time) AS slot_duration
FROM (SELECT r.*, ROW_NUMBER() OVER (PARTITION BY provider_id, start_time
                                     ORDER BY end_time DESC) AS rn
      FROM raw_slots r) x
WHERE rn = 1
ORDER BY provider_id, slot_start"""

# One row per creditable therapy appointment.
APPT_QUERY = f"""SELECT app.id AS appt_id, app.provider_id,
       DATEADD(minute,330,app.start_time) AS appt_start,
       CASE WHEN app.mode='offline' THEN 'Offline' ELSE 'Online' END AS appt_channel,
       CASE WHEN app.status='COMPLETED' THEN 'COMPLETED'
            WHEN app.updated_at > app.start_time THEN 'No Show' ELSE NULL END AS appt_final_status
FROM allo_consultations.appointments app
JOIN allo_persons.providers pro ON app.provider_id=pro.id AND pro.deleted_at IS NULL AND pro.is_therapist=1
WHERE app.deleted_at IS NULL
  AND app.type_id = '{TH}'
  AND DATEADD(minute, 330, app.start_time) >= DATEADD(month, -2, CURRENT_DATE)
  AND (app.status='COMPLETED' OR app.updated_at > app.start_time)"""

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
            for cell in rec:
                if cell.get("isNull"):
                    row.append(None)
                elif "stringValue" in cell:
                    row.append(cell["stringValue"])
                elif "longValue" in cell:
                    row.append(cell["longValue"])
                else:
                    row.append(list(cell.values())[0])
            rows.append(row)
        token = result.get("NextToken")
        if not token:
            break
    return rows


def tile(slots):
    """Greedy non-overlap sweep over one therapist's candidate slots.

    `roster_slots` can still hold two durations for the same window after the
    program-matched join (a 30-min and a 40-min grid over the same block). A
    therapist can only be in one of them, so sweep by start time, prefer the
    longer slot on ties, and drop anything that starts before the last kept
    slot ends. Returns the kept slots in start order.
    """
    kept, last_end = [], None
    for s in sorted(slots, key=lambda s: (s["start"], -s["dur"])):
        if last_end is not None and s["start"] < last_end:
            continue
        kept.append(s)
        last_end = s["end"]
    return kept


def main():
    slot_rows = run_query(SLOT_QUERY, "slots")
    appt_rows = run_query(APPT_QUERY, "appointments")
    prog_rows = run_query(PROGRAM_QUERY, "programs")

    prog = {}
    for name, mh, sh in prog_rows:
        prog[name] = "MH" if (mh or 0) > (sh or 0) else "SH"

    # ---- candidate slots -> one non-overlapping grid per therapist ----
    by_provider = defaultdict(list)
    for pid, therapist, channel, st, et, dur in slot_rows:
        by_provider[pid].append({"therapist": therapist, "channel": channel,
                                 "start": st, "end": et, "dur": int(dur),
                                 "c_off": 0, "ns_off": 0, "c_on": 0, "ns_on": 0})
    raw_n = len(slot_rows)
    kept_by_provider = {pid: tile(sl) for pid, sl in by_provider.items()}
    kept_n = sum(len(v) for v in kept_by_provider.values())
    sys.stderr.write(f"[tile] {raw_n} candidate slots -> {kept_n} non-overlapping "
                     f"({raw_n - kept_n} overlapping dropped)\n")

    # ---- credit each appointment to exactly one surviving slot ----
    unmatched = 0
    for appt_id, pid, appt_start, appt_channel, status in appt_rows:
        if status is None:
            continue
        hit = None
        for s in kept_by_provider.get(pid, ()):
            if s["start"] <= appt_start < s["end"]:
                hit = s
                break
            if s["start"] > appt_start:
                break
        if hit is None:
            unmatched += 1
            continue
        done = status == "COMPLETED"
        if appt_channel == "Offline":
            hit["c_off" if done else "ns_off"] += 1
        else:
            hit["c_on" if done else "ns_on"] += 1
    sys.stderr.write(f"[credit] {len(appt_rows)} appointments, {unmatched} outside any slot\n")

    # ---- aggregate to dt x therapist x channel ----
    agg = defaultdict(lambda: [0] * 10)
    for pid, slots in kept_by_provider.items():
        for s in slots:
            key = (s["start"][:10], s["therapist"], s["channel"])
            a = agg[key]
            d = s["dur"]
            g_off = 1 if (s["c_off"] + s["ns_off"]) > 0 else 0
            n_off = 1 if s["c_off"] > 0 else 0
            g_on = 1 if (s["c_on"] + s["ns_on"]) > 0 else 0
            n_on = 1 if s["c_on"] > 0 else 0
            a[0] += 1            # slots
            a[1] += d            # mins
            a[2] += g_off; a[3] += g_off * d
            a[4] += n_off; a[5] += n_off * d
            a[6] += g_on;  a[7] += g_on * d
            a[8] += n_on;  a[9] += n_on * d

    out_rows = [[dt, ther, prog.get(ther, "SH"), ch] + vals
                for (dt, ther, ch), vals in sorted(agg.items())]
    cols = ["dt", "therapist", "program", "channel", "slots", "mins",
            "g_off_slots", "g_off_min", "n_off_slots", "n_off_min",
            "g_on_slots", "g_on_min", "n_on_slots", "n_on_min"]
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
