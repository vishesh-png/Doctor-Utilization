#!/bin/zsh
# Daily refresh for the Doctor/Therapist Utilization tracker.
# Pulls fresh data from Redshift, commits the data files, pushes to
# vishesh-png/Doctor-Utilization (which redeploys GitHub Pages).
# Run by launchd: com.allo.doctor-utilization-refresh (daily 08:45).
# Needs the `redshift-data` AWS SSO profile to be logged in; if the token has
# expired the fetch fails loudly below and nothing is committed.

set -e
export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin"
cd /Users/visheshrawat/allo-bi/doctor-utilization-tracker

echo "=== refresh $(date '+%Y-%m-%d %H:%M:%S') ==="

python3 fetch_data.py
python3 fetch_therapist_data.py

if git diff --quiet -- data.js data_therapist.js; then
  echo "no data changes, skipping push"
  exit 0
fi

git add data.js data_therapist.js
git -c user.name="vishesh-png" -c user.email="vishesh@allohealth.care" \
  commit -m "auto-refresh data $(date '+%Y-%m-%d')"
git push origin main
echo "pushed $(date '+%Y-%m-%d %H:%M:%S')"
