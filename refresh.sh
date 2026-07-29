#!/bin/zsh
# Hourly refresh for the Doctor/Therapist Utilization tracker.
# Pulls fresh data from Redshift, commits the data files, pushes to
# vishesh-png/Doctor-Utilization (which redeploys GitHub Pages).
# Run by launchd: com.allo.doctor-utilization-refresh (hourly at :45).
# Needs the `redshift-data` AWS SSO profile to be logged in; if the token has
# expired the fetch fails loudly below and nothing is committed.
# Commits only when the DATA changed (the embedded "updated" timestamp alone
# doesn't count), so hourly runs don't spam no-op commits.

set -e
export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin"
cd /Users/visheshrawat/allo-bi/doctor-utilization-tracker

echo "=== refresh $(date '+%Y-%m-%d %H:%M:%S') ==="

python3 fetch_data.py
python3 fetch_therapist_data.py

strip_ts() { sed 's/"updated":"[^"]*"//'; }
changed=0
for f in data.js data_therapist.js; do
  if ! diff -q <(git show "HEAD:$f" | strip_ts) <(strip_ts < "$f") >/dev/null 2>&1; then
    changed=1
  fi
done
if [ "$changed" = "0" ]; then
  echo "no data changes, reverting timestamp bump and skipping push"
  git checkout -- data.js data_therapist.js
  exit 0
fi

git add data.js data_therapist.js
git -c user.name="vishesh-png" -c user.email="vishesh@allohealth.care" \
  commit -m "auto-refresh data $(date '+%Y-%m-%d')"
git push origin main
echo "pushed $(date '+%Y-%m-%d %H:%M:%S')"
