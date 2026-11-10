#!/usr/bin/env bash
# Launch the full KK/LCS experiment: one tmux session per variant (B is followed by the D_all sensitivity),
# plus kklcs_eval which waits for every metrics.json and then writes output/report/.
# Each session: site CV (5 cell folds; D/D_all skip the fold holding the only collocated pair) then temporal
# holdout (train < 2026-01-01, test KK Jan–Apr 2026). 3 LightGBM threads each, nice 10 (12 cores; the
# china_era5 / china_hima download sessions are left alone). Re-running resumes from finished folds.
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
PY=/home/work1/miniconda3/envs/Airqua_env/bin/python
LOG="$DIR/logs"
mkdir -p "$LOG"
COMMON="--cv site temporal --n-trials 30 --inner-k 5 --n-jobs 3"

launch() {   # session  log-name  variants...
  local s=$1 name=$2; shift 2
  if tmux has-session -t "$s" 2>/dev/null; then echo "tmux $s already exists — skipped"; return; fi
  tmux new-session -d -s "$s" -c "$DIR" \
    "nice -n 10 $PY -u run_kk_lcs.py --variant $* $COMMON 2>&1 | tee -a $LOG/$name.log"
  echo "launched tmux $s → $LOG/$name.log"
}

launch kklcs_A      A       A
launch kklcs_B_Dall B_Dall  B D_all
launch kklcs_C      C       C
launch kklcs_D      D       D

# E (supplementary: D correction + tuned LCS weight) waits for kklcs_A to finish so the box stays at 12 threads
if ! tmux has-session -t kklcs_E 2>/dev/null; then
  tmux new-session -d -s kklcs_E -c "$DIR" \
    "while tmux has-session -t kklcs_A 2>/dev/null; do sleep 120; done; \
     nice -n 10 $PY -u run_kk_lcs.py --variant E $COMMON 2>&1 | tee -a $LOG/E.log"
  echo "launched tmux kklcs_E → $LOG/E.log (starts when kklcs_A ends)"
fi

if ! tmux has-session -t kklcs_eval 2>/dev/null; then
  tmux new-session -d -s kklcs_eval -c "$DIR" \
    "until [ \$(ls output/{A,B,C,D,D_all,E}/{site,temporal}/metrics.json 2>/dev/null | wc -l) -eq 12 ]; do sleep 300; done; \
     $PY -u evaluate_kk_lcs.py --root output 2>&1 | tee -a $LOG/evaluate.log"
  echo "launched tmux kklcs_eval → $LOG/evaluate.log (waits for all 12 metrics.json)"
fi
