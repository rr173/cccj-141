#!/bin/sh
# 手动复做：分进程启动两个模拟源，用 plan/run 跑一个查询。
set -eu
cd "$(dirname "$0")/.."
D=/tmp/fedq-manual
mkdir -p "$D"
python3 -m fedq serve --source A --port 8011 --log "$D/A.http.jsonl" &
PA=$!
python3 -m fedq serve --source B --port 8012 --log "$D/B.http.jsonl" &
PB=$!
trap 'kill $PA $PB 2>/dev/null || true' EXIT
sleep 1
python3 -m fedq queries --out-dir "$D/queries"
echo "== 计划 =="
python3 -m fedq plan --query "$D/queries/q1_A文本等值候选超集.json"
echo "== 执行 =="
python3 -m fedq run --query "$D/queries/q1_A文本等值候选超集.json" \
    --out "$D/r1.jsonl" --print --workdir "$D/work"
echo "== A 的 HTTP 日志（可见远端先回 3 个候选行）=="
cat "$D/A.http.jsonl"
