#!/bin/sh
# 手动复做：分进程启动两个版本源，走 打开视图→带血缘执行→读结果→展开→反查→
# 关闭视图→（新进程）继续导航 的全流程。所有命令与真实输出都留在工作目录。
set -eu
cd "$(dirname "$0")/.."
D=/tmp/fedq-lineage-manual
rm -rf "$D"; mkdir -p "$D/logs" "$D/cfg" "$D/out"

echo "== 1) 导出血缘演示夹具（两源历史 + 查询 + 协调者配置）=="
python3 -m fedq lineage-fixtures --out-dir "$D/cfg"

echo "== 2) 分进程启动两个版本源 =="
python3 -m fedq serve-history --history "$D/cfg/A.json" --port 8031 \
    --log "$D/logs/A.http.jsonl" &
PA=$!
python3 -m fedq serve-history --history "$D/cfg/B.json" --port 8032 \
    --log "$D/logs/B.http.jsonl" &
PB=$!
trap 'kill $PA $PB 2>/dev/null || true' EXIT
sleep 1

echo "== 3) 打开读取视图（版本固定）=="
OPEN=$(python3 -m fedq view-open --config "$D/cfg/coordinator.json" --workdir "$D/coord")
echo "$OPEN"
VID=$(echo "$OPEN" | python3 -c "import sys,json;print(json.load(sys.stdin)['view_id'])")

echo "== 4) 带血缘执行主例两个查询 =="
python3 -m fedq lineage-run --view-id "$VID" --workdir "$D/coord" \
    --query "$D/cfg/q_L1_主例_直接连接汇总.json" --name L1 | tee "$D/out/L1.run.json"
python3 -m fedq lineage-run --view-id "$VID" --workdir "$D/coord" \
    --query "$D/cfg/q_L2_主例_投影distinct再汇总.json" --name L2 | tee "$D/out/L2.run.json"
R1=$(python3 -m fedq lineage-runs --workdir "$D/coord" | python3 -c "
import sys, json
for line in sys.stdin:
    r = json.loads(line)
    if r['name'] == 'L1':
        print(r['run_id']); break")
R2=$(python3 -m fedq lineage-runs --workdir "$D/coord" | python3 -c "
import sys, json
for line in sys.stdin:
    r = json.loads(line)
    if r['name'] == 'L2':
        print(r['run_id']); break")

echo "== 5) 读结果（row_ref）并逐层展开 =="
python3 -m fedq lineage-rows --run-id "$R1" --workdir "$D/coord" | tee "$D/out/L1.rows.json"
REF=$(python3 -m fedq lineage-rows --run-id "$R1" --workdir "$D/coord" \
    | python3 -c "import sys,json;print(json.load(sys.stdin)['items'][0]['row_ref'])")
echo "-- 汇总行 $REF 的推导（两个匹配对逐份计入）--"
python3 -m fedq lineage-expand --row-ref "$REF" --workdir "$D/coord" \
    | tee "$D/out/L1.expand.json"
MREF=$(python3 -m fedq lineage-rows --run-id "$R2" --workdir "$D/coord" \
    | python3 -c "import sys,json;print(json.load(sys.stdin)['items'][0]['row_ref'])")
DREF=$(python3 -m fedq lineage-expand --row-ref "$MREF" --workdir "$D/coord" \
    | python3 -c "import sys,json;print(json.load(sys.stdin)['derivation']['members']['items'][0]['row_ref'])")
echo "-- L2 的 distinct 结果 $DREF 保留两个备选依据 --"
python3 -m fedq lineage-expand --row-ref "$DREF" --workdir "$D/coord" \
    | tee "$D/out/L2.expand_distinct.json"

echo "== 6) 按源行反查 =="
python3 -m fedq lineage-lookup --run-id "$R1" --source A --table accounts --pk 1 \
    --workdir "$D/coord" | tee "$D/out/L1.lookup_a1.json"
python3 -m fedq lineage-lookup --run-id "$R1" --source B --table entries --pk 3 \
    --workdir "$D/coord" | tee "$D/out/L1.lookup_z.json"

echo "== 7) 关闭视图、停掉源进程；新进程凭证据库继续导航 =="
python3 -m fedq view-close --view-id "$VID" --workdir "$D/coord"
kill $PA $PB 2>/dev/null || true
trap - EXIT
sleep 0.3
python3 -m fedq lineage-show --run-id "$R1" --workdir "$D/coord" \
    | python3 -c "import sys,json;d=json.load(sys.stdin);print('state',d['state'],'verified',d['verified'])"
python3 -m fedq lineage-expand --row-ref "$REF" --workdir "$D/coord" --page 0 --page-size 1 \
    | python3 -c "import sys,json;d=json.load(sys.stdin);print('展开成员总数',d['derivation']['members']['total'])"
python3 -m fedq lineage-runs --workdir "$D/coord"
echo
echo "产物目录: $D（HTTP 日志 logs/、run 与展开 out/、证据库 coord/lineage/）"
