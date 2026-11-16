#!/bin/sh
# 结果数据血缘：一键端到端验收（内嵌两个版本源 A/B，跑全部血缘场景）。
# 场景：主例袋重数vs备选、left补null与真实null、全null/空集、正负抵消、
#       重复投影、同表两次引用、共享子推导（不枚举组合）、大组多页、反查多页、
#       过滤/limit 淘汰不混入、独立参照三方对照、证据写失败不冒充成功、
#       关闭视图+停源后新进程导航。
# 演示结束后视图已关闭、源已停止；随后用 CLI（新进程）凭证据库继续导航。
set -eu
cd "$(dirname "$0")/.."
WORKDIR="${1:-/tmp/fedq-lineage-demo}"
echo "工作目录: $WORKDIR"
python3 -m fedq lineage-demo --workdir "$WORKDIR"

COORD="$WORKDIR/coord"
TRANSCRIPT="$WORKDIR/cli-transcript.txt"
: > "$TRANSCRIPT"
say() { echo "$@" | tee -a "$TRANSCRIPT"; }

say ""
say "================ 以下为视图关闭、源停止后的新进程导航 ================"
RUNS=$(python3 -m fedq lineage-runs --workdir "$COORD")
say "$RUNS"
R1=$(echo "$RUNS" | python3 -c "
import sys, json
for line in sys.stdin:
    r = json.loads(line)
    if r['name'].startswith('L1_'):
        print(r['run_id']); break")
R2=$(echo "$RUNS" | python3 -c "
import sys, json
for line in sys.stdin:
    r = json.loads(line)
    if r['name'].startswith('L2_'):
        print(r['run_id']); break")
R7=$(echo "$RUNS" | python3 -c "
import sys, json
for line in sys.stdin:
    r = json.loads(line)
    if r['name'].startswith('L7_'):
        print(r['run_id']); break")

say ""
say "== lineage-show（核对查询/源坐标/校验和）=="
python3 -m fedq lineage-show --run-id "$R1" --workdir "$COORD" \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print(json.dumps({'state':d['state'],'verified':d['verified'],'sources':d['sources'],'columns':d['columns']},ensure_ascii=False))" | tee -a "$TRANSCRIPT"

say ""
say "== lineage-rows（L1 结果，带 row_ref）=="
python3 -m fedq lineage-rows --run-id "$R1" --workdir "$COORD" --page-size 5 | tee -a "$TRANSCRIPT"
REF=$(python3 -m fedq lineage-rows --run-id "$R1" --workdir "$COORD" --page-size 5 \
    | python3 -c "import sys,json;print(json.load(sys.stdin)['items'][0]['row_ref'])")

say ""
say "== lineage-expand（L1 汇总行 → 两个匹配对，page-size=1 逐页）=="
python3 -m fedq lineage-expand --row-ref "$REF" --workdir "$COORD" --page 0 --page-size 1 | tee -a "$TRANSCRIPT"
python3 -m fedq lineage-expand --row-ref "$REF" --workdir "$COORD" --page 1 --page-size 1 | tee -a "$TRANSCRIPT"

say ""
say "== lineage-expand（L2 的 distinct 结果 → 两个备选依据）=="
DREF=$(python3 -m fedq lineage-rows --run-id "$R2" --workdir "$COORD" --page-size 5 \
    | python3 -c "import sys,json;print(json.load(sys.stdin)['items'][0]['row_ref'])")
MREF=$(python3 -m fedq lineage-expand --row-ref "$DREF" --workdir "$COORD" \
    | python3 -c "import sys,json;print(json.load(sys.stdin)['derivation']['members']['items'][0]['row_ref'])")
python3 -m fedq lineage-expand --row-ref "$MREF" --workdir "$COORD" --page-size 5 | tee -a "$TRANSCRIPT"

say ""
say "== lineage-lookup（L7 自连接：flows pk=1 经两个输入位置被使用）=="
python3 -m fedq lineage-lookup --run-id "$R7" --source A --table flows --pk 1 \
    --workdir "$COORD" --page-size 10 | tee -a "$TRANSCRIPT"

say ""
echo "演示产物: $WORKDIR"
echo "  结果与展开样例: $WORKDIR/results/"
echo "  查询文件:       $WORKDIR/queries/"
echo "  证据库:         $COORD/lineage/<run_id>/（含一个 error 失败目录）"
echo "  CLI 导航留痕:   $TRANSCRIPT"
