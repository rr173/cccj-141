#!/bin/sh
# 持续查询订阅：分进程手动复做（真实 HTTP 源进程 + 独立维护进程 + CLI 推进/读取）。
#
# 流程：
#   1) 导出小夹具并分进程启动一个版本化源（head=0，只开放初始表）；
#   2) sub-register 在明确位置 p=0 初始化（一次快照），读初始 left 明细与聚合；
#   3) 逐笔 admin/publish 发布提交，sub-advance 推进、sub-changes 分页取净变化；
#   4) sub-status / sub-diag 看状态与访问诊断；sub-stop 停止后拒绝推进；
#   5) sub-fixtures 同时导出查询 JSON，供人工查看查询树。
set -eu
cd "$(dirname "$0")/.."
WORK="${1:-/tmp/fedq-sub-manual}"
PORT=8041
rm -rf "$WORK"
mkdir -p "$WORK"

echo "== 1) 导出小夹具（单源历史 + 配置 + 查询）"
python3 -m fedq sub-fixtures --out-dir "$WORK/fixtures" --port "$PORT"

echo "== 2) 分进程启动版本化源（head=0）"
python3 -m fedq serve-history --history "$WORK/fixtures/OD.json" \
    --port "$PORT" --log "$WORK/OD.http.jsonl" &
SRV=$!
trap 'kill $SRV 2>/dev/null || true' EXIT
sleep 1

WD="$WORK/subs"
echo "== 3) 登记 left 明细订阅与全局聚合订阅（位置 p=0）"
DETAIL=$(python3 -m fedq sub-register --config "$WORK/fixtures/sub_coordinator.json" \
    --query "$WORK/fixtures/queries/q_left明细.json" --workdir "$WD" \
    --note manual-detail | python3 -c 'import sys,json;print(json.load(sys.stdin)["sub_id"])')
AGG=$(python3 -m fedq sub-register --config "$WORK/fixtures/sub_coordinator.json" \
    --query "$WORK/fixtures/queries/q_left聚合.json" --workdir "$WD" \
    --note manual-agg | python3 -c 'import sys,json;print(json.load(sys.stdin)["sub_id"])')
echo "  明细订阅=$DETAIL 聚合订阅=$AGG"

echo "== 4) 初始结果（o1 无配送，left 补 null 行）"
python3 -m fedq sub-result --sub-id "$DETAIL" --workdir "$WD" \
    | python3 -c 'import sys,json;d=json.load(sys.stdin);[print(" ",r) for r in d["items"]]'

echo "== 5) 逐笔发布并推进，分页读取每笔净变化"
for SEQ in 1 2 3 4; do
    python3 -m fedq admin --source OD --url "http://127.0.0.1:$PORT" \
        --head "$SEQ" >/dev/null
    python3 -m fedq sub-advance --sub-id "$AGG" --to "$SEQ" --workdir "$WD" >/dev/null
    echo "  -- 提交 $SEQ 聚合净变化（after_seq=$((SEQ-1))）:"
    python3 -m fedq sub-changes --sub-id "$AGG" --after-seq "$((SEQ-1))" \
        --page-size 50 --workdir "$WD" \
        | python3 -c 'import sys,json;[print("   ",i) for i in json.load(sys.stdin)["items"]]'
done

echo "== 6) 状态与访问诊断（只重算受影响连接键/分组）"
python3 -m fedq sub-status --sub-id "$AGG" --workdir "$WD"
python3 -m fedq sub-diag --sub-id "$AGG" --workdir "$WD" \
    | python3 -c 'import sys,json;print("  累计诊断:",json.load(sys.stdin)["cumulative"])'

echo "== 7) 停止后拒绝推进（读取仍可用）"
python3 -m fedq sub-stop --sub-id "$DETAIL" --workdir "$WD"
python3 -m fedq sub-advance --sub-id "$DETAIL" --workdir "$WD" 2>/dev/null || \
    echo "  已按预期拒绝推进（SubscriptionClosed）"

echo
echo "手动复做完成。产物目录: $WORK"
