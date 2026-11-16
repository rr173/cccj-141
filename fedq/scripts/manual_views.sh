#!/bin/sh
# 手动复做：分进程启动三个独立版本源，演示 打开→查看→查询→推进→重启→淘汰→关闭 全流程，
# 并用独立穷举程序核对最大闭合向量（不与生产选择共享代码）。
set -eu
cd "$(dirname "$0")/.."
D=/tmp/fedq-views-manual
rm -rf "$D"; mkdir -p "$D/logs" "$D/cfg" "$D/out"

echo "== 1) 导出三源历史与协调配置 =="
python3 -m fedq gen-scenarios --out-dir "$D/cfg"

echo "== 2) 独立穷举核对（枚举所有候选向量）=="
python3 -m fedq enumcheck --config "$D/cfg/coordinator.json" | tail -3

echo "== 3) 分进程启动三个独立版本实例 =="
python3 -m fedq serve-history --history "$D/cfg/A.json" --port 8021 \
    --log "$D/logs/A.http.jsonl" &
PA=$!
python3 -m fedq serve-history --history "$D/cfg/B.json" --port 8022 \
    --log "$D/logs/B.http.jsonl" &
PB=$!
python3 -m fedq serve-history --history "$D/cfg/C.json" --port 8023 \
    --log "$D/logs/C.http.jsonl" &
PC=$!
trap 'kill $PA $PB $PC 2>/dev/null || true' EXIT
sleep 1

cat > "$D/cfg/urls.json" <<'JSON'
{"sources":{"A":{"url":"http://127.0.0.1:8021"},
            "B":{"url":"http://127.0.0.1:8022"},
            "C":{"url":"http://127.0.0.1:8023"}}}
JSON

echo "== 4) 打开依赖闭合视图（期望 F=(1,1,1)）=="
OPEN=$(python3 -m fedq view-open --config "$D/cfg/urls.json" --workdir "$D/coord")
echo "$OPEN"
VID=$(echo "$OPEN" | python3 -c "import sys,json;print(json.load(sys.stdin)['view_id'])")

echo "== 5) 查看视图（F、退回解释、源身份、每页 HTTP 读取依据）=="
python3 -m fedq view-show --view-id "$VID" --workdir "$D/coord" | head -40

python3 -c "
import sys; sys.path.insert(0,'.')
from fedq.scenarios import view_queries
import json
for name,q,_ in view_queries():
    json.dump(q, open('$D/cfg/'+name+'.json','w'), ensure_ascii=False)
"

echo "== 6) 同一视图内多份查询（不同表/列/连接，绑定同一 F）=="
for f in "$D"/cfg/q*.json; do
    nm=$(basename "$f" .json)
    python3 -m fedq view-run --view-id "$VID" --query "$f" \
        --out "$D/out/$nm.jsonl" --name "$nm" --workdir "$D/coord"
done

echo "== 7) 推进三源到全闭合（A->4 B->4 C->3），新视图看到新版本 =="
python3 -m fedq admin --source A --url http://127.0.0.1:8021 --head 4 >/dev/null
python3 -m fedq admin --source B --url http://127.0.0.1:8022 --head 4 >/dev/null
python3 -m fedq admin --source C --url http://127.0.0.1:8023 --head 3 >/dev/null
python3 -m fedq view-open --config "$D/cfg/urls.json" --workdir "$D/coord" \
    | python3 -c "import sys,json;print('新视图 F =', json.load(sys.stdin)['F'])"

echo "== 8) 重启协调者（新进程）后凭原 view_id 查询，再淘汰旧状态 =="
python3 -m fedq view-run --view-id "$VID" --query "$D/cfg/q1_A_items过滤排序.json" \
    --out "$D/out/reuse.jsonl" --name reuse --workdir "$D/coord"
python3 -m fedq admin --source A --url http://127.0.0.1:8021 --earliest 4 >/dev/null
python3 -m fedq admin --source B --url http://127.0.0.1:8022 --earliest 4 >/dev/null
python3 -m fedq admin --source C --url http://127.0.0.1:8023 --earliest 3 >/dev/null
python3 -m fedq view-run --view-id "$VID" --query "$D/cfg/q1_A_items过滤排序.json" \
    --out "$D/out/after-evict.jsonl" --name after-evict --workdir "$D/coord"
echo "淘汰后老视图仍可读 ✓"

echo "== 9) 关闭后拒绝新查询 =="
python3 -m fedq view-close --view-id "$VID" --workdir "$D/coord"
python3 -m fedq view-list --workdir "$D/coord"
echo
echo "HTTP 日志（可见实例身份、提交位置 at、视图身份 view_id）:"
head -3 "$D/logs/A.http.jsonl"
