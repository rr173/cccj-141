#!/bin/sh
# 可复做脚本：端到端演示（内嵌启动 A、B 两个 HTTP 模拟源，跑全部演示查询与失败边界）。
set -eu
cd "$(dirname "$0")/.."
WORKDIR="${1:-/tmp/fedq-demo}"
echo "工作目录: $WORKDIR"
python3 -m fedq demo --workdir "$WORKDIR"
echo
echo "HTTP 日志:   $WORKDIR/A.http.jsonl $WORKDIR/B.http.jsonl $WORKDIR/Btiny.http.jsonl"
echo "查询结果:    $WORKDIR/*.jsonl （首行 incomplete，结尾 ok 才算完整）"
