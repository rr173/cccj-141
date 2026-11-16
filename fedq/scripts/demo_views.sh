#!/bin/sh
# 跨源读取视图：一键端到端验收（内嵌启动三个独立版本源 A/B/C，跑全部生命周期场景）。
# 场景：手算环 F=(1,1,1)、earliest 拒绝、无依赖保留全上界、相互要求成环、
#       打开期/翻页期推进（含跨表同笔 upsert+delete）、重启复用、淘汰后本地冻结、
#       第三源失败清理、实例替换、查询中关闭、两视图共享版本只关一个。
set -eu
cd "$(dirname "$0")/.."
WORKDIR="${1:-/tmp/fedq-views-demo}"
echo "工作目录: $WORKDIR"
python3 -m fedq view-demo --workdir "$WORKDIR"
echo
echo "HTTP 请求日志: $WORKDIR/A.http.jsonl B.http.jsonl C.http.jsonl"
echo "查询结果:      $WORKDIR/results/*.jsonl"
echo "视图目录库:    $WORKDIR/coord/views.db（含源身份/版本/字段/每页 HTTP 读取依据）"
