#!/bin/sh
# 持续查询订阅：一键端到端验收（内嵌一个版本化源，跑全部生命周期场景）。
# 场景：left 连接逐笔增删与聚合净变化、distinct 支持撤回、同笔替换只交付净差、
#       自连接/键搬家/null/抵消/分组消失/空集、分页稳定、登记拒绝、停止隔离、
#       断页 HTTP 失败不推进、真终止进程重启、历史断档、实例换脸、规模键访问诊断。
set -eu
cd "$(dirname "$0")/.."
WORKDIR="${1:-/tmp/fedq-sub-demo}"
SCALE="${2:-2000}"
echo "工作目录: $WORKDIR  规模键数: $SCALE"
python3 -m fedq sub-demo --workdir "$WORKDIR" --scale-keys "$SCALE"
echo
echo "HTTP 请求日志: $WORKDIR/OD.http.jsonl（snapshot/* 仅初始化，推进只走 /changes/*）"
echo "手写核对台账: $WORKDIR/results/hand.json（独立核对器逐笔完整表重算，不用增量代码）"
echo "订阅本地库:   $WORKDIR/subsvc/subscriptions.db 与 subscriptions/*.state.db"
