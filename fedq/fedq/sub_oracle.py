"""持续查询订阅的**独立小样本核对器**。

它不调用 fedq.subscription / sub_service 的任何增量维护代码，而是：
1. 自己从提交历史重放每个位置的完整表状态（纯 Python dict）；
2. 调用既有的独立参照解释器 fedq.reference（与执行器/增量引擎都不共享求值代码）
   在完整表上跑原查询，得到该位置的完整袋结果；
3. 相邻位置的袋按 Counter 相减得到净变化（新重数-旧重数，0 不输出）。

因此它给出的是“从完整表重新计算的预期袋与差分”，只用于小数据演示与测试对照，
绝不作为生产答案的来源。
"""

from __future__ import annotations

from collections import Counter

from . import reference as REF
from .versions import history_from_config


class IndependentOracle:
    def __init__(self, history_config):
        # 独立重放：直接用历史事实层重建表，不读订阅的任何本地状态
        self.hist = history_from_config(history_config)
        self.columns = {
            t: dict(sch["columns"]) for t, sch in self.hist.schemas.items()}

    def table_state(self, at):
        """位置 at 结束后的完整表状态（独立重放，含同源多表整体生效）。

        返回 {源名: {表名: {columns, rows}}}，与参照解释器的 sources 入参同构。
        """
        state = self.hist.state_at(at)
        tables = {t: {
            "columns": self.columns[t],
            "rows": [list(v) for v in rows.values()]}
            for t, rows in state.items()}
        return {self.hist.name: tables}

    def expected_bag(self, query_json, at):
        """在 at 的完整表上跑参照解释器，返回 Counter(业务行元组 -> 重数)。"""
        cols, rows = REF.run(query_json, self.table_state(at))
        return cols, REF.as_multiset(rows)

    def expected_diff(self, query_json, at_prev, at_new):
        """两位置完整袋之差：Counter 相减，丢弃净变化为 0 的业务行。"""
        cols, old = self.expected_bag(query_json, at_prev)
        _, new = self.expected_bag(query_json, at_new)
        diff = Counter()
        for row, m in new.items():
            diff[row] += m
        for row, m in old.items():
            diff[row] -= m
        return cols, {row: d for row, d in diff.items() if d != 0}

    @staticmethod
    def as_signed_items(diff):
        """净变化拆成 (业务行, +1/-1) 的逐项列表，模拟下游逐笔取增删量。"""
        items = []
        for row, d in sorted(diff.items()):
            sign = 1 if d > 0 else -1
            for _ in range(abs(d)):
                items.append((list(row), sign))
        return items
