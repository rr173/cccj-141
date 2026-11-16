"""规划器：依据连接器能力决定远端提前做什么，剩余语义本地完成。

核心安全规则（每条下推都记录能力依据与语义前提）：

1. 远端谓词有两种角色：
   - exact（等价过滤）：远端通过集 == 统一语义通过集，无需残余；
   - superset（候选超集过滤）：远端通过集 ⊇ 统一语义通过集，**必须**留下残余条件本地精查。
   整数/bool 的 eq、lt 在 A、B 上都是 exact（null 在 unknown/false 下同样不通过）。
   A 的文本 eq 是 superset（规整后比较，统一为真的行远端必为真）。
   B 的文本 eq 是 exact；B 的文本 lt 按 UTF-8 字节，合法 Unicode 下与码点序相容 → exact。
   A 的文本 lt 基于规整文本，与码点序既非超集也非子集 → 不可下推。

2. 组合规则（不能随便分发只在单原子上安全的近似）：
   - AND：可逐合取项下推；超集项与不可下推项留作残余。
   - OR：必须全部分支可下推才整体下推；任一分支为超集则整体为超集，残余为整个 OR。
   - NOT：对超集近似取反会得到**子集**（可能漏行）→ 禁止；
     B 把 null 比较折叠为 false，NOT 在二值结果上取反，null 行远端为真而统一为 unknown
     → 远端是超集，只能作候选下推并留残余。

3. 聚合/limit 在残余过滤之前执行可能改变答案：
   - 有残余（或非等价过滤）时，聚合与 limit 一律不下推；
   - 绝不在远端先截前 N 行再本地筛选凑 N。
   - sort 本身不增删行，且局部残余过滤保序，故 sort 可在候选过滤之上精确下推
     （前提：排序键对该连接器是统一序）。

4. left 连接右侧的过滤：放在连接前（ON 语义，输入过滤）与连接后（WHERE）语义不同，
   规划器只按查询树原样执行，绝不跨连接移动谓词。

5. 投影列裁剪始终下推（外加行 id：分页去重与行身份需要）。
"""

from __future__ import annotations

from dataclasses import dataclass, field

from . import caps as C
from . import expr as E
from . import query as Q
from . import semantics as S


# ---------------------------------------------------------------- 谓词下推分析

@dataclass
class PA:
    mode: str                 # 'exact' | 'superset' | 'no'
    remote: E.Expr = None     # 发往远端的谓词
    residual: E.Expr = None   # 本地必须保留的精确复核（superset 时非空）
    collapses_null: bool = False  # 远端把 unknown 折叠为 false（B 方言），NOT 下推时关键
    reasons: list = field(default_factory=list)

    @classmethod
    def no(cls, reason):
        return cls("no", reasons=[reason])


def _field_lit(e):
    """识别 field op lit / lit op field；返回 (field, lit, swapped) 或 None。"""
    if isinstance(e.a, E.Field) and isinstance(e.b, E.Lit):
        return e.a, e.b, False
    if isinstance(e.a, E.Lit) and isinstance(e.b, E.Field):
        return e.b, e.a, True
    return None


def analyze_pred(pred, caps):
    """返回 PA。只依据 caps 的公开声明。"""
    if isinstance(pred, (E.Eq, E.Lt)):
        fl = _field_lit(pred)
        if fl is None:
            return PA.no("仅支持 字段-字面量 比较的下推")
        f, lit, _ = fl
        opname = "eq" if isinstance(pred, E.Eq) else "lt"
        if pred.kind not in caps.filter_ops:
            return PA.no(f"源 {caps.source} 未声明支持 {opname}")
        t = f.typ
        if t in (S.T_INT, S.T_BOOL):
            collapse = caps.null_compare == C.NULL_FALSE
            return PA("exact", remote=pred, collapses_null=collapse, reasons=[
                f"{t} {opname}：三方言下 null 均不通过（unknown/false 同样被过滤），值比较一致 → 等价下推"])
        if t == S.T_TEXT:
            if opname == "eq":
                if caps.text_eq == C.EQ_UNIFIED:
                    return PA("exact", remote=pred,
                              collapses_null=caps.null_compare == C.NULL_FALSE,
                              reasons=["文本等值与统一语义相同（码点、区分大小写、不去尾空格）→ 等价下推"])
                if caps.text_eq == C.EQ_FOLD_AZ_RSTRIP:
                    return PA("superset", remote=pred, residual=pred, reasons=[
                        "A 先折 A-Z→a-z、去尾部 ASCII 空格再比较：统一为真的行远端必为真 "
                        "→ 候选超集下推，本地保留精确残余"])
            else:  # lt on text
                if caps.text_order in (C.ORD_UNIFIED, C.ORD_UTF8_BYTES):
                    return PA("exact", remote=pred,
                              collapses_null=caps.null_compare == C.NULL_FALSE,
                              reasons=["文本 lt 按 UTF-8 字节；合法 Unicode 下字节序与码点序相容 "
                                       "（前提：文本均为合法 Unicode）→ 等价下推"])
                return PA.no("A 的文本序基于规整后文本，与码点序既非超集也非子集 → 不下推，本地执行")
        if t == S.T_NULL:
            return PA.no("null 字面量比较恒为 unknown，不下推")
        return PA.no(f"类型 {t} 不支持 {opname} 下推")

    if isinstance(pred, E.IsNull):
        if "is_null" not in caps.filter_ops:
            return PA.no(f"源 {caps.source} 未声明支持 is_null")
        return PA("exact", remote=pred, reasons=["is_null 与比较规则无关 → 等价下推"])

    if isinstance(pred, E.And):
        if "and" not in caps.filter_ops:
            return PA.no(f"源 {caps.source} 未声明支持 and")
        pas = [analyze_pred(a, caps) for a in pred.args]
        pushed = [p for p in pas if p.mode != "no"]
        if not pushed:
            return PA.no("AND 的所有合取项都不可下推")
        remote = E.And([p.remote for p in pushed]) if len(pushed) > 1 else pushed[0].remote
        residual_parts = []
        for p, a in zip(pas, pred.args):
            if p.mode == "no" or p.mode == "superset":
                residual_parts.append(a)  # 超集项与未下推项都需本地精确复核
        residual = None
        if residual_parts:
            residual = E.And(residual_parts) if len(residual_parts) > 1 else residual_parts[0]
        mode = "superset" if residual is not None else "exact"
        reasons = ["AND 按合取项分别判定；交集的超集仍是超集，残余项本地复核"]
        for p in pas:
            reasons += p.reasons
        return PA(mode, remote=remote, residual=residual,
                  collapses_null=any(p.collapses_null for p in pushed),
                  reasons=reasons)

    if isinstance(pred, E.Or):
        if "or" not in caps.filter_ops:
            return PA.no(f"源 {caps.source} 未声明支持 or")
        pas = [analyze_pred(a, caps) for a in pred.args]
        if any(p.mode == "no" for p in pas):
            return PA.no("OR 存在不可下推的分支 → 整体本地（不能只下推部分分支）")
        remote = E.Or([p.remote for p in pas])
        if any(p.mode == "superset" for p in pas):
            reasons = ["OR 的各分支超集之并仍是超集 → 整体候选下推，残余为整个 OR"]
            for p in pas:
                reasons += p.reasons
            return PA("superset", remote=remote, residual=pred,
                      collapses_null=any(p.collapses_null for p in pas),
                      reasons=reasons)
        return PA("exact", remote=remote,
                  collapses_null=any(p.collapses_null for p in pas),
                  reasons=["OR 全部分支等价 → 整体等价下推"])

    if isinstance(pred, E.Not):
        if "not" not in caps.filter_ops:
            return PA.no(f"源 {caps.source} 未声明支持 not")
        pa = analyze_pred(pred.x, caps)
        if pa.mode == "no":
            return PA.no("NOT 的子式不可下推 → 整体本地")
        if pa.collapses_null:
            return PA("superset", remote=E.Not(pa.remote), residual=pred, reasons=[
                "B 把 null 普通比较折叠为 false，NOT 在二值结果上取反：null 行远端为真、"
                "统一语义为 unknown → 远端是超集，候选下推并保留残余"])
        if pa.mode == "exact":
            return PA("exact", remote=E.Not(pa.remote),
                      reasons=["子式等价且连接器 NOT 忠实于三值逻辑 → 等价下推"])
        return PA.no("对超集近似取 NOT 会得到子集（可能漏行）→ 禁止下推")

    return PA.no(f"未支持的谓词节点: {pred.kind}")


# ---------------------------------------------------------------- 物理计划节点

@dataclass
class RemoteSubplan:
    source: str
    table: str
    project: list
    filter: E.Expr = None
    filter_mode: str = None       # 'exact' | 'superset'
    aggregate: Q.Aggregate = None
    sort: Q.Sort = None
    limit: int = None
    reasons: list = field(default_factory=list)

    def to_json(self):
        node = {"op": "scan", "table": self.table}
        if self.filter is not None:
            node = {"op": "filter", "pred": self.filter.to_json(), "input": node}
        node = {"op": "project", "cols": list(self.project), "input": node}
        if self.aggregate is not None:
            node = {"op": "aggregate", "group_by": list(self.aggregate.group_by),
                    "aggs": [a.to_json() for a in self.aggregate.aggs], "input": node}
        if self.sort is not None:
            node = {"op": "sort", "keys": [k.to_json() for k in self.sort.keys],
                    "input": node}
        if self.limit is not None:
            node = {"op": "limit", "n": self.limit, "input": node}
        return node


@dataclass
class PNode:
    kind: str                     # remote_scan/filter/project/join/aggregate/distinct/sort/limit
    schema: Q.Schema
    notes: list = field(default_factory=list)
    children: list = field(default_factory=list)
    remote: RemoteSubplan = None
    pred: E.Expr = None
    cols: list = None
    jkind: str = None
    lkey: str = None
    rkey: str = None
    right_name_map: dict = None
    group_by: list = None
    aggs: list = None
    keys: list = None
    n: int = None


# ---------------------------------------------------------------- 规划器

class Planner:
    def __init__(self, caps_by_source):
        self.caps = caps_by_source

    # ---- 列需求传播 ----
    def _demand(self, q):
        dem = {}

        def rec(node, needed):
            if isinstance(node, Q.Source):
                need = set(needed)
                if "id" in node.schema.as_dict():
                    need.add("id")  # 行 id 恒带：分页去重与行身份
                dem[id(node)] = need
            elif isinstance(node, Q.Filter):
                rec(node.input, needed | node.pred.fields())
            elif isinstance(node, Q.Project):
                rec(node.input, set(node.cols))
            elif isinstance(node, Q.Join):
                lneed = {n for n in needed if n in node.left.schema.as_dict()}
                rneed = {node.right_name_map[n] for n in needed
                         if n in node.right_name_map}
                rec(node.left, lneed | {node.lkey})
                rec(node.right, rneed | {node.rkey})
            elif isinstance(node, Q.Aggregate):
                rec(node.input, set(node.group_by) |
                    {a.field for a in node.aggs if a.field})
            elif isinstance(node, Q.Sort):
                rec(node.input, needed | {k.field for k in node.keys})
            elif isinstance(node, (Q.Limit, Q.Distinct)):
                rec(node.input, needed)
            else:
                raise Q.QueryError(f"规划器未支持的节点: {node.op}")

        rec(q, set(q.schema.names()))
        return dem

    # ---- 主入口 ----
    def plan(self, q):
        q.validate(self.caps)
        self._demand_map = self._demand(q)
        return self._node(q)

    def _node(self, q):
        chain = self._chain(q)
        if chain is not None:
            return self._plan_chain(*chain)
        if isinstance(q, Q.Source):
            demand = self._demand_map[id(q)]
            sub = RemoteSubplan(source=q.source, table=q.table,
                                project=sorted(demand))
            return PNode("remote_scan", self._remote_schema(q, sub), remote=sub,
                         notes=[f"远端子计划 {q.source}.{q.table}: "
                                f"project{sub.project}（列裁剪下推）"])
        if isinstance(q, Q.Filter):
            child = self._node(q.input)
            return PNode("filter", child.schema, pred=q.pred, children=[child],
                         notes=["残余/本地过滤：按统一语义在本地求值（三值逻辑，只有 true 通过）"])
        if isinstance(q, Q.Project):
            child = self._node(q.input)
            cols = [(c, child.schema.type_of(c)) for c in q.cols]
            return PNode("project", Q.Schema(cols, []), cols=list(q.cols),
                         children=[child], notes=["本地投影"])
        if isinstance(q, Q.Join):
            left = self._node(q.left)
            right = self._node(q.right)
            # 物理输出列：左侧实际列 + 右侧实际列按逻辑改名规则映射（与校验时一致）
            orig2out = {orig: out for out, orig in q.right_name_map.items()}
            out_cols = list(left.schema.cols)
            phys_rmap = {}
            for n, t in right.schema.cols:
                out_cols.append((orig2out[n], t))
                phys_rmap[orig2out[n]] = n
            note = ("inner 等值连接（本地执行，键为 null 永不匹配）"
                    if q.kind == "inner" else
                    "left 等值连接（本地执行）：右侧过滤的位置（连接前/后）按查询树原样保持，"
                    "不匹配时补 null 行")
            return PNode("join", Q.Schema(out_cols, []), jkind=q.kind, lkey=q.lkey,
                         rkey=q.rkey, right_name_map=phys_rmap,
                         children=[left, right], notes=[note])
        if isinstance(q, Q.Aggregate):
            child = self._node(q.input)
            cols = [(g, child.schema.type_of(g)) for g in q.group_by]
            cols += [(a.out, S.T_INT) for a in q.aggs]
            return PNode("aggregate", Q.Schema(cols, []), group_by=list(q.group_by),
                         aggs=list(q.aggs), children=[child],
                         notes=["本地分组聚合：sum 用 Python 整数精确计算，全 null 为 null；"
                                "count 空集为 0；count(字段) 跳过 null"])
        if isinstance(q, Q.Distinct):
            child = self._node(q.input)
            return PNode("distinct", child.schema, children=[child],
                         notes=["本地 distinct：统一语义（区分大小写、不去尾空格）"])
        if isinstance(q, Q.Sort):
            child = self._node(q.input)
            return PNode("sort", child.schema, keys=list(q.keys), children=[child],
                         notes=["本地全序排序（含唯一决胜字段，null 顺序显式）"])
        if isinstance(q, Q.Limit):
            child = self._node(q.input)
            return PNode("limit", child.schema, n=q.n, children=[child],
                         notes=["本地 limit：绝不在残余过滤之前截断远端结果"])
        raise Q.QueryError(f"规划器未支持的节点: {q.op}")

    @staticmethod
    def _chain(q):
        """识别 单源线性链：Source 之上仅 Filter/Project/Aggregate/Sort/Limit。"""
        ops = []
        cur = q
        while isinstance(cur, (Q.Filter, Q.Project, Q.Aggregate, Q.Sort, Q.Limit)):
            ops.append(cur)
            cur = cur.input
        if isinstance(cur, Q.Source) and ops:
            return cur, ops
        return None

    def _plan_chain(self, source, ops):
        caps = self.caps[source.source]
        demand = self._demand_map[id(source)]
        filters = [o for o in ops if isinstance(o, Q.Filter)]
        aggregate = next((o for o in ops if isinstance(o, Q.Aggregate)), None)
        sort = next((o for o in ops if isinstance(o, Q.Sort)), None)
        limit = next((o for o in ops if isinstance(o, Q.Limit)), None)
        top_schema = ops[0].schema

        reasons, residuals, remote_preds = [], [], []
        filter_mode = None
        for f in filters:
            pa = analyze_pred(f.pred, caps)
            reasons += [f"[{caps.source}] {r}" for r in pa.reasons]
            if pa.mode != "no":
                remote_preds.append(pa.remote)
                filter_mode = pa.mode if filter_mode is None else (
                    "superset" if "superset" in (filter_mode, pa.mode) else "exact")
            if pa.residual is not None:
                residuals.append(pa.residual)
            elif pa.mode == "no":
                residuals.append(f.pred)
        has_residual = bool(residuals)
        remote_filter = None
        if remote_preds:
            remote_filter = E.And(remote_preds) if len(remote_preds) > 1 else remote_preds[0]

        # 聚合下推：仅当过滤全部等价（无残余）且连接器声明了相应聚合能力
        remote_agg = None
        agg_local_reason = None
        if aggregate is not None:
            need = {"count_star": "count", "count": "count", "sum": "sum_int"}
            missing = [need[a.f] for a in aggregate.aggs if need[a.f] not in caps.aggregates]
            if has_residual:
                agg_local_reason = ("存在残余过滤：聚合若先于残余执行会改变答案 → 聚合留在本地")
            elif missing:
                agg_local_reason = (f"连接器未声明聚合能力 {sorted(set(missing))} → "
                                    f"本地聚合（能力不足的受支持查询仍正确执行）")
            else:
                remote_agg = aggregate
                reasons.append(f"[{caps.source}] 源内聚合 count/sum_int 可独立完成"
                               f"（过滤等价、无连接）→ 真实下推；null 规则与统一一致")

        # 排序下推：键对该连接器必须是统一序；下方聚合若留本地则排序也留本地
        remote_sort = None
        sort_local_reason = None
        if sort is not None:
            if not caps.sort:
                sort_local_reason = "连接器未声明排序能力 → 本地排序"
            elif aggregate is not None and remote_agg is None:
                sort_local_reason = "聚合在本地执行 → 其上的 sort 一并本地"
            else:
                bad = [k.field for k in sort.keys
                       if sort.input.schema.type_of(k.field) == S.T_TEXT
                       and caps.text_order not in (C.ORD_UNIFIED, C.ORD_UTF8_BYTES)]
                if bad:
                    sort_local_reason = (f"文本排序键 {bad} 在源 {caps.source} 上不是统一序"
                                         f"（{caps.text_order}）→ 本地排序")
                else:
                    remote_sort = sort
                    reasons.append(f"[{caps.source}] 排序键为统一序；sort 不增删行、"
                                   f"本地残余过滤保序 → 排序可精确下推")

        # limit 下推：仅当下方一切等价（无残余、无本地聚合）且 sort 已下推
        remote_limit = None
        limit_local_reason = None
        if limit is not None:
            if has_residual:
                limit_local_reason = ("存在残余过滤：在远端先截前 N 行再本地筛选会漏行 "
                                      "→ limit 留在本地，由本地在全量候选上取正确后继")
            elif aggregate is not None and remote_agg is None:
                limit_local_reason = "聚合在本地 → limit 本地"
            elif remote_sort is None:
                limit_local_reason = "sort 未下推 → limit 本地（需要全序保证）"
            elif not caps.limit:
                limit_local_reason = "连接器未声明 limit 能力 → 本地 limit"
            else:
                remote_limit = limit.n
                reasons.append(f"[{caps.source}] 下方全部等价下推 → limit 可安全下推")

        # 远端投影列
        if remote_agg is not None:
            project = list(remote_agg.group_by) + [a.field for a in remote_agg.aggs
                                                   if a.field]
        else:
            project = sorted(demand)

        sub = RemoteSubplan(source=source.source, table=source.table,
                            project=project, filter=remote_filter,
                            filter_mode=filter_mode, aggregate=remote_agg,
                            sort=remote_sort, limit=remote_limit, reasons=reasons)
        scan_schema = self._remote_schema(source, sub)
        scan = PNode("remote_scan", scan_schema, remote=sub, notes=[
            f"远端子计划 {source.source}.{source.table}: " +
            (", ".join(self._remote_ops_desc(sub)) or "仅扫描")])

        node = scan
        if has_residual:
            pred = E.And(residuals) if len(residuals) > 1 else residuals[0]
            node = PNode("filter", scan.schema, pred=pred, children=[node],
                         notes=["残余过滤：远端为候选超集/未下推部分，本地按统一语义精确复核"])
        if aggregate is not None and remote_agg is None:
            node = PNode("aggregate", aggregate.schema, group_by=list(aggregate.group_by),
                         aggs=list(aggregate.aggs), children=[node],
                         notes=[agg_local_reason] if agg_local_reason else ["本地聚合"])
        if sort is not None and remote_sort is None:
            node = PNode("sort", node.schema, keys=list(sort.keys), children=[node],
                         notes=[sort_local_reason] if sort_local_reason else ["本地排序"])
        if limit is not None and remote_limit is None:
            node = PNode("limit", node.schema, n=limit.n, children=[node],
                         notes=[limit_local_reason] if limit_local_reason else ["本地 limit"])
        # 链顶输出列整形：非聚合链只保留需求列（远端已裁剪）；聚合链输出即聚合列
        if aggregate is not None:
            target = top_schema.names()
        else:
            target = [c for c in top_schema.names() if c in demand]
        if node.schema.names() != target:
            cols = [(c, node.schema.type_of(c)) for c in target]
            node = PNode("project", Q.Schema(cols, []), cols=target,
                         children=[node], notes=["输出列整形"])
        return node

    @staticmethod
    def _remote_ops_desc(sub):
        ops = []
        if sub.filter is not None:
            ops.append(f"filter[{sub.filter_mode}]({E.render(sub.filter)})")
        ops.append(f"project{sub.project}")
        if sub.aggregate is not None:
            ops.append("aggregate")
        if sub.sort is not None:
            ops.append("sort")
        if sub.limit is not None:
            ops.append(f"limit {sub.limit}")
        return ops

    @staticmethod
    def _remote_schema(source, sub):
        caps_table = source.schema.as_dict()
        if sub.aggregate is not None:
            cols = [(g, caps_table[g]) for g in sub.aggregate.group_by]
            cols += [(a.out, S.T_INT) for a in sub.aggregate.aggs]
            uniq = [frozenset(sub.aggregate.group_by)] if sub.aggregate.group_by else [frozenset()]
            return Q.Schema(cols, uniq)
        cols = [(c, caps_table[c]) for c in sub.project]
        uniq = [u for u in source.schema.unique if u.issubset(set(sub.project))]
        return Q.Schema(cols, uniq)


# ---------------------------------------------------------------- 计划展示

def render_plan(node, indent=0):
    pad = "  " * indent
    head = {
        "remote_scan": "RemoteScan",
        "filter": "Filter",
        "project": "Project",
        "join": "Join",
        "aggregate": "Aggregate",
        "distinct": "Distinct",
        "sort": "Sort",
        "limit": "Limit",
    }[node.kind]
    detail = ""
    if node.kind == "remote_scan":
        detail = f" {node.remote.source}.{node.remote.table}"
    elif node.kind == "filter":
        detail = f" {E.render(node.pred)}"
    elif node.kind == "project":
        detail = f" {node.cols}"
    elif node.kind == "join":
        detail = f" {node.jkind} ON {node.lkey} = {node.rkey}"
    elif node.kind == "aggregate":
        aggs = ", ".join(f"{a.f}({a.field or '*'}) AS {a.out}" for a in node.aggs)
        detail = f" GROUP BY {node.group_by} {aggs}"
    elif node.kind == "sort":
        detail = " " + ", ".join(
            f"{k.field} {'ASC' if k.asc else 'DESC'} "
            f"{'NULLS FIRST' if k.nulls_first else 'NULLS LAST'}" for k in node.keys)
    elif node.kind == "limit":
        detail = f" {node.n}"
    lines = [f"{pad}{head}{detail}"]
    for n in node.notes:
        lines.append(f"{pad}  · {n}")
    if node.kind == "remote_scan":
        for r in node.remote.reasons:
            lines.append(f"{pad}  · 依据: {r}")
    for c in node.children:
        lines.append(render_plan(c, indent + 1))
    return "\n".join(lines)
