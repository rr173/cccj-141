"""有类型的查询树（第一版不接受任意 SQL 文本、任意函数、相关子查询）。

节点：Source / Filter / Project / Join(inner|left 等值) / Aggregate(分组 count、整数 sum)
     / Distinct / Sort(全序，必须含唯一决胜字段，null 顺序显式) / Limit(只能作用于 Sort)。

validate() 自底向上解析每个节点的输出 Schema（列+类型+唯一集合），并做全部静态检查；
执行与计划只使用通过校验的树。
"""

from __future__ import annotations

from dataclasses import dataclass, field as _dc_field

from . import expr as E
from . import semantics as S


class QueryError(Exception):
    pass


@dataclass
class Schema:
    cols: list            # [(name, 'int'|'text'|'bool'), ...]
    unique: list          # [frozenset(names), ...] 每个集合都保证唯一

    def type_of(self, name):
        for n, t in self.cols:
            if n == name:
                return t
        raise QueryError(f"未知列: {name}")

    def names(self):
        return [n for n, _ in self.cols]

    def as_dict(self):
        return dict(self.cols)


# ---------------------------------------------------------------- 节点

class Node:
    op = "?"
    schema: Schema = None

    def inputs(self):
        return []

    def validate(self, caps_by_source):
        raise NotImplementedError

    def to_json(self):
        raise NotImplementedError


@dataclass
class Source(Node):
    source: str
    table: str
    op: str = _dc_field(default="source", init=False)

    def validate(self, caps_by_source):
        if self.source not in caps_by_source:
            raise QueryError(f"未知数据源: {self.source}")
        caps = caps_by_source[self.source]
        try:
            ts = caps.table(self.table)
        except KeyError as e:
            raise QueryError(str(e))
        self.schema = Schema(list(ts.columns.items()), [frozenset(ts.key)])
        return self.schema

    def to_json(self):
        return {"op": "source", "source": self.source, "table": self.table}


@dataclass
class Filter(Node):
    pred: E.Expr
    input: Node
    op: str = _dc_field(default="filter", init=False)

    def inputs(self):
        return [self.input]

    def validate(self, caps_by_source):
        sch = self.input.validate(caps_by_source)
        if isinstance(self.input, Aggregate):
            raise QueryError("不支持对聚合结果再过滤（HAVING 不属于第一版）")
        self.pred.typecheck(sch.as_dict())
        self.schema = sch
        return sch

    def to_json(self):
        return {"op": "filter", "pred": self.pred.to_json(), "input": self.input.to_json()}


@dataclass
class Project(Node):
    cols: list
    input: Node
    op: str = _dc_field(default="project", init=False)

    def inputs(self):
        return [self.input]

    def validate(self, caps_by_source):
        sch = self.input.validate(caps_by_source)
        for c in self.cols:
            sch.type_of(c)
        keep = [frozenset(u) for u in sch.unique if u.issubset(set(self.cols))]
        self.schema = Schema([(c, sch.type_of(c)) for c in self.cols], keep)
        return self.schema

    def to_json(self):
        return {"op": "project", "cols": list(self.cols), "input": self.input.to_json()}


@dataclass
class Join(Node):
    kind: str          # 'inner' | 'left'
    left: Node
    right: Node
    lkey: str
    rkey: str
    op: str = _dc_field(default="join", init=False)
    right_name_map: dict = _dc_field(default=None, init=False)  # 输出名 -> 右表原名

    def inputs(self):
        return [self.left, self.right]

    def validate(self, caps_by_source):
        if self.kind not in ("inner", "left"):
            raise QueryError(f"不支持的连接类型: {self.kind}（仅 inner/left 等值连接）")
        ls = self.left.validate(caps_by_source)
        rs = self.right.validate(caps_by_source)
        lt = ls.type_of(self.lkey)
        rt = rs.type_of(self.rkey)
        if lt != rt:
            raise QueryError(f"连接键类型不一致: {self.lkey}:{lt} vs {self.rkey}:{rt}")
        # 输出列：左侧保持原名；右侧冲突列自动加 r_ 前缀（规则公开、确定）
        self.right_name_map = {}
        used = set(ls.names())
        out = list(ls.cols)
        for n, t in rs.cols:
            m = n
            while m in used:
                m = "r_" + m
            used.add(m)
            self.right_name_map[m] = n
            out.append((m, t))
        rmap = {orig: out_name for out_name, orig in self.right_name_map.items()}
        uniq = []
        for lu in ls.unique:
            for ru in rs.unique:
                uniq.append(frozenset(set(lu) | {rmap[r] for r in ru}))
        self.schema = Schema(out, uniq)
        return self.schema

    def to_json(self):
        return {"op": "join", "kind": self.kind, "left": self.left.to_json(),
                "right": self.right.to_json(), "lkey": self.lkey, "rkey": self.rkey}


@dataclass
class Agg:
    f: str             # 'count_star' | 'count' | 'sum'
    out: str
    field: str = None

    def to_json(self):
        d = {"f": self.f, "out": self.out}
        if self.field is not None:
            d["field"] = self.field
        return d


@dataclass
class Aggregate(Node):
    group_by: list
    aggs: list         # [Agg]
    input: Node
    op: str = _dc_field(default="aggregate", init=False)

    def inputs(self):
        return [self.input]

    def validate(self, caps_by_source):
        sch = self.input.validate(caps_by_source)
        if isinstance(self.input, Aggregate):
            raise QueryError("不支持嵌套聚合（第一版）")
        for g in self.group_by:
            sch.type_of(g)
        seen = set()
        for a in self.aggs:
            if a.f == "count_star":
                pass
            elif a.f == "count":
                sch.type_of(a.field)
            elif a.f == "sum":
                if sch.type_of(a.field) != S.T_INT:
                    raise QueryError(f"sum 只支持整数字段: {a.field}")
            else:
                raise QueryError(f"未知聚合函数: {a.f}（仅 count(*)/count(字段)/sum(整数字段)）")
            if a.out in seen or a.out in self.group_by:
                raise QueryError(f"聚合输出列名冲突: {a.out}")
            seen.add(a.out)
        cols = [(g, sch.type_of(g)) for g in self.group_by] + [(a.out, S.T_INT) for a in self.aggs]
        uniq = [frozenset(self.group_by)] if self.group_by else [frozenset()]
        self.schema = Schema(cols, uniq)
        return self.schema

    def to_json(self):
        return {"op": "aggregate", "group_by": list(self.group_by),
                "aggs": [a.to_json() for a in self.aggs], "input": self.input.to_json()}


@dataclass
class Distinct(Node):
    input: Node
    op: str = _dc_field(default="distinct", init=False)

    def inputs(self):
        return [self.input]

    def validate(self, caps_by_source):
        sch = self.input.validate(caps_by_source)
        self.schema = Schema(list(sch.cols), [frozenset(sch.names())])
        return self.schema

    def to_json(self):
        return {"op": "distinct", "input": self.input.to_json()}


@dataclass
class SortKey:
    field: str
    asc: bool
    nulls_first: bool  # null 顺序必须显式指定

    def to_json(self):
        return {"field": self.field, "asc": self.asc, "nulls_first": self.nulls_first}


@dataclass
class Sort(Node):
    keys: list         # [SortKey]，必须构成全序（含唯一决胜字段）
    input: Node
    op: str = _dc_field(default="sort", init=False)

    def inputs(self):
        return [self.input]

    def validate(self, caps_by_source):
        sch = self.input.validate(caps_by_source)
        if isinstance(self.input, Sort):
            raise QueryError("不支持连续两级 sort（请合并为一个全序）")
        if not self.keys:
            raise QueryError("sort 至少需要一个键")
        key_fields = set()
        for k in self.keys:
            t = sch.type_of(k.field)
            if t == S.T_BOOL:
                raise QueryError(f"sort 不支持 bool 字段: {k.field}")
            key_fields.add(k.field)
        if not any(u.issubset(key_fields) for u in sch.unique):
            raise QueryError("order 必须由用户给出唯一决胜字段（如 id），使排序成为全序")
        self.schema = sch
        return sch

    def to_json(self):
        return {"op": "sort", "keys": [k.to_json() for k in self.keys],
                "input": self.input.to_json()}


@dataclass
class Limit(Node):
    n: int
    input: Node
    op: str = _dc_field(default="limit", init=False)

    def inputs(self):
        return [self.input]

    def validate(self, caps_by_source):
        if not isinstance(self.input, Sort):
            raise QueryError("limit 只能作用于全序 sort 之上（否则结果不确定）")
        if not isinstance(self.n, int) or isinstance(self.n, bool) or self.n < 0:
            raise QueryError("limit 必须是非负整数")
        self.schema = self.input.validate(caps_by_source)
        return self.schema

    def to_json(self):
        return {"op": "limit", "n": self.n, "input": self.input.to_json()}


# ---------------------------------------------------------------- JSON

def from_json(d):
    op = d["op"]
    if op == "source":
        return Source(d["source"], d["table"])
    if op == "filter":
        return Filter(E.from_json(d["pred"]), from_json(d["input"]))
    if op == "project":
        return Project(list(d["cols"]), from_json(d["input"]))
    if op == "join":
        return Join(d["kind"], from_json(d["left"]), from_json(d["right"]),
                    d["lkey"], d["rkey"])
    if op == "aggregate":
        aggs = [Agg(a["f"], a["out"], a.get("field")) for a in d["aggs"]]
        return Aggregate(list(d["group_by"]), aggs, from_json(d["input"]))
    if op == "distinct":
        return Distinct(from_json(d["input"]))
    if op == "sort":
        keys = [SortKey(k["field"], bool(k["asc"]), bool(k["nulls_first"]))
                for k in d["keys"]]
        return Sort(keys, from_json(d["input"]))
    if op == "limit":
        return Limit(int(d["n"]), from_json(d["input"]))
    raise QueryError(f"未知查询节点 op: {op!r}（第一版不接受任意 SQL/函数/相关子查询）")
