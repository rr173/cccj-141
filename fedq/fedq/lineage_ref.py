"""独立小样本血缘参照：纯 Python 枚举输入出现并自行推导。

与生产血缘执行器（fedq/lineage.py）**不共享任何求值与血缘代码**：
自己的三值逻辑、自己的算子、自己的出现图。只跑小数据，用于对照
生产 run 的结果、逐层展开与反查。

模型与生产一致（这是规格，不是实现共享）：
- 袋语义保留重数：同值不同出现各自独立；
- 推导三类：pass（直通）/ join（左右共同成立，右可为 null_fill）/
  distinct（备选列表）/ agg（成员逐份计入，带贡献）；
- 子推导按对象共享（DAG）：distinct 的备选、连接共享的左输入都是同一
  对象被多处引用，从不枚举备选分支的组合。
"""

from __future__ import annotations

from functools import cmp_to_key

UNKNOWN = object()          # 独立三值逻辑标记


class RefError(Exception):
    pass


# ---------------- 三值逻辑（独立实现） ----------------

def _typeof(v):
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "bool"
    if isinstance(v, int):
        return "int"
    if isinstance(v, str):
        return "text"
    raise RefError(f"bad value {v!r}")


def _eq(a, b):
    if a is None or b is None:
        return UNKNOWN
    if _typeof(a) != _typeof(b):
        raise RefError("eq 类型不一致")
    return a == b


def _lt(a, b):
    if a is None or b is None:
        return UNKNOWN
    ta, tb = _typeof(a), _typeof(b)
    if ta != tb or ta == "bool":
        raise RefError("lt 类型错误")
    return a < b


def _not(v):
    return UNKNOWN if v is UNKNOWN else (not v)


def _and(a, b):
    if a is False or b is False:
        return False
    if a is UNKNOWN or b is UNKNOWN:
        return UNKNOWN
    return True


def _or(a, b):
    if a is True or b is True:
        return True
    if a is UNKNOWN or b is UNKNOWN:
        return UNKNOWN
    return False


def _val(p, row):
    k = p["k"]
    if k == "field":
        return row[p["name"]]
    if k == "lit":
        v = p["v"]
        return int(v) if p["t"] == "int" and v is not None else v
    if k == "eq":
        return _eq(_val(p["a"], row), _val(p["b"], row))
    if k == "lt":
        return _lt(_val(p["a"], row), _val(p["b"], row))
    if k == "is_null":
        return _val(p["x"], row) is None
    if k == "and":
        acc = True
        for x in p["args"]:
            acc = _and(acc, _val(x, row))
        return acc
    if k == "or":
        acc = False
        for x in p["args"]:
            acc = _or(acc, _val(x, row))
        return acc
    if k == "not":
        return _not(_val(p["x"], row))
    raise RefError(f"未知谓词 {k}")


# ---------------- 出现与推导 ----------------

class Occ:
    """一个出现。deriv 形状：
    ("scan", src)                 src=(source, instance, table, at, pk_tuple, path)
    ("pass", occ)                 filter/project/sort/limit 的唯一输入
    ("join", left, right|None)    right=None 表示 left 连接补 null（null_fill）
    ("distinct", [alt occs])      备选推导列表
    ("agg", [(occ, contrib)])     成员列表；contrib={out: ("counted",)|
    ("sum", 精确整数值)|("null_skipped",)}
    """
    __slots__ = ("op", "path", "values", "deriv")

    def __init__(self, op, path, values, deriv):
        self.op = op
        self.path = path
        self.values = values          # tuple，业务值（含重数语义）
        self.deriv = deriv


def execute(qjson, data, meta):
    """返回根出现列表（按输出顺序）。data: {source: {table: {"columns","key","rows"}}}；
    meta: {source: {"instance_id","at"}}。"""
    _, roots = _eval(qjson, "", data, meta)
    return roots


def _eval(node, path, data, meta):
    op = node["op"]
    if op == "source":
        t = data[node["source"]][node["table"]]
        cols = list(t["columns"].keys())
        key = t.get("key", ["id"])
        m = meta[node["source"]]
        occs = []
        for r in t["rows"]:
            src = (node["source"], m["instance_id"], node["table"], m["at"],
                   tuple(r[cols.index(k)] for k in key), path)
            occs.append(Occ("source", path, tuple(r), ("scan", src)))
        return cols, occs
    if op == "filter":
        cols, occs = _eval(node["input"], path + "/i", data, meta)
        out = [Occ("filter", path, o.values, ("pass", o)) for o in occs
               if _val(node["pred"], dict(zip(cols, o.values))) is True]
        return cols, out
    if op == "project":
        cols, occs = _eval(node["input"], path + "/i", data, meta)
        idx = [cols.index(c) for c in node["cols"]]
        return list(node["cols"]), [Occ("project", path,
                                        tuple(o.values[i] for i in idx),
                                        ("pass", o)) for o in occs]
    if op == "join":
        lcols, lo = _eval(node["left"], path + "/l", data, meta)
        rcols, ro = _eval(node["right"], path + "/r", data, meta)
        li = lcols.index(node["lkey"])
        ri = rcols.index(node["rkey"])
        used = set(lcols)
        out_cols = list(lcols)
        for c in rcols:
            mm = c
            while mm in used:
                mm = "r_" + mm
            used.add(mm)
            out_cols.append(mm)
        out = []
        for l in lo:
            hit = False
            for r in ro:
                lv, rv = l.values[li], r.values[ri]
                if lv is not None and rv is not None and lv == rv:
                    hit = True
                    out.append(Occ("join", path, l.values + r.values,
                                   ("join", l, r)))
            if node["kind"] == "left" and not hit:
                out.append(Occ("join", path,
                               l.values + (None,) * len(rcols),
                               ("join", l, None)))
        return out_cols, out
    if op == "distinct":
        cols, occs = _eval(node["input"], path + "/i", data, meta)
        seen, order = {}, []
        for o in occs:
            if o.values in seen:
                seen[o.values].append(o)
            else:
                seen[o.values] = [o]
                order.append(o.values)
        return cols, [Occ("distinct", path, v, ("distinct", seen[v]))
                      for v in order]
    if op == "aggregate":
        cols, occs = _eval(node["input"], path + "/i", data, meta)
        gidx = [cols.index(g) for g in node["group_by"]]
        aggs = node["aggs"]
        groups, order = {}, []
        for o in occs:
            key = tuple(o.values[i] for i in gidx)
            if key not in groups:
                groups[key] = []
                order.append(key)
            contrib = {}
            for a in aggs:
                f = a["f"]
                if f == "count_star":
                    contrib[a["out"]] = ("counted",)
                else:
                    v = o.values[cols.index(a["field"])]
                    if v is None:
                        contrib[a["out"]] = ("null_skipped",)
                    elif f == "sum":
                        contrib[a["out"]] = ("sum", v)
                    else:
                        contrib[a["out"]] = ("counted",)
            groups[key].append((o, contrib))
        if not node["group_by"] and not order:
            order.append(())
            groups[()] = []
        out_cols = list(node["group_by"]) + [a["out"] for a in aggs]
        out = []
        for key in order:
            members = groups[key]
            vals = list(key)
            for a in aggs:
                f = a["f"]
                if f == "count_star":
                    vals.append(len(members))
                elif f == "count":
                    vals.append(sum(1 for _, c in members
                                    if c[a["out"]][0] == "counted"))
                else:
                    parts = [c[a["out"]][1] for _, c in members
                             if c[a["out"]][0] == "sum"]
                    vals.append(sum(parts) if parts else None)
            out.append(Occ("aggregate", path, tuple(vals),
                           ("agg", members)))
        return out_cols, out
    if op == "sort":
        cols, occs = _eval(node["input"], path + "/i", data, meta)
        keys = [(cols.index(k["field"]), k["asc"], k["nulls_first"])
                for k in node["keys"]]

        def cmp(a, b):
            for i, asc, nf in keys:
                x, y = a.values[i], b.values[i]
                if x is None and y is None:
                    continue
                if x is None:
                    c = -1 if nf else 1
                elif y is None:
                    c = 1 if nf else -1
                else:
                    c = (x > y) - (y > x)
                if c:
                    return c if asc else -c
            return 0

        return cols, [Occ("sort", path, o.values, ("pass", o))
                      for o in sorted(occs, key=cmp_to_key(cmp))]
    if op == "limit":
        cols, occs = _eval(node["input"], path + "/i", data, meta)
        return cols, [Occ("limit", path, o.values, ("pass", o))
                      for o in occs[: node["n"]]]
    raise RefError(f"未知算子 {op}")


# ---------------- 反查（独立推导） ----------------

def _srcs_of(occ, memo):
    """返回该出现可达的源行身份集合（DAG 共享按 id 备忘，不展开组合）。"""
    key = id(occ)
    if key in memo:
        return memo[key]
    d = occ.deriv
    k = d[0]
    if k == "scan":
        s = {d[1]}
    elif k == "pass":
        s = set(_srcs_of(d[1], memo))
    elif k == "join":
        s = set(_srcs_of(d[1], memo))
        if d[2] is not None:
            s |= _srcs_of(d[2], memo)
    elif k == "distinct":
        s = set()
        for a in d[1]:
            s |= _srcs_of(a, memo)
    elif k == "agg":
        s = set()
        for m, _ in d[1]:
            s |= _srcs_of(m, memo)
    else:
        raise RefError(f"未知推导 {k}")
    memo[key] = s
    return s


def srcs_of(occ):
    return set(_srcs_of(occ, {}))


def used_by(roots, source, table, pk, path=None):
    """返回使用了该源行的根出现下标集合。pk 为值元组；path 可限定输入位置。"""
    out = set()
    memo = {}
    for i, r in enumerate(roots):
        for src in _srcs_of(r, memo):
            s_source, _inst, s_table, _at, s_pk, s_path = src
            if s_source == source and s_table == table and s_pk == tuple(pk) \
                    and (path is None or s_path == path):
                out.add(i)
                break
    return out
