"""独立的纯 Python 关系解释器（参照实现，只跑小数据）。

与 executor 不共享任何求值代码：自己的三值逻辑、自己的表达式求值、袋语义（保留重数）。
用于对照完整结果。输入为查询树的 JSON 字典与原始数据（内存表）。
"""

from __future__ import annotations

from functools import cmp_to_key

UNKNOWN = "<?>"
INT64_MIN, INT64_MAX = -(2 ** 63), 2 ** 63 - 1


class RefError(Exception):
    pass


# ---------------- 三值逻辑（独立实现） ----------------

def _not(v):
    return UNKNOWN if v == UNKNOWN else (not v)


def _and(a, b):
    if a is False or b is False:
        return False
    if a == UNKNOWN or b == UNKNOWN:
        return UNKNOWN
    return True


def _or(a, b):
    if a is True or b is True:
        return True
    if a == UNKNOWN or b == UNKNOWN:
        return UNKNOWN
    return False


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
    ta, tb = _typeof(a), _typeof(b)
    if ta != tb:
        raise RefError(f"eq 类型不一致 {ta}/{tb}")
    return a == b


def _lt(a, b):
    if a is None or b is None:
        return UNKNOWN
    ta, tb = _typeof(a), _typeof(b)
    if ta != tb or ta == "bool":
        raise RefError(f"lt 类型错误 {ta}/{tb}")
    return a < b  # str 比较即码点序，区分大小写、不去尾空格


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


# ---------------- 关系算子（袋语义） ----------------

def run(plan, sources):
    """返回 (columns, rows)；rows 为 list[list]，保留袋重数。"""
    op = plan["op"]
    if op == "source":
        t = sources[plan["source"]][plan["table"]]
        cols = list(t["columns"].keys())
        return cols, [list(r) for r in t["rows"]]
    if op == "filter":
        cols, rows = run(plan["input"], sources)
        return cols, [r for r in rows
                      if _val(plan["pred"], dict(zip(cols, r))) is True]
    if op == "project":
        cols, rows = run(plan["input"], sources)
        idx = [cols.index(c) for c in plan["cols"]]
        return plan["cols"], [[r[i] for i in idx] for r in rows]
    if op == "join":
        lcols, lrows = run(plan["left"], sources)
        rcols, rrows = run(plan["right"], sources)
        li = lcols.index(plan["lkey"])
        ri = rcols.index(plan["rkey"])
        used = set(lcols)
        rmap = {}
        out = list(lcols)
        for c in rcols:
            m = c
            while m in used:
                m = "r_" + m
            used.add(m)
            rmap[c] = m
            out.append(m)
        rows = []
        matched_right = set()
        for l in lrows:
            hit = False
            for j, r in enumerate(rrows):
                if l[li] is not None and r[ri] is not None and l[li] == r[ri]:
                    hit = True
                    matched_right.add(j)
                    rows.append(list(l) + list(r))
            if plan["kind"] == "left" and not hit:
                rows.append(list(l) + [None] * len(rcols))
        return out, rows
    if op == "aggregate":
        cols, rows = run(plan["input"], sources)
        gidx = [cols.index(g) for g in plan["group_by"]]
        aggs = plan["aggs"]
        groups, order = {}, []
        for r in rows:
            key = tuple(r[i] for i in gidx)
            if key not in groups:
                groups[key] = [0, {a["out"]: [0, False] for a in aggs}]
                order.append(key)
            g = groups[key]
            g[0] += 1
            for a in aggs:
                st = g[1][a["out"]]
                if a["f"] == "count_star":
                    st[0] += 1
                else:
                    v = r[cols.index(a["field"])]
                    if v is not None:
                        st[0] += v if a["f"] == "sum" else 1
                        st[1] = True
        if not plan["group_by"] and not order:
            order.append(())
            groups[()] = [0, {a["out"]: [0, False] for a in aggs}]
        out = list(plan["group_by"]) + [a["out"] for a in aggs]
        result = []
        for key in order:
            g = groups[key]
            row = list(key)
            for a in aggs:
                st = g[1][a["out"]]
                if a["f"] == "sum":
                    row.append(st[0] if st[1] else None)
                else:
                    row.append(st[0])
            result.append(row)
        return out, result
    if op == "distinct":
        cols, rows = run(plan["input"], sources)
        seen, out_rows = set(), []
        for r in rows:
            key = tuple(r)
            if key not in seen:
                seen.add(key)
                out_rows.append(r)
        return cols, out_rows
    if op == "sort":
        cols, rows = run(plan["input"], sources)
        keys = [(cols.index(k["field"]), k["asc"], k["nulls_first"])
                for k in plan["keys"]]

        def cmp(a, b):
            for i, asc, nf in keys:
                x, y = a[i], b[i]
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

        return cols, sorted(rows, key=cmp_to_key(cmp))
    if op == "limit":
        cols, rows = run(plan["input"], sources)
        return cols, rows[: plan["n"]]
    raise RefError(f"未知算子 {op}")


def as_multiset(rows):
    from collections import Counter
    return Counter(tuple(r) for r in rows)
