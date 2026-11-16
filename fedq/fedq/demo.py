"""演示查询与端到端演示脚本。

每个演示对应任务书的一条要求；全部结果与独立参照解释器对照。
"""

from __future__ import annotations

import json
import os
import threading

from . import datasets, query as Q, reference
from .client import Client, TokenError
from .executor import Executor, QueryFailed, read_result
from .planner import Planner, render_plan


def fld(name):
    return {"k": "field", "name": name}


def lit_int(v):
    return {"k": "lit", "t": "int", "v": str(v)}


def lit_text(v):
    return {"k": "lit", "t": "text", "v": v}


def eq(a, b):
    return {"k": "eq", "a": a, "b": b}


def lt(a, b):
    return {"k": "lt", "a": a, "b": b}


def NOT(x):
    return {"k": "not", "x": x}


def OR(*args):
    return {"k": "or", "args": list(args)}


def AND(*args):
    return {"k": "and", "args": list(args)}


def src(source, table):
    return {"op": "source", "source": source, "table": table}


def filt(pred, inp):
    return {"op": "filter", "pred": pred, "input": inp}


def proj(cols, inp):
    return {"op": "project", "cols": cols, "input": inp}


def sort(keys, inp):
    return {"op": "sort",
            "keys": [{"field": f, "asc": a, "nulls_first": nf} for f, a, nf in keys],
            "input": inp}


def limit(n, inp):
    return {"op": "limit", "n": n, "input": inp}


def agg(group_by, aggs, inp):
    return {"op": "aggregate", "group_by": group_by, "aggs": aggs, "input": inp}


def join(kind, left, right, lkey, rkey):
    return {"op": "join", "kind": kind, "left": left, "right": right,
            "lkey": lkey, "rkey": rkey}


# ---------------------------------------------------------------- 演示查询

def demo_queries():
    """返回 [(名称, 查询JSON, 说明)]。"""
    qs = []

    # 1) A 的文本等值：候选超集 + 本地精筛。统一语义只取 'X' 一行。
    qs.append(("q1_A文本等值候选超集",
               proj(["id", "name"],
                    filt(eq(fld("name"), lit_text("X")), src("A", "items"))),
               "A 有 'X'、'x'、'X '（尾空格）三行；统一 name='X' 只取第一行。"
               "A 远端先回三个候选，本地精确复核。"))

    # 2) B 的 NOT 与 null：NOT(name='X') 不得选中 name=null 的行
    qs.append(("q2_B的NOT与null",
               sort([("id", True, False)],
                    filt(NOT(eq(fld("name"), lit_text("X"))), src("B", "entries"))),
               "B 把 null 比较折叠为 false 再取反 → 远端是超集；"
               "本地残余按三值逻辑排除 null 行（统一：NOT(unknown)=unknown 不通过）。"))

    # 3) OR+NOT 嵌套 + 全序 + limit：远端排序首行在本地被过滤，limit 取正确后继
    pred3 = OR(lt(lit_int(5), fld("amount")),
               NOT(eq(fld("name"), lit_text("X"))))
    qs.append(("q3_OR_NOT嵌套与limit后继",
               limit(2, sort([("amount", True, False), ("id", True, False)],
                             filt(pred3, src("B", "entries")))),
               "远端可下推候选过滤与排序，但 limit 不得下推（残余过滤会改变答案）："
               "远端排序首行 id=2 在本地被残余过滤，limit 2 必须取后继 id=5、id=3。"))

    # 4a) left 连接：右侧条件放在连接前（ON 语义，输入过滤）
    right_filtered = filt(lt(lit_int(3), fld("qty")), src("B", "entries"))
    qs.append(("q4a_left连接_ON侧过滤",
               sort([("name", True, False)],
                    agg(["name"],
                        [{"f": "count_star", "out": "c"},
                         {"f": "count", "field": "qty", "out": "cq"},
                         {"f": "sum", "field": "qty", "out": "sq"}],
                        join("left", src("A", "items"), right_filtered,
                             "amount", "amount"))),
               "右侧过滤在连接前：A 行全部保留，不匹配补 null；"
               "count(*)/count(qty)/sum(qty) 语义分明，全 null 组 sum 为 null。"))

    # 4b) 同一条件放在连接后（WHERE 语义）——结果必须不同
    qs.append(("q4b_left连接_WHERE过滤",
               sort([("name", True, False)],
                    agg(["name"],
                        [{"f": "count_star", "out": "c"},
                         {"f": "count", "field": "qty", "out": "cq"},
                         {"f": "sum", "field": "qty", "out": "sq"}],
                        filt(lt(lit_int(3), fld("qty")),
                             join("left", src("A", "items"), src("B", "entries"),
                                  "amount", "amount")))),
               "同一条件在连接后：补 null 的行被 WHERE 排除（unknown 不通过），"
               "分组集合与 4a 不同。两种位置语义不能混为一谈。"))

    # 5) distinct：统一语义区分大小写、不去尾空格
    qs.append(("q5_distinct统一语义",
               sort([("name", True, False)],
                    {"op": "distinct",
                     "input": proj(["name"], src("A", "items"))}),
               "'X'、'x'、'X ' 是三个不同值（A 的规整规则不适用于用户结果）。"))

    # 6a) 选择性整数过滤 + 列裁剪：真实下推，HTTP 少传行和字段
    qs.append(("q6a_整数过滤真实下推",
               proj(["id", "amount"],
                    filt(AND(lt(lit_int(20), fld("amount")),
                             lt(fld("amount"), lit_int(40))),
                         src("A", "items"))),
               "整数 lt 在 A 上等价 → 真实下推；只传 1 行、2 个字段。"))

    # 6b) 源内聚合下推：远端只回聚合行
    qs.append(("q6b_聚合下推",
               agg([], [{"f": "count_star", "out": "c"},
                        {"f": "sum", "field": "amount", "out": "s"}],
                   filt(lt(lit_int(20), fld("amount")), src("A", "items"))),
               "过滤等价 + 源内聚合可独立完成 → 整体下推；远端只回 1 个聚合行。"))

    # 6c) 空集聚合：count=0，sum=null
    qs.append(("q6c_空集聚合",
               agg([], [{"f": "count_star", "out": "c"},
                        {"f": "sum", "field": "amount", "out": "s"}],
                   filt(lt(lit_int(1000), fld("amount")), src("A", "items"))),
               "空集：count(*)=0，sum=null。"))
    return qs


# ---------------------------------------------------------------- 演示运行

def start_servers(workdir, max_page_bytes=512, tiny_bytes=26):
    caps_a, tables_a = datasets.source_a(max_page_bytes)
    caps_b, tables_b = datasets.source_b(max_page_bytes)
    _, tables_b_tiny = datasets.source_b(tiny_bytes)
    caps_b_tiny, _ = datasets.source_b(tiny_bytes)
    servers = []
    for caps, tables, log in (
            (caps_a, tables_a, os.path.join(workdir, "A.http.jsonl")),
            (caps_b, tables_b, os.path.join(workdir, "B.http.jsonl")),
            (caps_b_tiny, tables_b_tiny, os.path.join(workdir, "Btiny.http.jsonl"))):
        from .server import make_server
        httpd = make_server(caps, tables, log)
        threading.Thread(target=httpd.serve_forever, daemon=True).start()
        servers.append(httpd)
    urls = [f"http://127.0.0.1:{s.server_address[1]}" for s in servers]
    return servers, urls


def run_demo(workdir, out=print):
    os.makedirs(workdir, exist_ok=True)
    servers, (url_a, url_b, url_b_tiny) = start_servers(workdir)
    try:
        out("== 启动模拟源 ==")
        out(f"A: {url_a}   B: {url_b}   B(小页,错误注入): {url_b_tiny}")
        out(f"HTTP 日志: {workdir}/A.http.jsonl, B.http.jsonl, Btiny.http.jsonl")

        caps = {}
        for name, url in (("A", url_a), ("B", url_b)):
            c = Client(name, url)
            caps[name] = c.capabilities()
            out(f"\n-- 源 {name} 能力 --")
            out(json.dumps(caps[name].to_json(), ensure_ascii=False, indent=1))

        data = {"A": {"items": {"columns": datasets.source_a()[1]["items"]["columns"],
                                "rows": datasets.A_ITEMS}},
                "B": {"entries": {"columns": datasets.source_b()[1]["entries"]["columns"],
                                  "rows": datasets.B_ENTRIES}}}

        for name, qjson, note in demo_queries():
            out("\n" + "=" * 70)
            out(f"== {name}")
            out(f"   {note}")
            clients = {"A": Client("A", url_a), "B": Client("B", url_b)}
            planner = Planner(caps)
            q = Q.from_json(qjson)
            plan = planner.plan(q)
            out("-- 计划 --")
            out(render_plan(plan))
            exe = Executor(clients, os.path.join(workdir, "work"))
            out_path = os.path.join(workdir, f"{name}.jsonl")
            stats, rows = exe.execute(plan, out_path)
            result = read_result(out_path)
            assert result["complete"], "结果必须有成功结束标记"
            out(f"-- 结果（{len(result['rows'])} 行）列={result['columns']} --")
            for r in result["rows"]:
                out(f"   {r}")
            # 与独立参照解释器对照
            ref_cols, ref_rows = reference.run(qjson, data)
            assert ref_cols == result["columns"], (ref_cols, result["columns"])
            ordered = any(o["op"] == "sort" for o in _walk(qjson))
            if ordered:
                assert ref_rows == result["rows"], (ref_rows, result["rows"])
            else:
                assert reference.as_multiset(ref_rows) == \
                    reference.as_multiset(result["rows"]), (ref_rows, result["rows"])
            out("-- 与独立参照解释器对照: 一致 ✓")
            out(f"-- 执行器统计: {json.dumps(stats['sources'], ensure_ascii=False)}")

        _error_demos(url_a, url_b, url_b_tiny, workdir, caps, out)
        _degraded_caps_demo(url_a, caps, workdir, out)

        out("\n== 全部演示检查通过 ==")
    finally:
        for s in servers:
            s.shutdown()


def _walk(qjson):
    yield qjson
    for key in ("input", "left", "right"):
        if key in qjson:
            yield from _walk(qjson[key])


def _error_demos(url_a, url_b, url_b_tiny, workdir, caps, out):
    out("\n" + "=" * 70)
    out("== 失败边界 ==")

    # 1) 坏令牌
    c = Client("B", url_b)
    try:
        c._request("POST", "/query", {"token": "bogus", "page": 0})
        raise AssertionError("坏令牌必须失败")
    except TokenError as e:
        out(f"坏令牌 → 明确失败 ✓ ({e})")

    def expect_failure(name, url, table, source="B"):
        clients = {"A": Client("A", url_a), "B": Client("B", url)}
        planner = Planner(caps)
        q = Q.from_json(src(source, table))
        plan = planner.plan(q)
        exe = Executor(clients, os.path.join(workdir, "work"))
        out_path = os.path.join(workdir, f"err_{table}.jsonl")
        try:
            exe.execute(plan, out_path)
            raise AssertionError(f"{name} 必须失败")
        except QueryFailed as e:
            r = read_result(out_path)
            assert not r["complete"] and r["status"] == "error"
            src_rows = {k: v["rows"] for k, v in r["stats"].get("sources", {}).items()}
            out(f"{name} → 明确失败 ✓ ({e})；已读源行 {src_rows}、结果行 "
                f"{len(r['rows'])}，无成功结束标记，不得视为完整结果")

    # 2) 中途 HTTP 错误（小页强制多页，第 2 页注入 500）
    expect_failure("中途HTTP错误(flaky)", url_b_tiny, "flaky")
    # 3) 坏字段类型（text 列混入 int）
    expect_failure("坏字段类型(badtypes)", url_a, "badtypes", source="A")
    # 4) 分页重复 id
    expect_failure("分页重复id(dupids)", url_b_tiny, "dupids")


def _degraded_caps_demo(url_a, caps, workdir, out):
    out("\n" + "=" * 70)
    out("== 连接器能力不足时仍正确执行 ==")
    weak = dict(caps)
    import copy
    weak_a = copy.deepcopy(caps["A"])
    weak_a.aggregates = frozenset()   # 假装 A 没有聚合能力
    weak["A"] = weak_a
    qjson = dict(demo_queries()[7][1])  # q6b 聚合查询
    clients = {"A": Client("A", url_a)}
    plan = Planner(weak).plan(Q.from_json(qjson))
    out(render_plan(plan))
    exe = Executor(clients, os.path.join(workdir, "work"))
    out_path = os.path.join(workdir, "degraded.jsonl")
    exe.execute(plan, out_path)
    r = read_result(out_path)
    assert r["complete"] and r["rows"] == [[2, 80]], r["rows"]
    out(f"无聚合能力的连接器：聚合留在本地，结果仍正确 ✓ {r['rows']}")
