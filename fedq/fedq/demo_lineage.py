"""结果数据血缘的端到端验收演示（小夹具入库，规模数据运行时生成）。

覆盖任务书全部验收点：
1) 主例：A 的一笔金额 10 的账目匹配 B 的两行——直接连接后汇总金额是 20
   （A 的同一行参与两次，两个匹配对都可展开）；先投影成金额并 distinct 再汇总，
   金额是 10，且这个 10 保留两种备选依据（不是 20，也不是只留一个匹配对）。
2) 小例子：left 未匹配 vs 右侧真实 null、全 null/空集、正负抵消与零贡献、
   重复投影、同表两次引用——手写预期结果出现与推导关系，逐条核对。
3) 独立参照程序（fedq/lineage_ref.py，自行枚举输入出现并推导，不调用生产
   血缘生成器）对全部查询做结果/展开/反查三方对照。
4) 多层 distinct + 连接共享子推导：证据边数是按出现共享的线性数，
   不枚举备选分支的全部组合。
5) 多页大组（2500 成员）小页逐次展开，核对总贡献与反查；反查本身也多页。
6) 过滤/limit 只保留实际出现：被淘汰的兄弟行不出现在追溯与反查中。
7) 完成后关闭原视图、停掉源进程，新对象（脚本里是新进程）仍能展开与反查。
8) 一次证据写出失败：run 明确标记不完整，不能冒充成功；目录可识别。
"""

from __future__ import annotations

import json
import os
import shutil

from . import lineage_ref
from .coordinator import Coordinator
from .demo_views import Fleet
from .lineage import LineageError, LineageService, LineageStateError
from .scenarios import (agg, eq, filt, fld, join, limit, lit_int, lt, proj,
                        sort, src)
from .versions import SourceHistory

SCALE_N = 2500          # 大组成员数（运行时生成）


def _check(cond, msg):
    if not cond:
        raise AssertionError("断言失败: " + msg)


def dist(inp):
    return {"op": "distinct", "input": inp}


# ================================================================ 夹具

def fixture_histories(scale_n=SCALE_N):
    """两个逻辑源的小夹具；scale 表规模行运行时生成。"""
    a = SourceHistory("A", "inst-A-lin1", "A", {
        "accounts": {"columns": {"id": "int", "acct": "text", "amount": "int"},
                     "key": ["id"]},
        "flows": {"columns": {"id": "int", "amt": "int"}, "key": ["id"]},
        "nulls": {"columns": {"id": "int", "v": "int"}, "key": ["id"]},
        "anchor": {"columns": {"id": "int", "k": "int"}, "key": ["id"]},
    }, initial={
        # a1 金额10 匹配 B 两行；a2 金额20 匹配 B 一行(qty 为真实 null)；a3 无匹配
        "accounts": [[1, "a1", 10], [2, "a2", 20], [3, "a3", 30]],
        # 正负抵消 7,-3,-4,0；id=1 与 id=5 同为 7（重复投影/自连接用）
        "flows": [[1, 7], [2, -3], [3, -4], [4, 0], [5, 7]],
        "nulls": [[1, None], [2, None]],
        "anchor": [[1, 1]],
    }, commits=[], head=0)
    b = SourceHistory("B", "inst-B-lin1", "B", {
        "entries": {"columns": {"id": "int", "name": "text", "amount": "int",
                                "qty": "int"}, "key": ["id"]},
        "adjusts": {"columns": {"id": "int", "amount": "int"}, "key": ["id"]},
        "scale": {"columns": {"id": "int", "k": "int", "amt": "int"},
                  "key": ["id"]},
    }, initial={
        "entries": [[1, "x", 10, 5], [2, "y", 10, None], [3, "z", 20, None]],
        "adjusts": [[1, 10], [2, 10], [3, 10]],
        "scale": [[i, 1, (i % 7) - 3] for i in range(1, scale_n + 1)],
    }, commits=[], head=0)
    return {"A": a, "B": b}


def ref_data(hists):
    """供独立参照程序使用的数据与源坐标（与视图冻结内容一致，F 全 0）。"""
    data, meta = {}, {}
    for s, h in hists.items():
        meta[s] = {"instance_id": h.instance_id, "at": 0}
        data[s] = {t: {"columns": sch["columns"], "key": sch.get("key", ["id"]),
                       "rows": h.initial[t]}
                   for t, sch in h.schemas.items()}
    return data, meta


# ================================================================ 演示查询

def _join_ab10():
    """A.accounts 中 id=1 的账目 ⋈ B.entries ON amount（主例公共部分）。"""
    return join("inner",
                filt(eq(fld("id"), lit_int(1)),
                     proj(["id", "amount"], src("A", "accounts"))),
                proj(["id", "amount"], src("B", "entries")),
                "amount", "amount")


def lineage_queries():
    qs = []
    qs.append(("L1_主例_直接连接汇总",
               agg([], [{"f": "sum", "field": "amount", "out": "s"},
                        {"f": "count_star", "out": "c"}], _join_ab10()),
               "金额10的账目匹配B两行：sum=20、count=2，两个匹配对都可展开"))
    qs.append(("L2_主例_投影distinct再汇总",
               agg([], [{"f": "sum", "field": "amount", "out": "s"},
                        {"f": "count_star", "out": "c"}],
                   dist(proj(["amount"], _join_ab10()))),
               "投影成金额并distinct后sum=10；这个10保留两种备选依据"))
    left_ab = join("left", proj(["id", "acct", "amount"], src("A", "accounts")),
                   proj(["id", "name", "amount", "qty"], src("B", "entries")),
                   "amount", "amount")
    qs.append(("L3_left连接_未匹配与真实null",
               sort([("id", True, False), ("r_id", True, False)], left_ab),
               "a3无匹配补null（标记）；a1×y、a2×z 的右侧是 qty 为 null 的真实行"))
    qs.append(("L3b_left连接聚合_null计数",
               agg([], [{"f": "count_star", "out": "c"},
                        {"f": "count", "field": "qty", "out": "cq"},
                        {"f": "sum", "field": "amount", "out": "s"}], left_ab),
               "count(*)=4、count(qty)=1：真实null与补null都不计入cq，但展开不同"))
    qs.append(("L4a_全null聚合",
               agg([], [{"f": "count_star", "out": "c"},
                        {"f": "count", "field": "v", "out": "cv"},
                        {"f": "sum", "field": "v", "out": "sv"}],
                   src("A", "nulls")),
               "count(*)=2、count(v)=0、sum(v)=null；两个成员都因null未计入"))
    qs.append(("L4b_空集全局聚合",
               agg([], [{"f": "count_star", "out": "c"},
                        {"f": "count", "field": "v", "out": "cv"},
                        {"f": "sum", "field": "v", "out": "sv"}],
                   filt(lt(lit_int(100), fld("id")), src("A", "nulls"))),
               "全局空集聚合也有一条可解释结果：count=0、sum=null、明确没有输入"))
    qs.append(("L4c_空分组聚合无结果",
               agg(["id"], [{"f": "count_star", "out": "c"}],
                   filt(lt(lit_int(100), fld("id")), src("A", "nulls"))),
               "空的分组聚合没有结果行"))
    qs.append(("L4d_分组键与成员",
               agg(["id"], [{"f": "count_star", "out": "c"},
                            {"f": "sum", "field": "v", "out": "sv"}],
                   src("A", "nulls")),
               "分组键说明本组成员；两组各一个成员、sum 全 null"))
    qs.append(("L5_正负抵消与零贡献",
               agg([], [{"f": "sum", "field": "amt", "out": "s"},
                        {"f": "count_star", "out": "c"}],
                   filt(lt(fld("id"), lit_int(5)), src("A", "flows"))),
               "7,-3,-4,0 抵消为 0：四个贡献都保留（含 0），来源不消失"))
    qs.append(("L6_重复投影保留两个出现",
               proj(["amt"], src("A", "flows")),
               "未经过distinct：两个 amt=7 是两个出现，各有 row_ref"))
    qs.append(("L6b_distinct合并为备选",
               dist(proj(["amt"], src("A", "flows"))),
               "distinct 后 amt=7 只剩一个结果，但保留两个备选依据"))
    qs.append(("L7_同表两次引用",
               sort([("id", True, False), ("r_id", True, False)],
                    join("inner", proj(["id", "amt"], src("A", "flows")),
                         proj(["id", "amt"], src("A", "flows")),
                         "amt", "amt")),
               "flows 自连接：源行身份按查询树输入位置 /i/l/i 与 /i/r/i 区分"))
    qs.append(("L8_多层distinct与共享子推导",
               agg([], [{"f": "sum", "field": "amount", "out": "s"},
                        {"f": "count_star", "out": "c"}],
                   proj(["amount"],
                        join("inner",
                             dist(proj(["amount"], _join_ab10())),
                             dist(proj(["amount"], src("B", "adjusts"))),
                             "amount", "amount"))),
               "两个 distinct 的结果再连接：子推导共享，证据边线性增长，不枚举组合"))
    qs.append(("L9_大组多页展开",
               agg(["k"], [{"f": "count_star", "out": "c"},
                           {"f": "sum", "field": "amt", "out": "s"},
                           {"f": "count", "field": "amt", "out": "ca"}],
                   src("B", "scale")),
               f"单组 {SCALE_N} 个成员：小页逐次展开核对总贡献"))
    join_scale = join("inner", proj(["id", "k"], src("A", "anchor")),
                      proj(["id", "k", "amt"], src("B", "scale")), "k", "k")
    qs.append(("L10_反查多页",
               proj(["id", "r_id", "amt"],
                    sort([("id", True, False), ("r_id", True, False)],
                         join_scale)),
               f"A.anchor 一行被 {SCALE_N} 个结果使用：反查分页不遗漏不重复"))
    qs.append(("L10b_limit只保留实际出现",
               limit(3, sort([("id", True, False), ("r_id", True, False)],
                             join_scale)),
               "limit 3：反查 scale pk>=4 的源行不得命中任何结果"))
    return qs


# ================================================================ 对照工具（演示与测试共用）

def fetch_all_rows(service, run_id, page_size=100):
    """翻遍结果页：校验不遗漏、不重复，返回 items 列表。"""
    first = service.rows(run_id, 0, page_size)
    total, items = first["total"], list(first["items"])
    page = 1
    while len(items) < total:
        r = service.rows(run_id, page, page_size)
        _check(r["items"], f"run {run_id} 第 {page} 页不应为空（total={total}）")
        items.extend(r["items"])
        page += 1
    _check(len(items) == total, f"结果翻页应得 {total} 项，实得 {len(items)}")
    refs = [it["row_ref"] for it in items]
    _check(len(set(refs)) == len(refs), "结果翻页出现重复 row_ref")
    _check([it["seq"] for it in items] == list(range(total)), "结果 seq 应连续")
    return items


def fetch_all_pages(fetch):
    """fetch(page) -> 含 total/items 的页；收集全部 items。"""
    first = fetch(0)
    total, items = first["total"], list(first["items"])
    page = 1
    while len(items) < total:
        r = fetch(page)
        _check(r["items"], f"第 {page} 页不应为空（total={total}）")
        items.extend(r["items"])
        page += 1
    _check(len(items) == total, f"分页应得 {total} 项，实得 {len(items)}")
    return items


def _norm_contrib(c):
    return {out: ({"kind": "sum", "value": str(v[1])} if v[0] == "sum"
                  else {"kind": v[0]}) for out, v in c.items()}


def walk_to(service, row_ref, op):
    """沿直通推导向下走到指定算子的出现（用于穿过 sort/project 等）。"""
    ex = service.expand(row_ref)
    while ex["node"]["op"] != op:
        _check(ex["derivation"]["kind"] == "pass",
               f"walk_to 只穿过直通节点，遇到 {ex['node']['op']}")
        ex = service.expand(ex["derivation"]["input"]["row_ref"])
    return ex


def compare_expand(service, run_id, row_ref, occ, page_size=2):
    """生产展开（故意用小页） vs 独立参照出现，逐层递归对照。"""
    prod = service.expand(row_ref, page=0, page_size=page_size)
    _check(prod["values"] == list(occ.values),
           f"{row_ref} 值不符: {prod['values']} != {list(occ.values)}")
    _check(prod["node"]["op"] == occ.op,
           f"{row_ref} 算子不符: {prod['node']['op']} != {occ.op}")
    d = occ.deriv
    deriv = prod["derivation"]
    kind = d[0]
    if kind == "scan":
        _check(deriv["kind"] == "scan", "应为 scan 推导")
        sr = deriv["source_row"]
        got = (sr["source"], sr["instance_id"], sr["table"], sr["at"],
               tuple(sr["pk"]), sr["path"])
        _check(got == d[1], f"源行身份不符: {got} != {d[1]}")
        return
    if kind == "pass":
        _check(deriv["kind"] == "pass" and deriv["op"] == occ.op,
               "应为直通推导")
        compare_expand(service, run_id, deriv["input"]["row_ref"], d[1],
                       page_size)
        return
    if kind == "join":
        _check(deriv["kind"] == "join", "应为 join 推导")
        edges = {e["slot"]: e for e in deriv["edges"]}
        compare_expand(service, run_id, edges["left"]["row_ref"], d[1],
                       page_size)
        if d[2] is None:
            _check(edges["right"].get("marker") == "null_fill"
                   and "row_ref" not in edges["right"],
                   "无匹配右侧必须是 null_fill 标记，不得虚构源行")
        else:
            _check("row_ref" in edges["right"], "匹配时右侧必须是真实出现")
            compare_expand(service, run_id, edges["right"]["row_ref"], d[2],
                           page_size)
        return
    if kind == "distinct":
        _check(deriv["kind"] == "distinct", "应为 distinct 推导")
        alts = fetch_all_pages(
            lambda p: service.expand(row_ref, p, page_size)["derivation"]
            ["alternatives"])
        _check(len(alts) == len(d[1]),
               f"备选数不符: {len(alts)} != {len(d[1])}")
        for item, alt_occ in zip(alts, d[1]):
            compare_expand(service, run_id, item["row_ref"], alt_occ,
                           page_size)
        return
    if kind == "agg":
        _check(deriv["kind"] == "aggregate", "应为 aggregate 推导")
        members = fetch_all_pages(
            lambda p: service.expand(row_ref, p, page_size)["derivation"]
            ["members"])
        _check(len(members) == len(d[1]),
               f"成员数不符: {len(members)} != {len(d[1])}")
        for item, (m_occ, contrib) in zip(members, d[1]):
            _check(item["contribs"] == _norm_contrib(contrib),
                   f"成员贡献不符: {item['contribs']} != {_norm_contrib(contrib)}")
            compare_expand(service, run_id, item["row_ref"], m_occ, page_size)
        # 汇总口径：counted/null_skipped/value 与成员贡献一致
        for a in deriv["aggregates"]:
            out = a["out"]
            counted = sum(1 for _, c in d[1] if c[out][0] != "null_skipped")
            skipped = sum(1 for _, c in d[1] if c[out][0] == "null_skipped")
            _check(a["counted"] == counted and a["null_skipped"] == skipped,
                   f"聚合 {out} 计数口径不符")
        _check(deriv["empty_input"] == (len(d[1]) == 0),
               "empty_input 标记不符")
        return
    raise AssertionError(f"未知推导 {kind}")


def check_run_vs_reference(service, run_id, qjson, data, meta,
                           expand_sample=None, lookup_sample=None):
    """结果、逐层展开、按源行反查 三方对照独立参照程序。"""
    roots = lineage_ref.execute(qjson, data, meta)
    items = fetch_all_rows(service, run_id)
    _check([it["values"] for it in items] == [list(o.values) for o in roots],
           f"run {run_id} 结果与独立参照不符: "
           f"{[it['values'] for it in items][:5]} != "
           f"{[list(o.values) for o in roots][:5]}")
    # 展开对照：小数据全量，大数据抽样（首尾+均匀抽样）
    idxs = list(range(len(roots)))
    if expand_sample is not None and len(idxs) > expand_sample:
        step = max(1, len(idxs) // (expand_sample - 2))
        idxs = sorted({0, len(roots) - 1} | set(range(0, len(roots), step)))
    for i in idxs:
        compare_expand(service, run_id, items[i]["row_ref"], roots[i])
    # 反查对照：每个源行身份（含未使用的）都与参照一致
    for s, tables in data.items():
        for t, td in tables.items():
            cols = list(td["columns"].keys())
            key = td.get("key", ["id"])
            rows = td["rows"]
            if lookup_sample is not None and len(rows) > lookup_sample:
                step = max(1, len(rows) // lookup_sample)
                rows = rows[::step]
            for r in rows:
                pk = [r[cols.index(k)] for k in key]
                expected = lineage_ref.used_by(roots, s, t, tuple(pk))
                got = fetch_all_pages(
                    lambda p, s=s, t=t, pk=pk:
                    service.lookup(run_id, s, t, pk, page=p, page_size=97))
                _check({it["seq"] for it in got} == expected,
                       f"反查 {s}.{t} pk={pk} 不符: "
                       f"{sorted(it['seq'] for it in got)[:8]} != "
                       f"{sorted(expected)[:8]}")
    return roots, items


# ================================================================ 验收流程

def run(workdir, out=print):
    if os.path.exists(workdir):
        shutil.rmtree(workdir)
    os.makedirs(workdir, exist_ok=True)
    res_dir = os.path.join(workdir, "results")
    q_dir = os.path.join(workdir, "queries")
    os.makedirs(res_dir)
    os.makedirs(q_dir)

    hists = fixture_histories()
    data, meta = ref_data(hists)
    fleet = Fleet(workdir)
    fleet.start(hists)
    coord = Coordinator(os.path.join(workdir, "coord"))
    service = LineageService(coord.workdir)
    queries = lineage_queries()
    for name, qjson, note in queries:
        with open(os.path.join(q_dir, f"{name}.json"), "w",
                  encoding="utf-8") as f:
            json.dump(qjson, f, ensure_ascii=False, indent=1)

    runs = {}
    try:
        opened = coord.open({"sources": {n: {"url": u}
                                         for n, u in fleet.urls.items()}},
                            note="血缘演示视图")
        vid = opened["view_id"]
        out(f"[0] 打开视图 {vid}，F={json.dumps(opened['F'])}（两源版本固定）")

        # ---- 全部查询各做一次带血缘执行 ----
        for name, qjson, note in queries:
            info = coord.lineage_run(vid, qjson, name=name)
            runs[name] = info["run_id"]
            out(f"  带血缘执行 {name}: run_id={info['run_id']} "
                f"行数={info['rows_out']}")
        _check(len(set(runs.values())) == len(runs), "run_id 必须互不相同")

        # ---- 1) 主例：袋重数 vs 备选来源 ----
        out("\n[1] 主例：直接连接汇总=20（两个匹配对）；投影+distinct 后=10（两种备选）")
        r1 = fetch_all_rows(service, runs["L1_主例_直接连接汇总"])
        _check([it["values"] for it in r1] == [[20, 2]],
               "L1 手写预期：sum=20, count=2")
        ex1 = service.expand(r1[0]["row_ref"], 0, 10)
        _check(ex1["derivation"]["members"]["total"] == 2, "L1 应有 2 个成员")
        pair_refs = [m["row_ref"] for m in
                     ex1["derivation"]["members"]["items"]]
        pairs = []
        for mref in pair_refs:
            mx = service.expand(mref)
            _check(mx["derivation"]["kind"] == "join", "成员应是连接出现")
            edges = {e["slot"]: e for e in mx["derivation"]["edges"]}
            pairs.append((mx["values"], edges["left"]["values"],
                          edges["right"]["values"]))
        # 手写预期：两个匹配对 (a1×x) 与 (a1×y)，A 的同一行参与两次
        _check([p[0] for p in pairs] == [[1, 10, 1, 10], [1, 10, 2, 10]],
               f"L1 两个匹配对不符: {pairs}")
        _check(pairs[0][1] == pairs[1][1] == [1, 10],
               "A 的同一行 [1,10] 应参与两次")
        _check(pairs[0][2] == [1, 10] and pairs[1][2] == [2, 10],
               "右侧应是 B 的两条不同投影出现 [1,10] 与 [2,10]")
        # 再向下展开到扫描层：两条右侧分别追到 B.entries 主键 1 与 2 的真实行
        right_pks = []
        for pidx in (0, 1):
            rref = [e for e in service.expand(pair_refs[pidx])["derivation"]
                    ["edges"] if e["slot"] == "right"][0]["row_ref"]
            rx = service.expand(rref)                # project → scan
            sx = service.expand(rx["derivation"]["input"]["row_ref"])
            right_pks.append(sx["derivation"]["source_row"]["pk"])
        _check(right_pks == [[1], [2]],
               f"两个匹配对的右侧应分别追到 B.entries pk=1 与 pk=2: {right_pks}")
        left_ref_0 = [e for e in service.expand(pair_refs[0])["derivation"]
                      ["edges"] if e["slot"] == "left"][0]["row_ref"]
        left_ref_1 = [e for e in service.expand(pair_refs[1])["derivation"]
                      ["edges"] if e["slot"] == "left"][0]["row_ref"]
        _check(left_ref_0 == left_ref_1,
               "两个匹配对应共享同一个左侧出现（子推导共享，不是复制）")
        out(f"  L1: sum=20 由 2 个匹配对逐份计入；A 行出现 {left_ref_0} 被两对共享 ✓")

        r2 = fetch_all_rows(service, runs["L2_主例_投影distinct再汇总"])
        _check([it["values"] for it in r2] == [[10, 1]],
               "L2 手写预期：sum=10, count=1（不是 20）")
        ex2 = service.expand(r2[0]["row_ref"], 0, 10)
        mem = ex2["derivation"]["members"]["items"]
        _check(len(mem) == 1 and mem[0]["contribs"]["s"] ==
               {"kind": "sum", "value": "10"}, "L2 的 sum 只有一份 10 的贡献")
        dx = service.expand(mem[0]["row_ref"], 0, 10)
        _check(dx["derivation"]["kind"] == "distinct", "L2 成员应来自 distinct")
        alts = dx["derivation"]["alternatives"]
        _check(alts["total"] == 2, "distinct 结果应保留 2 个备选依据")
        alt_joins = []
        for a in alts["items"]:
            ax = service.expand(a["row_ref"])          # project 出现
            jx = service.expand(ax["derivation"]["input"]["row_ref"])
            _check(jx["derivation"]["kind"] == "join", "备选应各自回到一个匹配对")
            alt_joins.append(jx["values"])
        _check(sorted(alt_joins) == [[1, 10, 1, 10], [1, 10, 2, 10]],
               "L2 的两个备选必须仍是原来的两个匹配对（不能只留一个）")
        out(f"  L2: sum=10；distinct 结果 {mem[0]['row_ref']} 保留 2 个备选匹配对 "
            f"{alt_joins} ✓")

        # ---- 2) 小例子（手写预期 + 参照对照） ----
        out("\n[2] 小例子：left未匹配/真实null、全null/空集、抵消、重复投影、同表两次")
        r3 = fetch_all_rows(service, runs["L3_left连接_未匹配与真实null"])
        _check([it["values"] for it in r3] == [
            [1, "a1", 10, 1, "x", 10, 5],
            [1, "a1", 10, 2, "y", 10, None],
            [2, "a2", 20, 3, "z", 20, None],
            [3, "a3", 30, None, None, None, None]], "L3 手写预期（4 行）")
        ex3null = walk_to(service, r3[3]["row_ref"], "join")
        redges = {e["slot"]: e for e in ex3null["derivation"]["edges"]}
        _check(redges["right"].get("marker") == "null_fill"
               and "row_ref" not in redges["right"],
               "a3 无匹配：右侧必须是 null_fill 标记，不得虚构右源行")
        ex3real = walk_to(service, r3[1]["row_ref"], "join")
        redges2 = {e["slot"]: e for e in ex3real["derivation"]["edges"]}
        _check("row_ref" in redges2["right"],
               "a1×y 的右侧必须是真实出现（qty 为 null 的真实行）")
        rscan = service.expand(redges2["right"]["row_ref"])
        while rscan["derivation"]["kind"] == "pass":
            rscan = service.expand(rscan["derivation"]["input"]["row_ref"])
        _check(rscan["derivation"]["source_row"]["pk"] == [2],
               "真实右侧应追到 B.entries 主键 2")
        out("  L3: 补null标记 vs 真实null行 区分 ✓（a3 无右行；a1×y 右行真实存在、qty=null）")

        r3b = fetch_all_rows(service, runs["L3b_left连接聚合_null计数"])
        _check([it["values"] for it in r3b] == [[4, 1, 70]],
               "L3b 手写预期：count(*)=4, count(r_qty)=1, sum=70")
        ex3b = service.expand(r3b[0]["row_ref"], 0, 10)
        aggs = {a["out"]: a for a in ex3b["derivation"]["aggregates"]}
        _check(aggs["cq"]["counted"] == 1 and aggs["cq"]["null_skipped"] == 3,
               "count(r_qty)：1 计入、3 因 null 未计入")
        mem3b = ex3b["derivation"]["members"]["items"]
        kinds = [m["contribs"]["cq"]["kind"] for m in mem3b]
        _check(kinds == ["counted", "null_skipped", "null_skipped",
                         "null_skipped"], f"逐成员 r_qty 贡献不符: {kinds}")
        # 真实 null 行与补 null 行在成员展开中可区分
        m_y = service.expand(mem3b[1]["row_ref"])
        m_a3 = service.expand(mem3b[3]["row_ref"])
        e_y = {e["slot"]: e for e in m_y["derivation"]["edges"]}
        e_a3 = {e["slot"]: e for e in m_a3["derivation"]["edges"]}
        _check("row_ref" in e_y["right"] and
               e_a3["right"].get("marker") == "null_fill",
               "成员展开必须区分真实右行(值为null)与没有右行(标记)")
        out("  L3b: count(r_qty)=1 计入 x；y/z 真实行 qty=null 与 a3 补null 都未计入，"
            "但成员展开可区分 ✓")

        r4a = fetch_all_rows(service, runs["L4a_全null聚合"])
        _check([it["values"] for it in r4a] == [[2, 0, None]],
               "L4a 手写预期：count(*)=2, count(v)=0, sum(v)=null")
        ex4a = service.expand(r4a[0]["row_ref"], 0, 10)
        _check(all(m["contribs"]["sv"]["kind"] == "null_skipped"
                   for m in ex4a["derivation"]["members"]["items"]),
               "L4a 两个成员对 sum 都应因 null 未计入")
        r4b = fetch_all_rows(service, runs["L4b_空集全局聚合"])
        _check([it["values"] for it in r4b] == [[0, 0, None]],
               "L4b 手写预期：count=0、sum=null")
        ex4b = service.expand(r4b[0]["row_ref"], 0, 10)
        _check(ex4b["derivation"]["empty_input"] is True and
               ex4b["derivation"]["members"]["total"] == 0,
               "L4b 必须明确没有输入")
        r4c = fetch_all_rows(service, runs["L4c_空分组聚合无结果"])
        _check(r4c == [], "L4c 空分组聚合没有结果行")
        r4d = fetch_all_rows(service, runs["L4d_分组键与成员"])
        _check([it["values"] for it in r4d] == [[1, 1, None], [2, 1, None]],
               "L4d 手写预期：两组 (id=1,id=2)，sum 全 null")
        ex4d = service.expand(r4d[0]["row_ref"], 0, 10)
        _check(ex4d["derivation"]["group_key"] == {"id": 1},
               "L4d 分组键应说明本组")
        out("  L4: 全null sum=null、空集 count=0/sum=null 且明确无输入、空分组无结果 ✓")

        r5 = fetch_all_rows(service, runs["L5_正负抵消与零贡献"])
        _check([it["values"] for it in r5] == [[0, 4]], "L5 手写预期：sum=0")
        ex5 = service.expand(r5[0]["row_ref"], 0, 10)
        contribs5 = [m["contribs"]["s"] for m in
                     ex5["derivation"]["members"]["items"]]
        _check(contribs5 == [{"kind": "sum", "value": "7"},
                             {"kind": "sum", "value": "-3"},
                             {"kind": "sum", "value": "-4"},
                             {"kind": "sum", "value": "0"}],
               f"L5 抵消与零贡献都必须保留: {contribs5}")
        out("  L5: 7,-3,-4,0 抵消为 0，四个贡献逐份保留 ✓")

        r6 = fetch_all_rows(service, runs["L6_重复投影保留两个出现"])
        _check([it["values"] for it in r6] ==
               [[7], [-3], [-4], [0], [7]], "L6 手写预期：两个 amt=7 各自保留")
        _check(r6[0]["row_ref"] != r6[4]["row_ref"],
               "同值的两个出现必须有不同 row_ref")
        r6b = fetch_all_rows(service, runs["L6b_distinct合并为备选"])
        _check([it["values"] for it in r6b] == [[7], [-3], [-4], [0]],
               "L6b 手写预期：distinct 后 4 行")
        ex6b = service.expand(r6b[0]["row_ref"], 0, 10)
        _check(ex6b["derivation"]["alternatives"]["total"] == 2,
               "distinct 的 amt=7 应有 2 个备选依据")
        out("  L6: 投影保留两个 amt=7（不同 row_ref）；distinct 合并但留 2 个备选 ✓")

        r7 = fetch_all_rows(service, runs["L7_同表两次引用"])
        _check([it["values"] for it in r7] == [
            [1, 7, 1, 7], [1, 7, 5, 7], [2, -3, 2, -3], [3, -4, 3, -4],
            [4, 0, 4, 0], [5, 7, 1, 7], [5, 7, 5, 7]], "L7 手写预期（7 对）")
        lk7 = fetch_all_pages(
            lambda p: service.lookup(runs["L7_同表两次引用"], "A", "flows", [1],
                                     page=p, page_size=2))
        via = sorted((it["seq"], it["via"]["path"]) for it in lk7)
        _check(via == [(0, "/i/l/i"), (0, "/i/r/i"), (1, "/i/l/i"),
                       (5, "/i/r/i")],
               f"flows pk=1 应经两个输入位置被使用: {via}")
        lk7l = service.lookup(runs["L7_同表两次引用"], "A", "flows", [1],
                              path="/i/l/i")
        _check({it["seq"] for it in lk7l["items"]} == {0, 1},
               "限定左输入位置后只剩左侧使用")
        out("  L7: 同表两次引用按 /i/l/i 与 /i/r/i 区分；反查可限定位置 ✓")

        # ---- 3) 多层 distinct + 共享子推导：不枚举组合 ----
        out("\n[3] 多层 distinct + 连接共享：证据边线性，不枚举备选组合")
        r8 = fetch_all_rows(service, runs["L8_多层distinct与共享子推导"])
        _check([it["values"] for it in r8] == [[10, 1]],
               "L8 手写预期：sum=10, count=1")
        import sqlite3 as _sq
        ev = _sq.connect(os.path.join(service.dir, runs["L8_多层distinct与共享子推导"],
                                      "evidence.db"))
        n_occ = ev.execute("SELECT COUNT(*) FROM occs").fetchone()[0]
        n_edge = ev.execute("SELECT COUNT(*) FROM edges").fetchone()[0]
        n_usage = ev.execute("SELECT COUNT(*) FROM usage").fetchone()[0]
        ev.close()
        # 手写清点：28 个出现、25 条边、6 对反查（推导见 docs/LINEAGE.md）
        _check((n_occ, n_edge, n_usage) == (28, 25, 6),
               f"L8 证据规模应为线性 (28,25,6)，实得 {(n_occ, n_edge, n_usage)}")
        ex8 = service.expand(r8[0]["row_ref"], 0, 10)
        mem8 = ex8["derivation"]["members"]["items"][0]
        pj8 = service.expand(mem8["row_ref"])
        j8 = service.expand(pj8["derivation"]["input"]["row_ref"])
        _check(len(j8["derivation"]["edges"]) == 2,
               "连接出现应只有左右两条依据边（不是 2×3=6 条组合边）")
        d1 = service.expand([e for e in j8["derivation"]["edges"]
                             if e["slot"] == "left"][0]["row_ref"])
        d2 = service.expand([e for e in j8["derivation"]["edges"]
                             if e["slot"] == "right"][0]["row_ref"])
        _check(d1["derivation"]["alternatives"]["total"] == 2 and
               d2["derivation"]["alternatives"]["total"] == 3,
               "两侧 distinct 各自保留备选（2 与 3），但连接层不展开组合")
        out(f"  L8: 出现 {n_occ}、边 {n_edge}、反查对 {n_usage}；"
            f"连接层 2 条边，备选 2/3 各自展开，无 2×3 组合 ✓")

        # ---- 4) 大组多页 + 反查多页 ----
        out(f"\n[4] 大组 {SCALE_N} 成员小页展开；反查多页")
        r9 = fetch_all_rows(service, runs["L9_大组多页展开"])
        expected_sum = sum((i % 7) - 3 for i in range(1, SCALE_N + 1))
        _check([it["values"] for it in r9] == [[1, SCALE_N, expected_sum, SCALE_N]],
               "L9 手写预期：单组 count=成员数、sum 精确")
        members9 = fetch_all_pages(
            lambda p: service.expand(r9[0]["row_ref"], p, 100)["derivation"]
            ["members"])
        _check(len({m["row_ref"] for m in members9}) == SCALE_N,
               "大组成员分页不得重复")
        total9 = sum(int(m["contribs"]["s"]["value"]) for m in members9)
        _check(total9 == expected_sum,
               f"逐页累计贡献 {total9} 应等于汇总 {expected_sum}")
        out(f"  L9: 100/页 × 25 页累计贡献={total9} == sum={expected_sum} ✓")

        lk10 = fetch_all_pages(
            lambda p: service.lookup(runs["L10_反查多页"], "A", "anchor", [1],
                                     page=p, page_size=100))
        _check(len(lk10) == SCALE_N and
               len({it["row_ref"] for it in lk10}) == SCALE_N and
               {it["seq"] for it in lk10} == set(range(SCALE_N)),
               "anchor 反查应覆盖全部结果且不重复")
        lk10b = service.lookup(runs["L10_反查多页"], "B", "scale", [7])
        _check(lk10b["total"] == 1 and lk10b["items"][0]["values"] == [1, 7, -3],
               "scale pk=7 只出现在一个结果中")
        out(f"  L10: anchor 反查 {len(lk10)} 项（100/页 × 25）✓；"
            f"scale pk=7 恰好 1 项 ✓")

        # ---- 5) 过滤/limit 淘汰不混入 ----
        out("\n[5] 过滤/limit 只保留实际出现，淘汰行不进追溯与反查")
        lk_elim = service.lookup(runs["L5_正负抵消与零贡献"], "A", "flows", [5])
        _check(lk_elim["total"] == 0, "被过滤的 flows pk=5 不得出现在 L5 反查")
        r10b = fetch_all_rows(service, runs["L10b_limit只保留实际出现"])
        _check([it["values"] for it in r10b] ==
               [[1, 1, 1, 1, -2], [1, 1, 2, 1, -1], [1, 1, 3, 1, 0]],
               "L10b 手写预期：前 3 行（连接全列）")
        for pk, expect in ((1, 1), (2, 1), (3, 1), (4, 0), (100, 0)):
            lk = service.lookup(runs["L10b_limit只保留实际出现"],
                                "B", "scale", [pk])
            _check(lk["total"] == expect,
                   f"limit 后 scale pk={pk} 反查应为 {expect} 项")
        out("  被过滤/limit 淘汰的源行反查均为 0 项 ✓")

        # ---- 6) 独立参照程序三方对照（全部查询） ----
        out("\n[6] 独立参照程序对照：结果/逐层展开/按源行反查")
        for name, qjson, _note in queries:
            big = name.startswith(("L9", "L10_"))
            check_run_vs_reference(
                service, runs[name], qjson, data, meta,
                expand_sample=8 if big else None,
                lookup_sample=200 if big else None)
            out(f"  {name}: 结果+展开+反查 与独立参照一致 ✓")

        # ---- 7) 血缘不是业务列 ----
        out("\n[7] 血缘信息不混作业务列")
        r1_cols = service.rows(runs["L1_主例_直接连接汇总"])["columns"]
        _check(r1_cols == ["s", "c"], "结果列只有业务列")
        bad = filt(eq(fld("row_ref"), lit_int(1)), src("A", "accounts"))
        try:
            coord.lineage_run(vid, bad, name="bad")
            raise AssertionError("按 row_ref 过滤必须被拒绝")
        except Exception as e:
            _check("row_ref" in str(e) or "未知列" in str(e),
                   f"应报未知列: {e}")
        out("  过滤/分组/去重只见业务列；按 row_ref 过滤被拒绝 ✓")

        # ---- 8) 证据写出失败不能冒充成功 ----
        out("\n[8] 证据写出中途失败：明确不完整")
        def fail_hook(stage, count):
            if stage == "edge" and count >= 1:
                raise OSError("模拟磁盘写失败（证据写出中途）")
        try:
            coord.lineage_run(vid, queries[0][1], name="L1_失败注入",
                              fail_hook=fail_hook)
            raise AssertionError("注入失败必须抛出")
        except LineageError as e:
            bad_run = e.run_id
            out(f"  失败 run: {bad_run}（{e}）")
        cat = {r["run_id"]: r for r in service.list_runs()}
        _check(cat[bad_run]["state"] == "error", "目录库必须标记 error")
        _check(cat[runs["L1_主例_直接连接汇总"]]["state"] == "ok",
               "正常 run 不受失败 run 影响")
        bad_dir = os.path.join(service.dir, bad_run)
        _check(os.path.exists(os.path.join(bad_dir, "INCOMPLETE.txt")),
               "失败目录必须有 INCOMPLETE.txt 标志")
        with open(os.path.join(bad_dir, "run.json"), encoding="utf-8") as f:
            _check(json.load(f)["state"] == "error", "run.json 必须标记 error")
        for fn in (lambda: service.rows(bad_run),
                   lambda: service.expand(bad_run + ":1"),
                   lambda: service.lookup(bad_run, "A", "accounts", [1])):
            try:
                fn()
                raise AssertionError("不完整 run 不得提供导航")
            except LineageStateError:
                pass
        out("  失败目录可识别（runs.db=error、run.json=error、INCOMPLETE.txt）；"
            "导航接口拒绝提供看似完整的血缘 ✓")

        # ---- 9) 核对可校验性 ----
        out("\n[9] 已完成 run 的查询/结果/源坐标可核对")
        shown = service.show(runs["L1_主例_直接连接汇总"])
        _check(shown["verified"] is True, "结果校验和必须可重算核对")
        _check(shown["sources"] == [
            {"source": "A", "instance_id": "inst-A-lin1", "at": 0},
            {"source": "B", "instance_id": "inst-B-lin1", "at": 0}],
            "源坐标（逻辑源/实例/读取版本）应完整留存")
        out(f"  L1 校验和重算一致 ✓；源坐标 {shown['sources']} ✓")

        # ---- 保存产物 ----
        summary = {"view_id": vid, "runs": {}, "failed_run": bad_run}
        for name, run_id in runs.items():
            first = service.rows(run_id, 0, 5)
            summary["runs"][name] = {"run_id": run_id, "state": "ok",
                                     "rows_out": first["total"],
                                     "first_page": first["items"]}
            with open(os.path.join(res_dir, f"{name}.rows.json"), "w",
                      encoding="utf-8") as f:
                json.dump(first, f, ensure_ascii=False, indent=1)
        artifacts = {
            "L1.expand_root": service.expand(r1[0]["row_ref"], 0, 10),
            "L2.expand_distinct": dx,
            "L3.expand_null_fill": ex3null,
            "L4b.expand_empty": ex4b,
            "L5.expand_cancel": ex5,
            "L7.lookup_self_join": service.lookup(
                runs["L7_同表两次引用"], "A", "flows", [1], page=0, page_size=10),
            "L9.expand_page0": service.expand(r9[0]["row_ref"], 0, 5),
            "L10.lookup_page0": service.lookup(runs["L10_反查多页"],
                                               "A", "anchor", [1],
                                               page=0, page_size=5),
        }
        for nm, obj in artifacts.items():
            with open(os.path.join(res_dir, f"{nm}.json"), "w",
                      encoding="utf-8") as f:
                json.dump(obj, f, ensure_ascii=False, indent=1)
        with open(os.path.join(res_dir, "summary.json"), "w",
                  encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=1)
        with open(os.path.join(workdir, "commands.txt"), "w",
                  encoding="utf-8") as f:
            f.write("# 复做导航（视图已关闭、源已停止，纯凭证据库）\n")
            for name, run_id in runs.items():
                f.write(f"python3 -m fedq lineage-rows --run-id {run_id} "
                        f"--workdir {coord.workdir} --page-size 5   # {name}\n")
            f.write(f"python3 -m fedq lineage-expand --row-ref "
                    f"{r1[0]['row_ref']} --workdir {coord.workdir}\n")
            f.write(f"python3 -m fedq lineage-lookup --run-id "
                    f"{runs['L7_同表两次引用']} --source A --table flows "
                    f"--pk 1 --workdir {coord.workdir}\n")
            f.write(f"python3 -m fedq lineage-runs --workdir {coord.workdir}\n")

        # ---- 10) 关闭视图、停源后仍能导航（新对象；脚本里是新进程） ----
        out("\n[10] 关闭原视图、停掉源进程：凭证据库继续导航")
        coord.close(vid)
        fleet.stop()
        service2 = LineageService(coord.workdir)   # 新对象=新进程角色
        r1b = fetch_all_rows(service2, runs["L1_主例_直接连接汇总"])
        _check([it["values"] for it in r1b] == [[20, 2]],
               "视图关闭、源停止后结果仍可读")
        ex2b = service2.expand(r2[0]["row_ref"], 0, 10)
        mem2b = ex2b["derivation"]["members"]["items"][0]
        dx2b = service2.expand(mem2b["row_ref"], 0, 10)
        _check(dx2b["derivation"]["alternatives"]["total"] == 2,
               "视图关闭后备选依据仍可展开")
        lk9b = service2.lookup(runs["L9_大组多页展开"], "B", "scale", [1234])
        _check(lk9b["total"] == 1, "视图关闭后反查仍可用")
        _check(service2.show(runs["L1_主例_直接连接汇总"])["verified"],
               "视图关闭后校验和仍可核对")
        out("  新对象在视图关闭、源进程停止后：读结果/展开/反查/核对全部可用 ✓")

        out("\n================ 血缘验收全部通过 ================")
        out(f"产物目录: {workdir}（结果与展开 results/、查询 queries/、"
            f"证据库 coord/lineage/<run_id>/、复做命令 commands.txt）")
        return {"view_id": vid, "runs": runs, "failed_run": bad_run,
                "workdir": workdir}
    finally:
        fleet.stop()
