"""可复做场景：三份独立版本源的提交历史与演示查询。

所有数据在运行时生成，不提交大数据文件。历史同时用于：
- 进程内版本服务器（version_server）；
- 导出 JSON 配置（供 serve-history 分进程启动、enumcheck 独立穷举）。

主例（ring）：H=(A:3,B:3,C:2)、L 全 0；
A 第 2 笔要求 B:3，B 第 2 笔要求 C:3，C 第 2 笔要求 A:2，其余无额外依赖，
手算与程序都应得到 F=(1,1,1)。每个源另外准备了尚未开放的提交，供发布推进演示，
其中第 4 笔在同一提交内对两张表同时 upsert 与 delete。
"""

from __future__ import annotations

import json
import os

from .versions import Change, Commit, SourceHistory

# ---------------------------------------------------------------- 表结构

SCHEMAS = {
    "A": {
        "items": {"columns": {"id": "int", "name": "text",
                              "amount": "int", "flag": "bool"}, "key": ["id"]},
        "ledger": {"columns": {"id": "int", "ref": "text", "amt": "int"},
                   "key": ["id"]},
    },
    "B": {
        "entries": {"columns": {"id": "int", "name": "text",
                                "amount": "int", "qty": "int"}, "key": ["id"]},
        "stock": {"columns": {"id": "int", "sku": "text", "qty": "int"},
                  "key": ["id"]},
    },
    "C": {
        "parts": {"columns": {"id": "int", "name": "text", "price": "int"},
                  "key": ["id"]},
        "supply": {"columns": {"id": "int", "part_id": "int", "qty": "int"},
                   "key": ["id"]},
    },
}


# ---------------------------------------------------------------- 主例历史

def _a_history(instance="inst-A-0001", head=3, earliest=0):
    initial = {
        "items": [[1, "a0", 10, True], [2, "b0", 20, False]],
        "ledger": [[1, "L0", 100]],
    }
    commits = [
        Commit(1, {
            "items": Change("items", upserts=[[1, "a1", 11, True],
                                              [3, "c1", 30, True]]),
            "ledger": Change("ledger", upserts=[[2, "L1", 200]]),
        }, {}, "同源两表插入/更新，无跨源依赖"),
        Commit(2, {
            "items": Change("items", upserts=[[2, "b2", 22, True]], deletes=[3]),
            "ledger": Change("ledger", upserts=[[3, "L2", 300]]),
        }, {"B": 3}, "要求 B:3；同笔更新 items 并删除 id=3"),
        Commit(3, {
            "items": Change("items", upserts=[[5, "e3", 50, True]]),
            "ledger": Change("ledger", upserts=[[4, "L3", 400]]),
        }, {}, "无额外依赖"),
        Commit(4, {  # 发布推进用：同笔跨表 upsert + delete
            "items": Change("items", upserts=[[1, "a4", 99, False]], deletes=[5]),
            "ledger": Change("ledger", upserts=[[1, "L4", 999]], deletes=[4]),
        }, {}, "同笔跨表更新 id=1 并删除 id=5（items）/id=4（ledger）"),
        Commit(5, {
            "items": Change("items", upserts=[[6, "f5", 60, True]]),
            "ledger": Change("ledger", upserts=[[5, "L5", 500]]),
        }, {}, "继续发布"),
    ]
    return SourceHistory("A", instance, "A", SCHEMAS["A"], initial,
                         commits, head=head, earliest=earliest)


def _b_history(instance="inst-B-0001", head=3, earliest=0):
    initial = {
        "entries": [[1, "x0", 1, 10], [2, "y0", 2, 20]],
        "stock": [[1, "SKU0", 7]],
    }
    commits = [
        Commit(1, {
            "entries": Change("entries", upserts=[[1, "x1", 1, 11],
                                                  [3, "z1", 3, 30]]),
            "stock": Change("stock", upserts=[[2, "SKU1", 8]]),
        }, {}, "无跨源依赖"),
        Commit(2, {
            "entries": Change("entries", upserts=[[4, "w2", 4, 40]], deletes=[3]),
            "stock": Change("stock", upserts=[[1, "SKU0b", 70]]),
        }, {"C": 3}, "要求 C:3（初始 head=2 时不可满足）"),
        Commit(3, {
            "entries": Change("entries", upserts=[[5, "v3", 5, 50]]),
            "stock": Change("stock", upserts=[[3, "SKU3", 9]]),
        }, {}, "无额外依赖"),
        Commit(4, {
            "entries": Change("entries", upserts=[[2, "y4", 2, 22]], deletes=[5]),
            "stock": Change("stock", upserts=[[4, "SKU4", 10]], deletes=[3]),
        }, {}, "同笔跨表更新 id=2、删除 id=5/3"),
        Commit(5, {
            "entries": Change("entries", upserts=[[6, "u5", 6, 60]]),
            "stock": Change("stock", upserts=[[5, "SKU5", 11]]),
        }, {}, "继续发布"),
    ]
    return SourceHistory("B", instance, "B", SCHEMAS["B"], initial,
                         commits, head=head, earliest=earliest)


def _c_history(instance="inst-C-0001", head=2, earliest=0):
    initial = {
        "parts": [[1, "p0", 100]],
        "supply": [[1, 1, 5]],
    }
    commits = [
        Commit(1, {
            "parts": Change("parts", upserts=[[1, "p1", 101], [2, "p2", 200]]),
            "supply": Change("supply", upserts=[[2, 2, 6]]),
        }, {}, "无跨源依赖"),
        Commit(2, {
            "parts": Change("parts", upserts=[[3, "p3", 300]], deletes=[2]),
            "supply": Change("supply", upserts=[[3, 1, 7]]),
        }, {"A": 2}, "要求 A:2；同笔删除 parts id=2"),
        Commit(3, {
            "parts": Change("parts", upserts=[[4, "p4", 400]]),
            "supply": Change("supply", upserts=[[4, 3, 8]]),
        }, {}, "开放后 B:2 的 C:3 前置才可得"),
        Commit(4, {
            "parts": Change("parts", upserts=[[1, "p1x", 150]], deletes=[4]),
            "supply": Change("supply", upserts=[[1, 1, 55]], deletes=[4]),
        }, {}, "同笔跨表更新 id=1、删除 id=4"),
    ]
    return SourceHistory("C", instance, "B", SCHEMAS["C"], initial,
                         commits, head=head, earliest=earliest)


def ring_histories(heads=None, earliests=None, instances=None):
    heads = heads or {"A": 3, "B": 3, "C": 2}
    earliests = earliests or {"A": 0, "B": 0, "C": 0}
    instances = instances or {}
    return {
        "A": _a_history(instances.get("A", "inst-A-0001"),
                        heads["A"], earliests["A"]),
        "B": _b_history(instances.get("B", "inst-B-0001"),
                        heads["B"], earliests["B"]),
        "C": _c_history(instances.get("C", "inst-C-0001"),
                        heads["C"], earliests["C"]),
    }


# ---------------------------------------------------------------- 无交叉依赖

def independent_histories():
    """无任何 requires，H 分量不同：最大合法向量就是 H 本身。"""
    hists = ring_histories(heads={"A": 3, "B": 2, "C": 1})
    for h in hists.values():
        for c in h.commits:
            c.requires = {}
    return hists


# ---------------------------------------------------------------- 相互要求（环）

def cycle_histories():
    """A:2 与 B:2 互相要求：环不拒绝，两笔已开放提交一起纳入 → F=(2,2)。"""
    h = ring_histories(heads={"A": 2, "B": 2, "C": 1})
    h["A"].commits[1].requires = {"B": 2}
    h["B"].commits[1].requires = {"A": 2}
    h["C"].commits[1].requires = {}
    return h


# ---------------------------------------------------------------- 配置导出

def history_config(hist):
    return {
        "name": hist.name, "instance_id": hist.instance_id,
        "dialect": hist.dialect, "tables": hist.schemas,
        "head": hist.head, "earliest": hist.earliest,
        "initial": hist.initial,
        "commits": [
            {"seq": c.seq, "requires": c.requires, "note": c.note,
             "changes": {t: {"upsert": ch.upserts, "delete": ch.deletes}
                         for t, ch in c.changes.items()}}
            for c in hist.commits],
    }


def write_configs(histories, urls, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    coord = {"sources": {}}
    for name, hist in histories.items():
        hcfg = history_config(hist)
        with open(os.path.join(out_dir, f"{name}.json"), "w",
                  encoding="utf-8") as f:
            json.dump(hcfg, f, ensure_ascii=False, indent=1)
        coord["sources"][name] = {"url": urls[name], "history": hcfg}
    with open(os.path.join(out_dir, "coordinator.json"), "w",
              encoding="utf-8") as f:
        json.dump(coord, f, ensure_ascii=False, indent=1)
    return os.path.join(out_dir, "coordinator.json")


def coordinator_config(urls):
    """进程内演示用：只需逻辑源名 → HTTP 地址。"""
    return {"sources": {n: {"url": u} for n, u in urls.items()}}


# ---------------------------------------------------------------- 演示查询

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


def src(source, table):
    return {"op": "source", "source": source, "table": table}


def filt(pred, inp):
    return {"op": "filter", "pred": pred, "input": inp}


def proj(cols, inp):
    return {"op": "project", "cols": cols, "input": inp}


def sort(keys, inp):
    return {"op": "sort",
            "keys": [{"field": f, "asc": a, "nulls_first": nf}
                     for f, a, nf in keys],
            "input": inp}


def limit(n, inp):
    return {"op": "limit", "n": n, "input": inp}


def join(kind, left, right, lkey, rkey):
    return {"op": "join", "kind": kind, "left": left, "right": right,
            "lkey": lkey, "rkey": rkey}


def agg(group_by, aggs, inp):
    return {"op": "aggregate", "group_by": group_by, "aggs": aggs, "input": inp}


def view_queries():
    """同一视图内的多份查询：不同表、不同列、不同过滤/连接/分页，全部绑定同一 F。"""
    qs = []
    # 1) 单源扫描 + 本地过滤 + 全序（A 方言文本差异不影响：过滤在本地按统一语义）
    qs.append(("q1_A_items过滤排序",
               sort([("id", True, False)],
                    filt(lt(lit_int(0), fld("amount")),
                         proj(["id", "name", "amount", "flag"],
                              src("A", "items")))),
               "A.items 截至 F[A] 的行，本地统一语义过滤并按 id 排序"))
    # 2) 跨源 inner 等值连接（不同表、不同列）；先排序再投影以保留全序
    qs.append(("q2_A_B连接",
               proj(["id", "name", "amount", "r_name", "qty"],
                    sort([("id", True, False), ("r_id", True, False)],
                         join("inner",
                              proj(["id", "name", "amount"], src("A", "items")),
                              proj(["id", "name", "amount", "qty"],
                                   src("B", "entries")),
                              "amount", "amount"))),
               "A.items ⋈ B.entries ON amount：连接结果必须对应同一 F"))
    # 3) 第三个源、另一张表 + limit（分页演示用）
    qs.append(("q3_C_parts扫描",
               limit(50, sort([("id", True, False)],
                              proj(["id", "name", "price"], src("C", "parts")))),
               "C.parts 全量排序后 limit；小批读取制造多页"))
    # 4) 同源第二张表 + 聚合，证明视图冻结覆盖同源多表
    qs.append(("q4_B_stock聚合",
               agg([], [{"f": "count_star", "out": "c"},
                        {"f": "sum", "field": "qty", "out": "sq"}],
                   proj(["id", "qty"], src("B", "stock"))),
               "B.stock 的 count/sum，验证同笔多表与删除行状态"))
    # 5) left 连接保留左侧全部行
    # 5) left 连接保留左侧全部行（ledger 同表自连接，键为非唯一 amt，验证补 null 路径）
    qs.append(("q5_left连接",
               proj(["id", "ref", "amt", "r_id"],
                    sort([("id", True, False), ("r_id", True, False)],
                         join("left",
                              proj(["id", "ref", "amt"], src("A", "ledger")),
                              proj(["id", "amt"], src("A", "ledger")),
                              "amt", "amt"))),
               "left 连接（同源自连接），不匹配补 null 的路径"))
    return qs
