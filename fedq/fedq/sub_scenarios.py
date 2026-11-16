"""持续查询订阅的可复做场景：单版本化源（订单 + 配送），运行时生成小夹具。

数据规模刻意小到能手写核对；规模数据（大量互不相关键）在演示运行时生成，
仓库只保留本夹具定义、测试与说明。

主验收线（订单 o1 属于客户 K、金额 10，初始无配送）：
  初始        orders: o1(K,10)                 deliveries: （空）
  提交1 加 d1(o1)     → left 出现一条匹配；count(*)=1,count(did)=1,sum=10
  提交2 加 d2(o1)     → 两条匹配；count(*)=2,count(did)=2,sum=20
  提交3 删 d1         → 回到一条匹配（d2）
  提交4 删 d2         → 最后一个匹配撤回，补回 left null 扩展行；count(did)=0,sum=null
  提交5 同笔：o1 金额改 15、删旧配送 d3、加新配送 d4（先在提交 5 之前插入 d3）
另含 null 值、正负抵消、连接键搬家、自连接、分组消失与空集所需的小数据。

所有提交 requires 均为空：本订阅不接跨源依赖，带非空 requires 的源登记即拒。
"""

from __future__ import annotations

import json
import os

from .versions import Change, Commit, SourceHistory

# ---------------------------------------------------------------- 表结构

TABLES = {
    "orders": {
        "columns": {"id": "text", "customer": "text", "amount": "int",
                    "grp": "text"},
        "key": ["id"],
    },
    "deliveries": {
        "columns": {"id": "text", "order_id": "text", "qty": "int"},
        "key": ["id"],
    },
    "parts": {
        "columns": {"id": "text", "pid": "text", "v": "int"},
        "key": ["id"],
    },
}

INSTANCE = "inst-od-0001"
SOURCE = "OD"


def subscription_history(head=None, earliest=0, instance=INSTANCE):
    """主验收历史：订单/配送的 8 笔提交，全部 requires={}。"""
    initial = {
        # o1 属于客户 K、金额 10；o2 用于分组/null/抵消；o3 用于键搬家
        "orders": [
            ["o1", "K", 10, "g1"],
            ["o2", "K2", 5, "g2"],
            ["o3", "K3", 7, "g2"],
        ],
        "deliveries": [],
        "parts": [
            ["p1", "a", 3],
            ["p2", "a", -3],     # 同 pid 正负抵消：sum=0 但有非 null 输入
            ["p3", "b", None],   # 全 null 输入：sum=null、count(v)=0
        ],
    }
    commits = [
        Commit(1, {
            "deliveries": Change("deliveries", upserts=[["d1", "o1", 1]])},
            {}, "添加第一条匹配配送 d1"),
        Commit(2, {
            "deliveries": Change("deliveries", upserts=[["d2", "o1", 2]])},
            {}, "添加第二条匹配配送 d2"),
        Commit(3, {
            "deliveries": Change("deliveries", deletes=["d1"])},
            {}, "删除其中一条配送 d1（d2 仍支持 o1）"),
        Commit(4, {
            "deliveries": Change("deliveries", deletes=["d2"])},
            {}, "删除最后一条配送：left 撤回匹配并补回 null 扩展行"),
        Commit(5, {
            "deliveries": Change("deliveries", upserts=[["d3", "o1", 9]])},
            {}, "重新加入 d3，为同笔替换做准备"),
        Commit(6, {
            # 同笔提交：订单金额 10→15、删旧配送 d3、加另一配送 d4，只交付最终净差
            "orders": Change("orders", upserts=[["o1", "K", 15, "g1"]]),
            "deliveries": Change("deliveries",
                                 upserts=[["d4", "o1", 4]], deletes=["d3"]),
        }, {}, "同笔改金额+删旧配送+加新配送"),
        Commit(7, {
            # 连接键搬家：o3 的 grp 从 g2 搬到 g1（影响自连接/分组连接的键）
            "orders": Change("orders", upserts=[["o3", "K3", 7, "g1"]])},
            {}, "连接键搬家：o3.grp g2→g1"),
        Commit(8, {
            # 分组消失：删除 o2（g2 唯一订单）；并制造 null 连接键行
            "orders": Change("orders",
                             upserts=[["o4", None, None, None]],
                             deletes=["o2"])},
            {}, "分组 g2 消失 + null 客户/金额/grp 行进入"),
    ]
    head = len(commits) if head is None else head
    return SourceHistory(SOURCE, instance, "B", TABLES, initial,
                         commits, head=head, earliest=earliest)


# ---------------------------------------------------------------- 查询构造

def fld(name):
    return {"k": "field", "name": name}


def src(table):
    return {"op": "source", "source": SOURCE, "table": table}


def proj(cols, inp):
    return {"op": "project", "cols": cols, "input": inp}


def join(kind, left, right, lkey, rkey):
    return {"op": "join", "kind": kind, "left": left, "right": right,
            "lkey": lkey, "rkey": rkey}


def agg(group_by, aggs, inp):
    return {"op": "aggregate", "group_by": group_by, "aggs": aggs, "input": inp}


def distinct(inp):
    return {"op": "distinct", "input": inp}


def filt(pred, inp):
    return {"op": "filter", "pred": pred, "input": inp}


def lit_int(v):
    return {"k": "lit", "t": "int", "v": str(v)}


def lt(a, b):
    return {"k": "lt", "a": a, "b": b}


def left_join_detail_query():
    """订单 LEFT JOIN 配送 ON id=order_id 的明细查询（带重数输出）。"""
    return proj(
        ["id", "customer", "amount", "r_id", "order_id", "qty"],
        join("left",
             proj(["id", "customer", "amount"], src("orders")),
             proj(["id", "order_id", "qty"], src("deliveries")),
             "id", "order_id"))


def left_join_agg_query():
    """在 left 连接明细上做全局 count(*)、count(配送id)、sum(金额)。"""
    return agg([], [
        {"f": "count_star", "out": "cnt_star"},
        {"f": "count", "field": "r_id", "out": "cnt_did"},
        {"f": "sum", "field": "amount", "out": "sum_amount"},
    ], join("left",
            proj(["id", "customer", "amount"], src("orders")),
            proj(["id", "order_id", "qty"], src("deliveries")),
            "id", "order_id"))


def distinct_sum_query():
    """inner 连接 → 投影金额 → distinct → 全局 sum。

    用 inner（不是 left）：避免补 null 行被误当成配送支持；删除一条支持时
    金额 10 仍由另一条配送支持，distinct 值不撤回；最后一条消失时才撤回。
    """
    matched = join("inner",
                   proj(["id", "amount"], src("orders")),
                   proj(["id", "order_id"], src("deliveries")),
                   "id", "order_id")
    amounts = proj(["amount"], matched)
    dedup = distinct(amounts)
    return agg([], [{"f": "sum", "field": "amount", "out": "sum_distinct"}],
                dedup)


def group_agg_query():
    """按 grp 分组的 count/sum：覆盖分组出现、键搬家与分组消失。"""
    return agg(["grp"], [
        {"f": "count_star", "out": "cnt"},
        {"f": "sum", "field": "amount", "out": "amt"},
    ], proj(["grp", "amount"], src("orders")))


def self_join_query():
    """orders 同表自连接（grp=grp）：同表不同输入位置、重数与 null 键。"""
    return proj(["id", "grp", "r_id"],
                join("inner",
                     proj(["id", "grp"], src("orders")),
                     proj(["id", "grp"], src("orders")),
                     "grp", "grp"))


def parts_group_query():
    """parts 按 pid 分组：正负抵消 sum=0、全 null 组 sum=null、空集对照。"""
    return agg(["pid"], [
        {"f": "count_star", "out": "cnt"},
        {"f": "count", "field": "v", "out": "cnt_v"},
        {"f": "sum", "field": "v", "out": "sum_v"},
    ], proj(["pid", "v"], src("parts")))


def empty_agg_query():
    """过滤后空集上的全局聚合：count(*)=0、count=0、sum=null 单行保留。"""
    return agg([], [
        {"f": "count_star", "out": "cnt_star"},
        {"f": "count", "field": "amount", "out": "cnt_amt"},
        {"f": "sum", "field": "amount", "out": "sum_amt"},
    ], filt(lt(lit_int(10 ** 12), fld("amount")),
            proj(["amount"], src("orders"))))


def subscription_queries():
    return [
        ("q_left明细", left_join_detail_query(),
         "订单 LEFT JOIN 配送明细（袋重数、null 补行）"),
        ("q_left聚合", left_join_agg_query(),
         "left 连接上的全局 count(*)/count(配送id)/sum(金额)"),
        ("q_distinct_sum", distinct_sum_query(),
         "inner 连接投影金额 distinct 再 sum（支持撤回语义）"),
        ("q_group", group_agg_query(),
         "按 grp 分组 count/sum（键搬家、分组消失、null 组）"),
        ("q_self_join", self_join_query(),
         "orders 同表自连接（同表两个输入位置）"),
        ("q_parts", parts_group_query(),
         "正负抵消 sum=0、全 null sum=null 的分组"),
        ("q_empty", empty_agg_query(),
         "空集全局聚合单行 count=0/sum=null"),
    ]


# ---------------------------------------------------------------- 规模数据

def scale_history(n_keys, head=None, earliest=0, instance=INSTANCE):
    """生成 n_keys 个互不相关键的大源，再加一笔只改一个键的提交。

    表 wide 每行一个唯一 join_key；最后一笔提交只 upsert 其中一行，
    用于证明增量维护只碰该键、不重扫全部关系。
    """
    schema = {
        "wide": {"columns": {"id": "int", "join_key": "text", "w": "int"},
                 "key": ["id"]},
        "dim": {"columns": {"id": "int", "join_key": "text", "d": "int"},
                "key": ["id"]},
    }
    initial = {
        "wide": [[i, f"k{i:08d}", i] for i in range(n_keys)],
        "dim": [[i, f"k{i:08d}", i * 2] for i in range(n_keys)],
    }
    commits = [
        Commit(1, {
            "wide": Change("wide", upserts=[[0, "k00000000", 999]])},
            {}, "只改 join_key=k00000000 这一个键"),
        Commit(2, {
            "dim": Change("dim", deletes=[1])},
            {}, "删除另一个键 k00000001 的右侧匹配"),
    ]
    head = len(commits) if head is None else head
    return SourceHistory(SOURCE, instance, "B", schema, initial,
                         commits, head=head, earliest=earliest)


def scale_join_query():
    return proj(["id", "join_key", "w", "r_id", "d"],
                join("inner",
                     proj(["id", "join_key", "w"], src("wide")),
                     proj(["id", "join_key", "d"], src("dim")),
                     "join_key", "join_key"))


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


def write_fixture_config(hist, url, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    hpath = os.path.join(out_dir, "OD.json")
    with open(hpath, "w", encoding="utf-8") as f:
        json.dump(history_config(hist), f, ensure_ascii=False, indent=1)
    cpath = os.path.join(out_dir, "sub_coordinator.json")
    with open(cpath, "w", encoding="utf-8") as f:
        json.dump({"sources": {hist.name: {"url": url}}}, f,
                  ensure_ascii=False, indent=1)
    qdir = os.path.join(out_dir, "queries")
    os.makedirs(qdir, exist_ok=True)
    for name, qjson, note in subscription_queries():
        with open(os.path.join(qdir, f"{name}.json"), "w",
                  encoding="utf-8") as f:
            json.dump(qjson, f, ensure_ascii=False, indent=1)
    return hpath, cpath, qdir
