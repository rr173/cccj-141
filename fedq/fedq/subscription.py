"""持续查询订阅：登记一次查询，持续维护会变化的关系结果。

定位（与既有功能的边界）：
- 这是**真正的增量关系维护**：每笔源提交按 upsert/delete 转成袋重数差分，
  沿查询树自底向上传播，只重算受影响的连接键/分组；不是定时重跑查询、
  不是比较两份完整文件、不保存旧读取视图，也不解释结果血缘。
- 只接**一个**已有版本化 HTTP 源（可查它的多张表）；不协调跨源依赖，
  任何提交带非空 requires 的源登记时直接拒绝。
- 普通联查、读取批（视图）、血缘入口保持原样，本模块不改动它们的路径。

支持的查询树节点：source / filter / project / join(inner|left 等值) / distinct /
aggregate（count(*)、count(字段)、整数 sum；聚合只允许在最外层）。
sort/limit 不支持，登记时显式报错，绝不静默忽略。

结果是**袋**：业务值相同的行带重数（mult），对外给完整有类型业务行 + 重数，
不延续单次执行的 row_ref。维护状态全部在 m_ 前缀的内部表里，业务字段名不被占用。

增量策略（每节点维护的本地状态，全部落 SQLite）：
- source 节点：m_src_<nid>，主键 → 当前完整行（初始化读一次真实快照，之后只改变更键）；
- 普通关系节点：m_bag_<nid>，业务行 jrow → 当前重数；
- join 节点：额外维护两侧连接键索引 m_join_<nid>，一笔提交只重算差分涉及的键，
  不相关键一行不碰；left 的 null 补行随最后一个/第一个匹配真实撤回与补回；
- distinct 节点：m_distinct_<nid> 记每个值的输入支持数，delete 到 0 才撤回该值；
- aggregate 节点（仅根）：m_agg_<nid>_idx 按分组键维护成员索引，只重算受影响分组；
  区分“贡献和恰好为 0”“有行但全 null（sum=null）”“组已无输入”，全局空集保留单行。

原子性：每笔提交的传播、结果袋更新、变更台账与已处理位置在**一个 SQLite 事务**里
共同落定；进程被杀死后重启，要么停在旧位置+旧完整结果，要么在新位置+完整新变更，
不会有半笔推进，再次推进不重复追加。
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time

from . import query as Q
from .coordinator import VersionClient, frozen_caps

# 仅在显式崩溃演示时使用：FEDQ_SUB_CRASH=hook:sub_id，命中后 os._exit(9)。
CRASH_ENV = "FEDQ_SUB_CRASH"


class SubscriptionError(Exception):
    """订阅操作的普通失败（参数非法、目标提交未开放等），不改变订阅状态。"""


class SubscriptionRebuild(SubscriptionError):
    """订阅进入 needs_rebuild：历史断档或源 instance_id 变更，必须重新建立。"""


class SubscriptionClosed(SubscriptionError):
    """订阅已停止：拒绝推进；读取类操作仍允许。"""


def _now():
    return time.strftime("%Y-%m-%dT%H:%M:%S")


def jenc(row):
    """业务行的规范 JSON 文本（列顺序由 schema 固定，None/bool/int 精确保留）。"""
    return json.dumps(list(row), ensure_ascii=False, separators=(",", ":"))


def jdec(jrow):
    return json.loads(jrow)


# ---------------------------------------------------------------- 查询树分析

class NodeInfo:
    def __init__(self, nid, path, node, children):
        self.nid = nid                # 后序整数 id
        self.path = path              # '' 根；'/0' 一元输入；'/l'、'/r' 连接输入
        self.node = node              # 已通过 validate 的 Q.Node
        self.children = children      # [NodeInfo]
        self.cols = node.schema.names()
        self.types = [t for _, t in node.schema.cols]

    @property
    def op(self):
        return self.node.op


def analyze(qnode):
    """后序编号并做订阅专属校验，返回 (infos 按 nid 升序, root_info)。"""
    infos = []

    def walk(node, path):
        children = []
        if isinstance(node, Q.Join):
            children.append(walk(node.left, path + "/l"))
            children.append(walk(node.right, path + "/r"))
        elif node.inputs():
            children.append(walk(node.inputs()[0], path + "/0"))
        nid = len(infos)
        info = NodeInfo(nid, path, node, children)
        infos.append(info)
        return info

    root = walk(qnode, "")

    def check(node, is_root):
        if isinstance(node, (Q.Sort, Q.Limit)):
            raise SubscriptionError(
                f"持续查询订阅不支持 sort/limit（节点 {node.op}），登记拒绝；"
                "需要排序请在下游报表侧对读取结果自行排序")
        if isinstance(node, Q.Aggregate) and not is_root:
            raise SubscriptionError(
                "持续查询订阅的聚合只能位于查询树最外层，嵌套聚合登记拒绝")
        for child in node.inputs():
            check(child, False)

    check(qnode, True)
    sources = [i for i in infos if isinstance(i.node, Q.Source)]
    source_names = {i.node.source for i in sources}
    if len(source_names) != 1:
        raise SubscriptionError(
            f"持续查询订阅只接一个版本化源，查询引用了 {sorted(source_names)}，"
            "跨源依赖不在支持范围，登记拒绝")
    tables = sorted({i.node.table for i in sources})
    return infos, root, sources, next(iter(source_names)), tables


# ---------------------------------------------------------------- 订阅存储（目录库 + 每订阅状态库）

class SubscriptionStore:
    def __init__(self, workdir):
        self.workdir = workdir
        self.dir = os.path.join(workdir, "subscriptions")
        os.makedirs(self.dir, exist_ok=True)
        self.registry_path = os.path.join(workdir, "subscriptions.db")
        self.lock = threading.RLock()
        self.reg = sqlite3.connect(self.registry_path, check_same_thread=False)
        self.reg.row_factory = sqlite3.Row
        with self.reg:
            self.reg.executescript(
                """
                CREATE TABLE IF NOT EXISTS subscriptions(
                    sub_id TEXT PRIMARY KEY,
                    state TEXT NOT NULL,          -- building|active|needs_rebuild|stopped
                    created TEXT, stopped TEXT,
                    source TEXT, url TEXT, instance_id TEXT,
                    position INTEGER, init_at INTEGER, head INTEGER,
                    query_json TEXT, cols TEXT, tables TEXT,
                    schema_json TEXT, error TEXT
                );
                """)

    def state_db_path(self, sub_id):
        return os.path.join(self.dir, f"{sub_id}.state.db")

    def create_registry(self, sub_id, source, url, instance_id, query_json,
                        cols, tables, schema_json, init_at=None):
        with self.lock, self.reg:
            self.reg.execute(
                "INSERT INTO subscriptions(sub_id,state,created,source,url,"
                "instance_id,position,init_at,head,query_json,cols,tables,"
                "schema_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (sub_id, "building", _now(), source, url, instance_id, None,
                 init_at, None, json.dumps(query_json, ensure_ascii=False),
                 json.dumps(cols, ensure_ascii=False), json.dumps(tables),
                 json.dumps(schema_json, ensure_ascii=False)))

    def get(self, sub_id):
        with self.lock:
            row = self.reg.execute("SELECT * FROM subscriptions WHERE sub_id=?",
                                   (sub_id,)).fetchone()
            if row is None:
                raise SubscriptionError(f"未知订阅: {sub_id}")
            return dict(row)

    def list_all(self):
        with self.lock:
            return [dict(r) for r in self.reg.execute(
                "SELECT sub_id,state,created,stopped,source,instance_id,position,"
                "init_at,head,error FROM subscriptions ORDER BY rowid")]

    def update(self, sub_id, **fields):
        if not fields:
            return
        sets = ", ".join(f"{k}=?" for k in fields)
        with self.lock, self.reg:
            self.reg.execute(f"UPDATE subscriptions SET {sets} WHERE sub_id=?",
                             (*fields.values(), sub_id))

    def list_building(self):
        with self.lock:
            return [dict(r) for r in self.reg.execute(
                "SELECT * FROM subscriptions WHERE state='building'")]

    def open_state_db(self, sub_id):
        con = sqlite3.connect(self.state_db_path(sub_id))
        con.row_factory = sqlite3.Row
        # 同步完整落盘 + 回滚日志：进程被杀后事务要么完整可见要么整体回滚。
        con.execute("PRAGMA synchronous=FULL")
        con.execute("PRAGMA foreign_keys=ON")
        return con


# ---------------------------------------------------------------- 增量维护引擎

class SubscriptionEngine:
    """一个订阅一个实例；状态全部在 SQLite，进程重启后凭磁盘继续。"""

    def __init__(self, store: SubscriptionStore, record: dict, client: VersionClient):
        self.store = store
        self.rec = record
        self.sub_id = record["sub_id"]
        self.client = client
        self.con = store.open_state_db(self.sub_id)
        self.query_json = json.loads(record["query_json"])
        self.schema_json = json.loads(record["schema_json"])
        caps = frozen_caps("B", record["source"], self.schema_json)
        # 方言只影响能力声明；本引擎全部本地按统一语义求值，方言不参与答案。
        self.qnode = Q.from_json(self.query_json)
        self.qnode.validate({record["source"]: caps})
        self.infos, self.root, self.source_infos, self.source_name, self.tables = \
            analyze(self.qnode)
        self.cols = self.root.cols
        self._pred_cache = {}

    def close(self):
        self.con.close()

    # ------------------------------------------------ 元信息
    def _meta_get(self, key, default=None):
        row = self.con.execute("SELECT value FROM m_meta WHERE key=?",
                               (key,)).fetchone()
        return default if row is None else json.loads(row["value"])

    def _meta_put(self, key, value):
        self.con.execute(
            "INSERT INTO m_meta(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, json.dumps(value, ensure_ascii=False)))

    def _table(self, kind, info):
        return f"m_{kind}_{info.nid}"

    def _pred(self, info):
        if info.nid not in self._pred_cache:
            self._pred_cache[info.nid] = info.node.pred
        return self._pred_cache[info.nid]

    def _row_dict(self, info, row):
        return dict(zip(info.cols, row))

    # ------------------------------------------------ 建库 DDL
    def init_schema(self):
        ddl = [
            "CREATE TABLE m_meta(key TEXT PRIMARY KEY, value TEXT)",
            "CREATE TABLE m_nodes(nid INTEGER PRIMARY KEY, path TEXT UNIQUE,"
            " op TEXT, detail TEXT, cols TEXT)",
            "CREATE TABLE m_diag(seq INTEGER PRIMARY KEY, detail TEXT NOT NULL)",
        ]
        for info in self.infos:
            if isinstance(info.node, Q.Source):
                ddl.append(
                    f"CREATE TABLE {self._table('src', info)}"
                    "(pk TEXT PRIMARY KEY, jrow TEXT NOT NULL)")
            else:
                ddl.append(
                    f"CREATE TABLE {self._table('bag', info)}"
                    "(jrow TEXT PRIMARY KEY, mult INTEGER NOT NULL)")
            if isinstance(info.node, Q.Join):
                ddl.append(
                    f"CREATE TABLE {self._table('join', info)}"
                    "(side TEXT NOT NULL, jkey TEXT NOT NULL, jrow TEXT NOT NULL,"
                    " mult INTEGER NOT NULL, PRIMARY KEY(side,jkey,jrow))")
                ddl.append(
                    f"CREATE INDEX {self._table('join', info)}_k ON "
                    f"{self._table('join', info)}(side,jkey)")
            if isinstance(info.node, Q.Distinct):
                ddl.append(
                    f"CREATE TABLE {self._table('distinct', info)}"
                    "(jrow TEXT PRIMARY KEY, support INTEGER NOT NULL)")
            if isinstance(info.node, Q.Aggregate):
                ddl.append(
                    f"CREATE TABLE {self._table('aggidx', info)}"
                    "(gkey TEXT NOT NULL, jrow TEXT NOT NULL, mult INTEGER NOT NULL)")
                ddl.append(
                    f"CREATE INDEX {self._table('aggidx', info)}_g ON "
                    f"{self._table('aggidx', info)}(gkey)")
                # 仍存活的分组：gkey -> 当前业务行（精确查找，不靠 LIKE 前缀）
                ddl.append(
                    f"CREATE TABLE {self._table('aggstate', info)}"
                    "(gkey TEXT PRIMARY KEY, jrow TEXT NOT NULL)")
        ddl.append(
            "CREATE TABLE m_changes(id INTEGER PRIMARY KEY AUTOINCREMENT,"
            " seq INTEGER NOT NULL, kind TEXT NOT NULL,"
            " jrow TEXT, delta INTEGER, ord INTEGER NOT NULL)")
        ddl.append("CREATE INDEX m_changes_cursor ON m_changes(seq,id)")
        self.con.executescript(";".join(ddl))
        for info in self.infos:
            self.con.execute(
                "INSERT INTO m_nodes(nid,path,op,detail,cols) VALUES(?,?,?,?,?)",
                (info.nid, info.path, info.op,
                 json.dumps(info.node.to_json(), ensure_ascii=False),
                 json.dumps(info.cols, ensure_ascii=False)))

    # ------------------------------------------------ 初始化（明确位置 p 的真实快照）
    def build_initial(self, position, snapshot_pages, diag):
        """snapshot_pages: 函数(table, page)->响应 dict（真实 HTTP 分页读取）。"""
        self.init_schema()
        with self.con:
            self._load_sources(position, snapshot_pages, diag)
            for info in self.infos:
                if isinstance(info.node, Q.Source):
                    continue
                self._build_node(info, diag)
            # 初始结果的可读边界：位置 p 的完成标记（无差分）
            self.con.execute(
                "INSERT INTO m_changes(seq,kind,jrow,delta,ord) "
                "VALUES(?,?,?,?,?)", (position, "boundary", None, None, 0))
            self.con.execute(
                "INSERT INTO m_diag(seq,detail) VALUES(?,?)",
                (position, json.dumps({"init": True, **diag}, ensure_ascii=False)))
            self._meta_put("position", position)
            self._meta_put("cumulative", self._zero_cum())

    def _load_sources(self, position, snapshot_pages, diag):
        # 同一张表可能在查询树的不同输入位置出现多次（自连接）：每个 source 节点
        # 都有独立的 m_src_<nid> 状态表，按节点而不是按表名各读一份快照。
        for info in self.source_infos:
            tbl = self._table("src", info)
            page = 0
            total = 0
            while True:
                resp = snapshot_pages(info.node.table, page)
                if resp["at"] != position:
                    raise SubscriptionError(
                        f"初始化快照版本漂移：期望 {position}，实际 {resp['at']}")
                rows = resp["rows"]
                self.con.executemany(
                    f"INSERT INTO {tbl}(pk,jrow) VALUES(?,?)",
                    [(self._pk_of(info, r), jenc(r)) for r in rows])
                total += len(rows)
                diag["http_rows"] = diag.get("http_rows", 0) + len(rows)
                if resp["next_page"] is None:
                    break
                page = resp["next_page"]
            diag.setdefault("source_rows", {})[
                f"{info.path}:{info.node.table}"] = total

    def _pk_of(self, info, row):
        sch = self.schema_json[info.node.table]
        cols = list(sch["columns"].keys())
        keycols = sch.get("key", ["id"])
        return jenc([row[cols.index(k)] for k in keycols])

    def _build_node(self, info, diag):
        node = info.node
        if isinstance(node, Q.Filter):
            child_info = info.children[0]
            pred = self._pred(info)
            self._bag_from_select(
                info, child_info, diag,
                lambda r, p=pred, cc=child_info:
                (p.eval(dict(zip(cc.cols, r))) is True, r))
        elif isinstance(node, Q.Project):
            child_info = info.children[0]
            keep = [child_info.cols.index(c) for c in node.cols]
            self._bag_from_select(
                info, child_info, diag,
                lambda r, keep=keep: (True, [r[i] for i in keep]))
        elif isinstance(node, Q.Join):
            self._build_join(info, diag)
        elif isinstance(node, Q.Distinct):
            self._build_distinct(info, diag)
        elif isinstance(node, Q.Aggregate):
            self._build_aggregate(info, diag)
        else:
            raise SubscriptionError(f"初始化未支持的节点 {node.op}")

    def _bag_from_select(self, info, child_info, diag, fn):
        """扫描子节点当前袋一次，fn(row)->(keep, out_row)，按重数写入本节点袋。"""
        out_tbl = self._table("bag", info)
        seen = {}
        for jrow, mult in self._iter_child(child_info):
            keep, out = fn(jdec(jrow))
            diag["rows_read"] = diag.get("rows_read", 0) + 1
            if not keep:
                continue
            oj = jenc(out)
            seen[oj] = seen.get(oj, 0) + mult
        self.con.executemany(
            f"INSERT INTO {out_tbl}(jrow,mult) VALUES(?,?)",
            list(seen.items()))
        diag["rows_written"] = diag.get("rows_written", 0) + len(seen)

    def _iter_child(self, child_info):
        if isinstance(child_info.node, Q.Source):
            tbl = self._table("src", child_info)
            return ((r["jrow"], 1) for r in self.con.execute(
                f"SELECT jrow FROM {tbl}"))
        tbl = self._table("bag", child_info)
        return ((r["jrow"], r["mult"]) for r in self.con.execute(
            f"SELECT jrow,mult FROM {tbl}"))

    def _build_join(self, info, diag):
        node = info.node
        li, ri = info.children
        jtbl = self._table("join", info)
        for side, cinfo, keyfield in (
                ("l", li, node.lkey), ("r", ri, node.rkey)):
            batch = []
            for jrow, mult in self._iter_child(cinfo):
                row = jdec(jrow)
                key = row[cinfo.cols.index(keyfield)]
                batch.append((side, jenc([key]), jrow, mult))
                diag["rows_read"] = diag.get("rows_read", 0) + 1
            self.con.executemany(
                f"INSERT INTO {jtbl}(side,jkey,jrow,mult) VALUES(?,?,?,?)", batch)
        # 初始化：全部键各重算一次，写入输出袋
        btbl = self._table("bag", info)
        keys = {r["jkey"] for r in self.con.execute(
            f"SELECT DISTINCT jkey FROM {jtbl} WHERE side='l'")}
        batch = []
        for jkey in keys:
            for oj, m in self._join_outputs_for_key(info, jkey).items():
                batch.append((oj, m))
        self.con.executemany(
            f"INSERT INTO {btbl}(jrow,mult) VALUES(?,?)", batch)
        diag["rows_written"] = diag.get("rows_written", 0) + len(batch)

    def _join_rows(self, info, side, jkey):
        jtbl = self._table("join", info)
        return [(jdec(r["jrow"]), r["mult"]) for r in self.con.execute(
            f"SELECT jrow,mult FROM {jtbl} WHERE side=? AND jkey=?",
            (side, jkey))]

    def _join_outputs_for_key(self, info, jkey):
        """给定键的完整提交后输出袋（dict 业务行 -> 重数），含 left null 补行。"""
        node = info.node
        left_rows = self._join_rows(info, "l", jkey)
        right_rows = self._join_rows(info, "r", jkey)
        out = {}
        key_is_null = jdec(jkey)[0] is None
        for lrow, lm in left_rows:
            hit = False
            if not key_is_null:
                for rrow, rm in right_rows:
                    hit = True
                    oj = jenc(lrow + rrow)
                    out[oj] = out.get(oj, 0) + lm * rm
            if node.kind == "left" and not hit:
                oj = jenc(lrow + [None] * len(info.children[1].cols))
                out[oj] = out.get(oj, 0) + lm
        return out

    def _build_distinct(self, info, diag):
        child = info.children[0]
        dtbl = self._table("distinct", info)
        btbl = self._table("bag", info)
        supp = {}
        for jrow, mult in self._iter_child(child):
            supp[jrow] = supp.get(jrow, 0) + mult
            diag["rows_read"] = diag.get("rows_read", 0) + 1
        self.con.executemany(
            f"INSERT INTO {dtbl}(jrow,support) VALUES(?,?)",
            list(supp.items()))
        self.con.executemany(
            f"INSERT INTO {btbl}(jrow,mult) VALUES(?,1)",
            [(j,) for j in supp])
        diag["rows_written"] = diag.get("rows_written", 0) + len(supp)

    def _build_aggregate(self, info, diag):
        node = info.node
        child = info.children[0]
        itbl = self._table("aggidx", info)
        stbl = self._table("aggstate", info)
        btbl = self._table("bag", info)
        gcols = [child.cols.index(g) for g in node.group_by]
        idx_batch, groups = [], set()
        for jrow, mult in self._iter_child(child):
            row = jdec(jrow)
            gkey = jenc([row[i] for i in gcols])
            idx_batch.append((gkey, jrow, mult))
            groups.add(gkey)
            diag["rows_read"] = diag.get("rows_read", 0) + 1
        self.con.executemany(
            f"INSERT INTO {itbl}(gkey,jrow,mult) VALUES(?,?,?)", idx_batch)
        state_batch, bag_batch = [], []
        for gkey in sorted(groups):
            out_jrow = jenc(self._agg_row(info, gkey))
            state_batch.append((gkey, out_jrow))
            bag_batch.append((out_jrow, 1))
        if not node.group_by:
            # 全局聚合只有一个组（空组键）：无输入时也要保留空集单行；
            # 有输入时上面已建过该组，不重复插入
            gkey = jenc([])
            if gkey not in groups:
                out_jrow = jenc(self._agg_row(info, gkey))
                state_batch.append((gkey, out_jrow))
                bag_batch.append((out_jrow, 1))
        self.con.executemany(
            f"INSERT INTO {stbl}(gkey,jrow) VALUES(?,?)", state_batch)
        self.con.executemany(
            f"INSERT INTO {btbl}(jrow,mult) VALUES(?,?)", bag_batch)
        diag["rows_written"] = diag.get("rows_written", 0) + len(bag_batch)

    def _agg_row(self, info, gkey):
        """只按分组键读成员索引，重算一个组的完整业务行（不扫其他组）。"""
        node = info.node
        child = info.children[0]
        members = self.con.execute(
            f"SELECT jrow,mult FROM {self._table('aggidx', info)} WHERE gkey=?",
            (gkey,)).fetchall()
        gvals = jdec(gkey)
        row = list(gvals)
        for agg in node.aggs:
            if agg.f == "count_star":
                row.append(sum(r["mult"] for r in members))
                continue
            fidx = child.cols.index(agg.field)
            count, total, has_nonnull = 0, 0, False
            for r in members:
                m = r["mult"]
                v = jdec(r["jrow"])[fidx]
                if v is not None:
                    has_nonnull = True
                    count += m
                    if agg.f == "sum":
                        total += v * m
            if agg.f == "count":
                row.append(count)
            else:  # sum：有非 null 输入才给整数和（哪怕恰好为 0），否则 null
                row.append(total if has_nonnull else None)
        return row

    # ------------------------------------------------ 单提交增量传播
    def apply_commit(self, commit, http_requests):
        """把一笔完整提交传播进本地状态；全部在一个事务内共同落定。"""
        seq = commit["seq"]
        if commit.get("requires"):
            # 登记时已拒，防御性再查：绝不悄悄忽略跨源依赖
            raise SubscriptionError(
                f"提交 {seq} 带非空 requires {commit['requires']}，订阅不支持")
        diag = {"http_requests": http_requests, "seq": seq,
                "src_pks": [], "nodes": {}}
        deltas = {i.nid: {} for i in self.infos}   # nid -> jrow -> 净重数差
        crash = self._crash_point()
        with self.con:
            # 1) 源节点：只碰变更主键，产出旧行 -1 / 新行 +1
            for info in self.source_infos:
                ch = commit["changes"].get(info.node.table)
                if ch is None:
                    continue
                self._source_deltas(info, ch, deltas[info.nid], diag)
            # 2) 自底向上传播（infos 已是后序）
            for info in self.infos:
                if isinstance(info.node, Q.Source):
                    continue
                self._propagate(info, deltas, diag)
            # 3) 根袋差分落盘 + 变更台账（同事务）。
            #    join/distinct/aggregate 在各自 _propagate 内已直接维护结果袋，
            #    这里只对“未自行落袋”的根（filter/project）按净差调整，避免重复施加。
            root_deltas = deltas[self.root.nid]
            if isinstance(self.root.node, (Q.Filter, Q.Project)):
                root_nd = diag["nodes"].setdefault(str(self.root.nid),
                                                   {"op": self.root.op,
                                                    "rows_read": 0,
                                                    "rows_written": 0})
                self._apply_bag_deltas(self.root, root_deltas, root_nd)
            order = 0
            for jrow in sorted(root_deltas):
                d = root_deltas[jrow]
                if d == 0:
                    continue
                self.con.execute(
                    "INSERT INTO m_changes(seq,kind,jrow,delta,ord) "
                    "VALUES(?,?,?,?,?)", (seq, "delta", jrow, d, order))
                order += 1
            # 无结果变化的提交也留下可识别的完成边界
            self.con.execute(
                "INSERT INTO m_changes(seq,kind,jrow,delta,ord) "
                "VALUES(?,?,?,?,?)", (seq, "boundary", None, None, order))
            self.con.execute(
                "INSERT INTO m_diag(seq,detail) VALUES(?,?)",
                (seq, json.dumps(diag, ensure_ascii=False)))
            cum = self._meta_get("cumulative", self._zero_cum())
            cum["commits"] += 1
            cum["http_requests"] += http_requests
            cum["src_pks_touched"] += len(diag["src_pks"])
            cum["join_keys_recomputed"] += sum(
                len(v.get("join_keys", []))
                for v in diag["nodes"].values())
            cum["groups_recomputed"] += sum(
                v.get("groups_recomputed", 0) for v in diag["nodes"].values())
            cum["rows_read"] += sum(
                v.get("rows_read", 0) for v in diag["nodes"].values())
            cum["rows_written"] += sum(
                v.get("rows_written", 0) for v in diag["nodes"].values())
            self._meta_put("position", seq)
            self._meta_put("cumulative", cum)
            if crash == "before_commit":
                os._exit(9)
        return {"seq": seq,
                "changes": [{"row": jdec(j), "delta": root_deltas[j]}
                            for j in sorted(root_deltas) if root_deltas[j] != 0],
                "diagnostics": diag, "cumulative": cum}

    def _crash_point(self):
        spec = os.environ.get(CRASH_ENV, "")
        if not spec:
            return None
        hook, _, target = spec.partition(":")
        if target == self.sub_id:
            return hook
        return None

    @staticmethod
    def _zero_cum():
        return {"commits": 0, "http_requests": 0, "src_pks_touched": 0,
                "join_keys_recomputed": 0, "groups_recomputed": 0,
                "rows_read": 0, "rows_written": 0}

    def _source_deltas(self, info, change, out, diag):
        tbl = self._table("src", info)
        upserts = {self._pk_of(info, r): r for r in change.get("upsert", [])}
        deletes = set(jenc([rid]) if not isinstance(rid, list) else jenc(rid)
                       for rid in change.get("delete", []))
        affected = set(upserts) | deletes
        for pk in sorted(affected):
            diag["src_pks"].append({"nid": info.nid, "pk": jdec(pk)})
            row = self.con.execute(
                f"SELECT jrow FROM {tbl} WHERE pk=?", (pk,)).fetchone()
            old = None if row is None else jdec(row["jrow"])
            # 与 versions.state_at 同序：upsert 覆盖后 delete 生效，同键 delete 胜出
            new = None if pk in deletes else upserts.get(pk)
            if new is not None:
                self.con.execute(
                    f"INSERT INTO {tbl}(pk,jrow) VALUES(?,?) "
                    "ON CONFLICT(pk) DO UPDATE SET jrow=excluded.jrow",
                    (pk, jenc(new)))
            else:
                self.con.execute(f"DELETE FROM {tbl} WHERE pk=?", (pk,))
            if old is not None and (new is None or jenc(old) != jenc(new)):
                out[jenc(old)] = out.get(jenc(old), 0) - 1
            if new is not None and (old is None or jenc(old) != jenc(new)):
                out[jenc(new)] = out.get(jenc(new), 0) + 1

    def _propagate(self, info, deltas, diag):
        node = info.node
        nd = diag["nodes"].setdefault(str(info.nid),
                                      {"op": node.op, "rows_read": 0,
                                       "rows_written": 0})
        child_deltas = [deltas[c.nid] for c in info.children]
        if isinstance(node, Q.Filter):
            pred = self._pred(info)
            child = info.children[0]
            for jrow, d in child_deltas[0].items():
                if d == 0:
                    continue
                row = jdec(jrow)
                nd["rows_read"] += 1
                if pred.eval(dict(zip(child.cols, row))) is True:
                    self._add_delta(deltas[info.nid], jrow, d)
        elif isinstance(node, Q.Project):
            child = info.children[0]
            idxs = [child.cols.index(c) for c in node.cols]
            for jrow, d in child_deltas[0].items():
                if d == 0:
                    continue
                row = jdec(jrow)
                nd["rows_read"] += 1
                self._add_delta(deltas[info.nid],
                                jenc([row[i] for i in idxs]), d)
        elif isinstance(node, Q.Join):
            self._propagate_join(info, deltas, nd)
        elif isinstance(node, Q.Distinct):
            self._propagate_distinct(info, deltas, nd)
        elif isinstance(node, Q.Aggregate):
            self._propagate_aggregate(info, deltas, nd)
        else:
            raise SubscriptionError(f"增量传播未支持的节点 {node.op}")

    @staticmethod
    def _add_delta(bucket, jrow, d):
        bucket[jrow] = bucket.get(jrow, 0) + d

    def _apply_bag_deltas(self, info, bucket, nd=None):
        tbl = self._table("bag", info)
        writes = 0
        for jrow, d in bucket.items():
            if d == 0:
                continue
            row = self.con.execute(
                f"SELECT mult FROM {tbl} WHERE jrow=?", (jrow,)).fetchone()
            old = 0 if row is None else row["mult"]
            new = old + d
            if new < 0:
                raise SubscriptionError(
                    f"内部错误：节点 {info.path} 行 {jrow} 重数减为负 {new}")
            if new == 0:
                self.con.execute(f"DELETE FROM {tbl} WHERE jrow=?", (jrow,))
            elif row is None:
                self.con.execute(
                    f"INSERT INTO {tbl}(jrow,mult) VALUES(?,?)", (jrow, new))
            else:
                self.con.execute(
                    f"UPDATE {tbl} SET mult=? WHERE jrow=?", (new, jrow))
            writes += 1
        if nd is not None:
            nd["rows_written"] += writes

    # ---------------- join 增量
    def _propagate_join(self, info, deltas, nd):
        node = info.node
        li, ri = info.children
        jtbl = self._table("join", info)
        affected = set()
        for side, cinfo, keyfield in (
                ("l", li, node.lkey), ("r", ri, node.rkey)):
            bucket = deltas[cinfo.nid]
            for jrow, d in bucket.items():
                if d == 0:
                    continue
                row = jdec(jrow)
                jkey = jenc([row[cinfo.cols.index(keyfield)]])
                affected.add(jkey)
                nd["rows_read"] += 1
                if d > 0:
                    self.con.execute(
                        f"INSERT INTO {jtbl}(side,jkey,jrow,mult) VALUES(?,?,?,?) "
                        "ON CONFLICT(side,jkey,jrow) DO UPDATE SET mult=mult+?",
                        (side, jkey, jrow, d, d))
                else:
                    cur = self.con.execute(
                        f"SELECT mult FROM {jtbl} WHERE side=? AND jkey=? AND jrow=?",
                        (side, jkey, jrow)).fetchone()
                    if cur is None:
                        # 差分与索引一致时不会发生：负差分必然对应已存在的索引行
                        raise SubscriptionError(
                            f"内部错误：连接索引缺少待减行 {side}/{jkey}/{jrow}")
                    newm = cur["mult"] + d
                    if newm == 0:
                        self.con.execute(
                            f"DELETE FROM {jtbl} WHERE side=? AND jkey=? AND jrow=?",
                            (side, jkey, jrow))
                    else:
                        self.con.execute(
                            f"UPDATE {jtbl} SET mult=? WHERE side=? AND jkey=? AND jrow=?",
                            (newm, side, jkey, jrow))
        out_bucket = deltas[info.nid]
        for jkey in sorted(affected):
            nd.setdefault("join_keys", []).append(jkey)
            # 旧输出：索引已被本提交修改，需在修改前快照——因此这里改为
            # “先算新输出，再用受影响行集合反推旧输出”，见 _join_key_diff。
            new_out = self._join_outputs_for_key(info, jkey)
            old_out = self._old_join_outputs(info, jkey, li, ri, deltas)
            for jrow, m in new_out.items():
                diff = m - old_out.pop(jrow, 0)
                if diff:
                    self._add_delta(out_bucket, jrow, diff)
            for jrow, m in old_out.items():
                if m:
                    self._add_delta(out_bucket, jrow, -m)
        self._apply_bag_deltas(info, out_bucket, nd)

    def _old_join_outputs(self, info, jkey, li, ri, deltas):
        """用“新输出撤销本提交差分”的方式重建该键的旧输出袋。

        连接输出对两侧袋都是多重线性的：对受影响键，先取当前（新）索引输出，
        再把两侧差分逐行临时回滚算出旧袋，避免扫任何不相关键。
        """
        node = info.node
        jtbl = self._table("join", info)
        # 临时把索引回退到旧状态
        touched = []
        for side, cinfo in (("l", li), ("r", ri)):
            keyfield = node.lkey if side == "l" else node.rkey
            for jrow, d in deltas[cinfo.nid].items():
                if d == 0:
                    continue
                row = jdec(jrow)
                if jenc([row[cinfo.cols.index(keyfield)]]) != jkey:
                    continue
                cur = self.con.execute(
                    f"SELECT mult FROM {jtbl} WHERE side=? AND jkey=? AND jrow=?",
                    (side, jkey, jrow)).fetchone()
                newm = None if cur is None else cur["mult"]
                oldm = (newm or 0) - d
                touched.append((side, jkey, jrow, newm, oldm))
                if oldm == 0:
                    self.con.execute(
                        f"DELETE FROM {jtbl} WHERE side=? AND jkey=? AND jrow=?",
                        (side, jkey, jrow))
                elif newm is None:
                    self.con.execute(
                        f"INSERT INTO {jtbl}(side,jkey,jrow,mult) VALUES(?,?,?,?)",
                        (side, jkey, jrow, oldm))
                else:
                    self.con.execute(
                        f"UPDATE {jtbl} SET mult=? WHERE side=? AND jkey=? AND jrow=?",
                        (oldm, side, jkey, jrow))
        old_out = self._join_outputs_for_key(info, jkey)
        # 恢复新状态
        for side, jkey2, jrow, newm, oldm in touched:
            if newm is None:
                self.con.execute(
                    f"DELETE FROM {jtbl} WHERE side=? AND jkey=? AND jrow=?",
                    (side, jkey2, jrow))
            elif oldm == 0:
                self.con.execute(
                    f"INSERT INTO {jtbl}(side,jkey,jrow,mult) VALUES(?,?,?,?)",
                    (side, jkey2, jrow, newm))
            else:
                self.con.execute(
                    f"UPDATE {jtbl} SET mult=? WHERE side=? AND jkey=? AND jrow=?",
                    (newm, side, jkey2, jrow))
        return old_out

    # ---------------- distinct 增量
    def _propagate_distinct(self, info, deltas, nd):
        dtbl = self._table("distinct", info)
        out_bucket = deltas[info.nid]
        for jrow, d in deltas[info.children[0].nid].items():
            if d == 0:
                continue
            nd["rows_read"] += 1
            row = self.con.execute(
                f"SELECT support FROM {dtbl} WHERE jrow=?", (jrow,)).fetchone()
            old = 0 if row is None else row["support"]
            new = old + d
            if new < 0:
                raise SubscriptionError(
                    f"内部错误：distinct 支持数减为负 {jrow}")
            if new == 0:
                self.con.execute(f"DELETE FROM {dtbl} WHERE jrow=?", (jrow,))
            elif row is None:
                self.con.execute(
                    f"INSERT INTO {dtbl}(jrow,support) VALUES(?,?)",
                    (jrow, new))
            else:
                self.con.execute(
                    f"UPDATE {dtbl} SET support=? WHERE jrow=?", (new, jrow))
            # 还有其他输入支持时值不消失；第一个支持进入/最后一个撤回才改变结果袋
            if old == 0 and new > 0:
                self._add_delta(out_bucket, jrow, 1)
            elif old > 0 and new == 0:
                self._add_delta(out_bucket, jrow, -1)
        self._apply_bag_deltas(info, out_bucket, nd)

    # ---------------- 聚合增量
    def _propagate_aggregate(self, info, deltas, nd):
        node = info.node
        child = info.children[0]
        itbl = self._table("aggidx", info)
        stbl = self._table("aggstate", info)
        gcols = [child.cols.index(g) for g in node.group_by]
        affected_groups = set()
        for jrow, d in deltas[child.nid].items():
            if d == 0:
                continue
            nd["rows_read"] += 1
            row = jdec(jrow)
            gkey = jenc([row[i] for i in gcols])
            affected_groups.add(gkey)
            if d > 0:
                self.con.execute(
                    f"INSERT INTO {itbl}(gkey,jrow,mult) VALUES(?,?,?)",
                    (gkey, jrow, d))
            else:
                # 差分按业务行净计：成员索引里同业务行重数直接调整
                cur = self.con.execute(
                    f"SELECT mult FROM {itbl} WHERE gkey=? AND jrow=?",
                    (gkey, jrow)).fetchone()
                newm = cur["mult"] + d
                if newm == 0:
                    self.con.execute(
                        f"DELETE FROM {itbl} WHERE gkey=? AND jrow=?",
                        (gkey, jrow))
                else:
                    self.con.execute(
                        f"UPDATE {itbl} SET mult=? WHERE gkey=? AND jrow=?",
                        (newm, gkey, jrow))
        if not node.group_by:
            affected_groups.add(jenc([]))
        out_bucket = deltas[info.nid]
        for gkey in sorted(affected_groups):
            nd["groups_recomputed"] = nd.get("groups_recomputed", 0) + 1
            state_row = self.con.execute(
                f"SELECT jrow FROM {stbl} WHERE gkey=?", (gkey,)).fetchone()
            old_jrow = None if state_row is None else state_row["jrow"]
            members = self.con.execute(
                f"SELECT COUNT(*) AS c FROM {itbl} WHERE gkey=?",
                (gkey,)).fetchone()["c"]
            if members == 0 and node.group_by:
                # 分组消失：撤回该组行；全局聚合保留空集单行
                if old_jrow is not None:
                    self.con.execute(f"DELETE FROM {stbl} WHERE gkey=?",
                                     (gkey,))
                    self._apply_agg_bag(info, old_jrow, None)
                    self._add_delta(out_bucket, old_jrow, -1)
                continue
            new_jrow = jenc(self._agg_row(info, gkey))
            if old_jrow is None:
                self.con.execute(
                    f"INSERT INTO {stbl}(gkey,jrow) VALUES(?,?)",
                    (gkey, new_jrow))
                self._apply_agg_bag(info, None, new_jrow)
                self._add_delta(out_bucket, new_jrow, 1)
            elif old_jrow != new_jrow:
                self.con.execute(
                    f"UPDATE {stbl} SET jrow=? WHERE gkey=?",
                    (new_jrow, gkey))
                self._apply_agg_bag(info, old_jrow, new_jrow)
                self._add_delta(out_bucket, old_jrow, -1)
                self._add_delta(out_bucket, new_jrow, 1)

    def _apply_agg_bag(self, info, old_jrow, new_jrow):
        """聚合节点的结果袋同步（全局聚合恒为单行，mult=1）。"""
        btbl = self._table("bag", info)
        if old_jrow is not None:
            self.con.execute(f"DELETE FROM {btbl} WHERE jrow=?", (old_jrow,))
        if new_jrow is not None:
            self.con.execute(
                f"INSERT INTO {btbl}(jrow,mult) VALUES(?,1) "
                "ON CONFLICT(jrow) DO UPDATE SET mult=1", (new_jrow,))

    # ------------------------------------------------ 读取当前结果
    def read_result(self):
        btbl = self._table("bag", self.root)
        items = [{"row": jdec(r["jrow"]), "mult": r["mult"]}
                 for r in self.con.execute(
                     f"SELECT jrow,mult FROM {btbl} ORDER BY jrow")]
        return {"columns": self.cols, "items": items,
                "position": self._meta_get("position")}

    # ------------------------------------------------ 变更分页
    def read_changes(self, after_seq, after_id, page_size):
        """稳定分页：游标 (after_seq, after_id) 重取得到相同内容。

        after_seq 是读取起点（只返回该提交之后的台账）；after_id 是页内稳定位置
        （台账自增 id）。SQL 用 (seq,id) 键集翻页：第一页用 seq>after_seq，
        后续页在同一游标位置上用 id>after_id 且仍满足 seq>after_seq。
        返回的 next_cursor 带回同一 after_seq 与本页最后一个 id，可直接续取。
        """
        position = self._meta_get("position")
        if after_seq > position:
            raise SubscriptionError(
                f"after_seq={after_seq} 超过已处理位置 {position}")
        if after_id is None:
            rows = self.con.execute(
                "SELECT * FROM m_changes WHERE seq>? ORDER BY seq,id LIMIT ?",
                (after_seq, page_size)).fetchall()
        else:
            rows = self.con.execute(
                "SELECT * FROM m_changes WHERE seq>? AND id>? "
                "ORDER BY seq,id LIMIT ?",
                (after_seq, after_id, page_size)).fetchall()
        items = []
        for r in rows:
            if r["kind"] == "boundary":
                items.append({"seq": r["seq"], "kind": "boundary"})
            else:
                items.append({"seq": r["seq"], "kind": "delta",
                              "row": jdec(r["jrow"]), "delta": r["delta"]})
        last = rows[-1] if rows else None
        next_cursor = None
        if last is not None:
            next_cursor = {"after_seq": after_seq, "after_id": last["id"]}
        return {"columns": self.cols, "items": items,
                "position": position, "next_cursor": next_cursor,
                "page_size": page_size}

    def diagnostics(self):
        position = self._meta_get("position")
        rows = self.con.execute(
            "SELECT seq,detail FROM m_diag ORDER BY seq").fetchall()
        return {"position": position,
                "cumulative": self._meta_get("cumulative", self._zero_cum()),
                "per_commit": [{"seq": r["seq"],
                                "detail": json.loads(r["detail"])} for r in rows]}
