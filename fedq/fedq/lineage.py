"""结果数据血缘：带血缘执行、可展开证据、按源行反查。

定位：这是**结果数据的血缘**功能——从某一条查询结果追到它实际使用的输入出现；
不是重新解释下推计划，也不是“修改一行后答案会怎样”的反事实分析，更不证明
“为什么没有匹配”的全库否定。

模型（详见 docs/LINEAGE.md）：
- 在已打开视图的冻结数据上执行**用户提交的查询树本身**（统一语义不变），
  逐算子产生“出现”（occurrence）：袋语义保留重数——投影后同值的两行是两个出现、
  inner 连接的每个匹配对是一个出现，各有自己的 row_ref（`run_id:occ_id`）。
- 血缘只记三类关系，边挂在“输出现 → 输入出现”上：
    all  共同成立：inner/left 连接的左右依据、filter/project/sort/limit 的唯一输入；
    any  备选推导：distinct 把相同业务行的多个输入出现合成一个结果，
         这些输入是备选的推导，不是多份业务记录；
    agg  逐份计入：聚合把每个实际输入出现计入同一汇总，边上带逐份贡献
         （counted / null_skipped / sum 精确值）。
  left 连接无匹配时，右侧边是 null_fill 标记（to_occ 为空），绝不虚构右源行。
- 源行身份 = 逻辑源 + 实例 + 表 + 读取版本(at) + 主键 + 查询树输入位置(path)。
  同一张表在查询中出现两次时，path 区分它走了哪个输入位置。
- 出现与边全部落 SQLite（证据库 evidence.db）：连接结果、distinct 备选、聚合成员
  都不常驻 Python；distinct 的备选边用集合式 INSERT...SELECT 写入，
  子推导按出现共享（DAG），从不展开备选分支的笛卡尔组合。
- 反查表 usage(src_id, root_occ) 由递归 CTE 在证据库内计算，只记录
  “源行 → 最终结果出现”的可达对；被 filter/limit 淘汰的出现从一开始就不产生
  输出现，因此追溯与反查不会混入被淘汰的兄弟行。
- 结果与证据都写完整才把 run 标成 ok；中途失败留下 state=error 的目录
  （run.json + INCOMPLETE.txt），导航接口拒绝提供看似完整的血缘。
- 证据库自包含：查询、结果、源坐标都在其中。原视图关闭、源进程消失后，
  另一个进程仍能展开与反查，不再请求源、不重跑原查询。

血缘列（occ_id/seq/row_ref）不是业务列：业务值表只含业务列，过滤/分组/去重
只作用于业务列，血缘信息不参与任何业务语义。
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import time
import uuid

from . import expr as E
from . import query as Q

MAX_PAGE_SIZE = 10000


class LineageError(Exception):
    """带血缘执行或导航失败。"""


class LineageNotFound(LineageError):
    """未知的 run_id / row_ref。"""


class LineageStateError(LineageError):
    """run 不完整（building/error）：拒绝给出看似完整的血缘。"""


def _now():
    return time.strftime("%Y-%m-%dT%H:%M:%S")


def _j(obj):
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def _q(name):
    return '"' + name.replace('"', '""') + '"'


def _numtext_cmp(a, b):
    x, y = int(a), int(b)
    return (x > y) - (y > x)


# 存储类型：int/bool → INTEGER，text → TEXT，sumtext → TEXT COLLATE NUMTEXT（大整数精确）
def _storage_of(logical):
    return {"int": "int", "text": "text", "bool": "bool"}[logical]


def _enc(v, st):
    if v is None:
        return None
    if st == "bool":
        return 1 if v else 0
    if st == "sumtext":
        return str(v)
    return v


def _dec(v, st):
    if v is None:
        return None
    if st == "bool":
        return bool(v)
    if st == "sumtext":
        return int(v)
    return v


EVIDENCE_SCHEMA = """
CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE nodes(
    node_id INTEGER PRIMARY KEY,
    path TEXT NOT NULL,          -- '' 根；之后每级 /i（一元输入）或 /l、/r（连接左右）
    op TEXT NOT NULL,
    detail TEXT NOT NULL,        -- json：算子细节（谓词/列/键/聚合/排序键…）
    cols TEXT NOT NULL           -- json：[[列名, 逻辑类型, 存储类型], ...]（仅业务列）
);
CREATE TABLE occs(
    occ_id INTEGER PRIMARY KEY,  -- run 内唯一；row_ref = run_id:occ_id
    node_id INTEGER NOT NULL,
    seq INTEGER NOT NULL         -- 该节点输出顺序（结果翻页、limit 都按它）
);
CREATE TABLE edges(
    edge_id INTEGER PRIMARY KEY, -- 插入序即分页序（稳定）
    from_occ INTEGER NOT NULL,   -- 输出现
    slot TEXT NOT NULL,          -- input|left|right|alt|member
    kind TEXT NOT NULL,          -- all|any|agg
    to_occ INTEGER,              -- 输入出现；null_fill 标记边为 NULL
    aux TEXT                     -- json：标记信息或聚合逐份贡献
);
CREATE INDEX edges_from ON edges(from_occ);
CREATE INDEX edges_to ON edges(to_occ);
CREATE TABLE src_rows(
    src_id INTEGER PRIMARY KEY,
    source TEXT NOT NULL,        -- 逻辑源
    instance_id TEXT NOT NULL,   -- 实例身份
    tbl TEXT NOT NULL,           -- 表
    at INTEGER NOT NULL,         -- 读取版本（视图 F 分量）
    pk TEXT NOT NULL,            -- json：主键值列表
    path TEXT NOT NULL           -- 查询树输入位置（同表两次引用据此区分）
);
CREATE INDEX src_lookup ON src_rows(source, tbl, pk);
CREATE TABLE occ_src(occ_id INTEGER PRIMARY KEY, src_id INTEGER NOT NULL);
CREATE TABLE usage(
    src_id INTEGER NOT NULL,
    root_occ INTEGER NOT NULL,   -- 最终结果出现（根节点出现）
    PRIMARY KEY(src_id, root_occ)
);
CREATE TABLE agg_summ(
    group_occ INTEGER NOT NULL,
    out TEXT NOT NULL,
    f TEXT NOT NULL,
    field TEXT,
    counted INTEGER NOT NULL,    -- 计入的出现数（count_star=成员数；count/sum=非null数）
    null_skipped INTEGER NOT NULL,
    value TEXT,                  -- 该聚合结果值（sum 为十进制文本；全 null 为 NULL）
    PRIMARY KEY(group_occ, out)
);
"""

NULL_FILL_AUX = {"marker": "null_fill",
                 "note": "left 连接无匹配：右侧补 null（非真实源行，无源行身份）"}


# ================================================================ 执行引擎

class _Engine:
    """在证据库内逐算子执行查询树；出现与边批量落盘，不常驻 Python。"""

    def __init__(self, con, frozen_path, sources_meta, fail_hook=None):
        self.con = con
        self.frozen_path = frozen_path
        self.sources_meta = sources_meta   # {source: {instance_id, at, dialect, schema_json}}
        self.fail_hook = fail_hook         # 演示/测试注入：fn(stage, count) 可抛异常
        self._fcon = None
        self._node_count = 0
        self._node_cols = {}               # nid -> [(name, logical, storage)]
        self._seq = {}                     # nid -> 下一个 seq
        self._next_occ = 1
        self._next_src = 1
        self._occ_total = 0
        self._edge_total = 0
        self._occs_buf = []
        self._vals_buf = []
        self._vals_sql = None
        self._vals_nid = None
        self._edges_buf = []
        self._src_buf = []
        self._osrc_buf = []
        self._summ_buf = []

    # ---------------- 基础 ----------------
    def _hook(self, stage, count):
        if self.fail_hook is not None:
            self.fail_hook(stage, count)

    def _frozen(self):
        if self._fcon is None:
            self._fcon = sqlite3.connect(self.frozen_path)
        return self._fcon

    def close(self):
        if self._fcon is not None:
            self._fcon.close()
            self._fcon = None

    def _register(self, path, op, detail, cols):
        """cols: [(name, logical, storage)]；建节点记录与业务值表 n<nid>。"""
        nid = self._node_count
        self._node_count += 1
        self.con.execute(
            "INSERT INTO nodes(node_id,path,op,detail,cols) VALUES(?,?,?,?,?)",
            (nid, path, op, _j(detail), _j(cols)))
        defs = ["occ_id INTEGER PRIMARY KEY", "seq INTEGER"]
        for name, _logical, st in cols:
            sqlt = {"int": "INTEGER", "bool": "INTEGER",
                    "text": "TEXT", "sumtext": "TEXT COLLATE NUMTEXT"}[st]
            defs.append(f"{_q(name)} {sqlt}")
        self.con.execute(f"CREATE TABLE n{nid} ({', '.join(defs)})")
        self._node_cols[nid] = cols
        return nid

    def _begin_emit(self, nid):
        self._flush()
        cols = self._node_cols[nid]
        marks = ",".join("?" for _ in range(2 + len(cols)))
        self._vals_sql = f"INSERT INTO n{nid} VALUES({marks})"
        self._vals_nid = nid

    def _emit(self, vals):
        """为当前节点产生一个输出现（业务值按存储类型编码）。返回 occ_id。"""
        oid = self._next_occ
        self._next_occ += 1
        nid = self._vals_nid
        seq = self._seq.get(nid, 0)
        self._seq[nid] = seq + 1
        self._occs_buf.append((oid, nid, seq))
        self._vals_buf.append((oid, seq) + tuple(vals))
        self._occ_total += 1
        if len(self._vals_buf) >= 500:
            self._flush()
        return oid

    def _edge(self, from_occ, slot, kind, to_occ, aux=None):
        self._edges_buf.append((from_occ, slot, kind, to_occ,
                                _j(aux) if aux is not None else None))
        self._edge_total += 1
        if len(self._edges_buf) >= 500:
            self._flush()

    def _flush(self):
        if self._vals_buf:
            self.con.executemany(
                "INSERT INTO occs(occ_id,node_id,seq) VALUES(?,?,?)", self._occs_buf)
            self.con.executemany(self._vals_sql, self._vals_buf)
            self._occs_buf, self._vals_buf = [], []
            self._hook("occ", self._occ_total)
        if self._edges_buf:
            self.con.executemany(
                "INSERT INTO edges(from_occ,slot,kind,to_occ,aux) VALUES(?,?,?,?,?)",
                self._edges_buf)
            self._edges_buf = []
            self._hook("edge", self._edge_total)
        if self._src_buf:
            self.con.executemany(
                "INSERT INTO src_rows(src_id,source,instance_id,tbl,at,pk,path)"
                " VALUES(?,?,?,?,?,?,?)", self._src_buf)
            self.con.executemany(
                "INSERT INTO occ_src(occ_id,src_id) VALUES(?,?)", self._osrc_buf)
            self._src_buf, self._osrc_buf = [], []
        if self._summ_buf:
            self.con.executemany(
                "INSERT INTO agg_summ(group_occ,out,f,field,counted,null_skipped,value)"
                " VALUES(?,?,?,?,?,?,?)", self._summ_buf)
            self._summ_buf = []

    # ---------------- 算子 ----------------
    def execute(self, q):
        root = self._exec(q, "")
        self._flush()
        self.con.commit()
        return root

    def _exec(self, node, path):
        if isinstance(node, Q.Source):
            return self._exec_source(node, path)
        if isinstance(node, Q.Filter):
            c = self._exec(node.input, path + "/i")
            return self._exec_filter(node, c, path)
        if isinstance(node, Q.Project):
            c = self._exec(node.input, path + "/i")
            return self._exec_project(node, c, path)
        if isinstance(node, Q.Join):
            ln = self._exec(node.left, path + "/l")
            rn = self._exec(node.right, path + "/r")
            return self._exec_join(node, ln, rn, path)
        if isinstance(node, Q.Aggregate):
            c = self._exec(node.input, path + "/i")
            return self._exec_aggregate(node, c, path)
        if isinstance(node, Q.Distinct):
            c = self._exec(node.input, path + "/i")
            return self._exec_distinct(node, c, path)
        if isinstance(node, Q.Sort):
            c = self._exec(node.input, path + "/i")
            return self._exec_sort(node, c, path)
        if isinstance(node, Q.Limit):
            c = self._exec(node.input, path + "/i")
            return self._exec_limit(node, c, path)
        raise LineageError(f"血缘执行未支持的节点: {node.op}")

    def _exec_source(self, node, path):
        meta = self.sources_meta[node.source]
        tschema = meta["schema_json"][node.table]
        tcols = list(tschema["columns"].items())
        key = tschema.get("key", ["id"])
        cols = [(n, l, _storage_of(l)) for n, l in tcols]
        nid = self._register(path, "source",
                             {"source": node.source, "table": node.table,
                              "instance_id": meta["instance_id"], "at": meta["at"]},
                             cols)
        self._begin_emit(nid)
        names = [n for n, _ in tcols]
        kidx = [names.index(k) for k in key]
        cur = self._frozen().execute(
            "SELECT jrow FROM frozen_rows WHERE tbl=? ORDER BY page,rowidx",
            (node.table,))
        while True:
            chunk = cur.fetchmany(500)
            if not chunk:
                break
            for (jrow,) in chunk:
                raw = json.loads(jrow)          # 行值按表列顺序
                vals = tuple(_enc(raw[i], cols[i][2]) for i in range(len(cols)))
                oid = self._emit(vals)
                pk = [raw[i] for i in kidx]
                sid = self._next_src
                self._next_src += 1
                self._src_buf.append((sid, node.source, meta["instance_id"],
                                      node.table, meta["at"], _j(pk), path))
                self._osrc_buf.append((oid, sid))
            self._flush()
        self._flush()
        self.con.commit()
        return nid

    def _exec_filter(self, node, cnid, path):
        ccols = self._node_cols[cnid]
        nid = self._register(path, "filter",
                             {"pred": node.pred.to_json(),
                              "render": E.render(node.pred)}, ccols)
        self._begin_emit(nid)
        psql, params = E.to_sql(node.pred)
        colnames = ", ".join(_q(c[0]) for c in ccols)
        cur = self.con.execute(
            f"SELECT occ_id, {colnames} FROM n{cnid} WHERE {psql} ORDER BY seq",
            params)
        while True:
            chunk = cur.fetchmany(500)
            if not chunk:
                break
            for row in chunk:
                oid = self._emit(row[1:])
                self._edge(oid, "input", "all", row[0])
            self._flush()
        self._flush()
        self.con.commit()
        return nid

    def _exec_project(self, node, cnid, path):
        ccols = self._node_cols[cnid]
        by_name = {c[0]: c for c in ccols}
        cols = [by_name[c] for c in node.cols]
        nid = self._register(path, "project", {"cols": list(node.cols)}, cols)
        self._begin_emit(nid)
        colnames = ", ".join(_q(c) for c in node.cols)
        cur = self.con.execute(
            f"SELECT occ_id, {colnames} FROM n{cnid} ORDER BY seq")
        while True:
            chunk = cur.fetchmany(500)
            if not chunk:
                break
            for row in chunk:
                oid = self._emit(row[1:])
                self._edge(oid, "input", "all", row[0])
            self._flush()
        self._flush()
        self.con.commit()
        return nid

    def _exec_join(self, node, lnid, rnid, path):
        lcols = self._node_cols[lnid]
        rcols = self._node_cols[rnid]
        orig2out = {orig: out for out, orig in node.right_name_map.items()}
        out_cols = list(lcols) + [(orig2out[c[0]], c[1], c[2]) for c in rcols]
        nid = self._register(path, "join",
                             {"kind": node.kind, "lkey": node.lkey,
                              "rkey": node.rkey}, out_cols)
        self._begin_emit(nid)
        lnames = [c[0] for c in lcols]
        lk_idx = lnames.index(node.lkey)
        lst = lcols[lk_idx][2]
        rst = next(c[2] for c in rcols if c[0] == node.rkey)
        rcolnames = ", ".join(_q(c[0]) for c in rcols)
        if lst == "sumtext" or rst == "sumtext":
            # 大整数和按规范十进制文本比较（任意精度安全；NULL 仍不匹配）
            where = f"CAST({_q(node.rkey)} AS TEXT) = CAST(? AS TEXT)"
        else:
            where = f"{_q(node.rkey)} = ?"
            self.con.execute(
                f"CREATE INDEX ij{nid} ON n{rnid}({_q(node.rkey)})")
        rsql = f"SELECT occ_id, {rcolnames} FROM n{rnid} WHERE {where} ORDER BY seq"
        lcolnames = ", ".join(_q(c[0]) for c in lcols)
        cur = self.con.execute(
            f"SELECT occ_id, {lcolnames} FROM n{lnid} ORDER BY seq")
        while True:
            chunk = cur.fetchmany(500)
            if not chunk:
                break
            for lrow in chunk:
                lk = lrow[1 + lk_idx]
                # 键为 null 永不匹配（统一语义；SQL `= NULL` 同样不匹配）
                matches = [] if lk is None else \
                    self.con.execute(rsql, (lk,)).fetchall()
                if matches:
                    for rrow in matches:
                        oid = self._emit(tuple(lrow[1:]) + tuple(rrow[1:]))
                        self._edge(oid, "left", "all", lrow[0])
                        self._edge(oid, "right", "all", rrow[0])
                elif node.kind == "left":
                    oid = self._emit(tuple(lrow[1:]) + (None,) * len(rcols))
                    self._edge(oid, "left", "all", lrow[0])
                    self._edge(oid, "right", "all", None, NULL_FILL_AUX)
            self._flush()
        self._flush()
        self.con.commit()
        return nid

    def _exec_distinct(self, node, cnid, path):
        ccols = self._node_cols[cnid]
        nid = self._register(path, "distinct", {}, ccols)
        self._begin_emit(nid)
        colnames = ", ".join(_q(c[0]) for c in ccols)
        # 首次出现顺序（与参照解释器一致）；GROUP BY 只按业务列去重
        cur = self.con.execute(
            f"SELECT {colnames}, MIN(seq) AS fseq FROM n{cnid} "
            f"GROUP BY {colnames} ORDER BY fseq")
        while True:
            chunk = cur.fetchmany(500)
            if not chunk:
                break
            for row in chunk:
                self._emit(row[:len(ccols)])
            self._flush()
        self._flush()
        # 备选边（any）：集合式写入——每个输入出现一条边，绝不枚举组合
        eqs = " AND ".join(f"i.{_q(c[0])} IS o.{_q(c[0])}" for c in ccols)
        self.con.execute(
            f"INSERT INTO edges(from_occ, slot, kind, to_occ) "
            f"SELECT o.occ_id, 'alt', 'any', i.occ_id "
            f"FROM n{cnid} i JOIN n{nid} o ON {eqs} "
            f"ORDER BY o.occ_id, i.occ_id")
        self._edge_total = self.con.execute(
            "SELECT COUNT(*) FROM edges").fetchone()[0]
        self._hook("edge", self._edge_total)
        self.con.commit()
        return nid

    def _exec_aggregate(self, node, cnid, path):
        ccols = self._node_cols[cnid]
        names = [c[0] for c in ccols]
        gidx = [names.index(g) for g in node.group_by]
        aidx = [(a, names.index(a.field) if a.field else None) for a in node.aggs]
        out_cols = [(g, ccols[i][1], ccols[i][2]) for g, i in
                    zip(node.group_by, gidx)]
        for a in node.aggs:
            out_cols.append((a.out, "int",
                             "sumtext" if a.f == "sum" else "int"))
        nid = self._register(path, "aggregate",
                             {"group_by": list(node.group_by),
                              "aggs": [a.to_json() for a in node.aggs]}, out_cols)
        colnames = ", ".join(_q(c[0]) for c in ccols)
        # 第一遍：流式累计分组状态（sum 用 Python 整数，任意精度）
        groups, order = {}, []
        for row in self.con.execute(
                f"SELECT {colnames} FROM n{cnid} ORDER BY seq"):
            key = tuple(row[i] for i in gidx)
            jk = _j(list(key))
            g = groups.get(jk)
            if g is None:
                g = groups[jk] = {"key": key,
                                  "acc": {a.out: [0, 0, 0, False]
                                          for a in node.aggs}}
                order.append(jk)
            for a, i in aidx:
                st = g["acc"][a.out]      # [计入数, null跳过数, sum合计, 见过非null]
                if a.f == "count_star":
                    st[0] += 1
                else:
                    v = row[i]
                    if v is None:
                        st[1] += 1
                    else:
                        st[0] += 1
                        if a.f == "sum":
                            st[2] += int(v)
                            st[3] = True
        # 全局空集聚合：也产生一条可解释结果（count=0、sum=null、没有输入）
        if not node.group_by and not order:
            order.append("[]")
            groups["[]"] = {"key": (),
                            "acc": {a.out: [0, 0, 0, False] for a in node.aggs}}
        # 产出组出现；jkey→组出现 映射落 SQLite（组成员不常驻 Python）
        self._begin_emit(nid)
        self.con.execute(f"CREATE TABLE g{nid} (jkey TEXT PRIMARY KEY,"
                         f" occ_id INTEGER)")
        gocc = {}
        for jk in order:
            g = groups[jk]
            vals = list(g["key"])
            for a in node.aggs:
                st = g["acc"][a.out]
                if a.f == "sum":
                    vals.append(str(st[2]) if st[3] else None)
                else:
                    vals.append(st[0])
            oid = self._emit(tuple(vals))
            gocc[jk] = oid
            self.con.execute(f"INSERT INTO g{nid} VALUES(?,?)", (jk, oid))
            for a in node.aggs:
                st = g["acc"][a.out]
                if a.f == "count_star":
                    counted, skipped, val = st[0], 0, str(st[0])
                elif a.f == "count":
                    counted, skipped, val = st[0], st[1], str(st[0])
                else:
                    counted, skipped = st[0], st[1]
                    val = str(st[2]) if st[3] else None
                self._summ_buf.append((oid, a.out, a.f, a.field,
                                       counted, skipped, val))
        self._flush()
        # 第二遍：逐成员写 agg 边（含逐份贡献），直接批量落盘
        for row in self.con.execute(
                f"SELECT occ_id, {colnames} FROM n{cnid} ORDER BY seq"):
            key = tuple(row[1 + i] for i in gidx)
            go = gocc[_j(list(key))]
            contrib = {}
            for a, i in aidx:
                if a.f == "count_star":
                    contrib[a.out] = {"kind": "counted"}
                else:
                    v = row[1 + i]
                    if v is None:
                        contrib[a.out] = {"kind": "null_skipped"}
                    elif a.f == "sum":
                        contrib[a.out] = {"kind": "sum", "value": str(v)}
                    else:
                        contrib[a.out] = {"kind": "counted"}
            self._edge(go, "member", "agg", row[0], contrib)
        self._flush()
        self.con.execute(f"DROP TABLE g{nid}")
        self.con.commit()
        return nid

    def _exec_sort(self, node, cnid, path):
        ccols = self._node_cols[cnid]
        nid = self._register(path, "sort",
                             {"keys": [k.to_json() for k in node.keys]}, ccols)
        self._begin_emit(nid)
        by_name = {c[0]: c for c in ccols}
        terms = []
        for k in node.keys:
            t = _q(k.field)
            if by_name[k.field][2] == "sumtext":
                t += " COLLATE NUMTEXT"
            t += " ASC" if k.asc else " DESC"
            t += " NULLS FIRST" if k.nulls_first else " NULLS LAST"
            terms.append(t)
        colnames = ", ".join(_q(c[0]) for c in ccols)
        cur = self.con.execute(
            f"SELECT occ_id, {colnames} FROM n{cnid} ORDER BY {', '.join(terms)}")
        while True:
            chunk = cur.fetchmany(500)
            if not chunk:
                break
            for row in chunk:
                oid = self._emit(row[1:])
                self._edge(oid, "input", "all", row[0])
            self._flush()
        self._flush()
        self.con.commit()
        return nid

    def _exec_limit(self, node, cnid, path):
        ccols = self._node_cols[cnid]
        nid = self._register(path, "limit", {"n": node.n}, ccols)
        self._begin_emit(nid)
        colnames = ", ".join(_q(c[0]) for c in ccols)
        # 只让实际保留下来的出现成为最终结果；被淘汰的兄弟行不产生输出现
        cur = self.con.execute(
            f"SELECT occ_id, {colnames} FROM n{cnid} ORDER BY seq LIMIT {node.n}")
        for row in cur:
            oid = self._emit(row[1:])
            self._edge(oid, "input", "all", row[0])
        self._flush()
        self.con.commit()
        return nid

    # ---------------- 收尾 ----------------
    def compute_usage(self, root_nid):
        """在证据库内计算 源行→最终结果 可达对（递归 CTE，集合操作）。"""
        self._hook("usage", 0)
        self.con.execute(
            "WITH RECURSIVE reach(occ, src) AS ("
            "  SELECT occ_id, src_id FROM occ_src "
            "  UNION "
            "  SELECT e.from_occ, r.src FROM edges e "
            "  JOIN reach r ON e.to_occ = r.occ"
            ") INSERT INTO usage(src_id, root_occ) "
            "SELECT DISTINCT r.src, r.occ FROM reach r "
            "WHERE r.occ IN (SELECT occ_id FROM occs WHERE node_id=?)",
            (root_nid,))
        self.con.commit()

    def rows_out(self, nid):
        return self._seq.get(nid, 0)

    def results_sha256(self, nid):
        cols = self._node_cols[nid]
        colnames = ", ".join(_q(c[0]) for c in cols)
        h = hashlib.sha256()
        for row in self.con.execute(
                f"SELECT {colnames} FROM n{nid} ORDER BY seq"):
            vals = [_dec(v, c[2]) for v, c in zip(row, cols)]
            h.update((_j(vals) + "\n").encode("utf-8"))
        return h.hexdigest()


# ================================================================ 服务

class LineageService:
    """带血缘执行与导航。目录布局：

    <workdir>/lineage/
      runs.db                 目录库：每次 run 的状态（building|ok|error）
      <run_id>/run.json       该 run 的自描述（查询、源坐标、状态、错误、校验和）
      <run_id>/evidence.db    证据库：节点、出现、边、源行身份、反查表
      <run_id>/INCOMPLETE.txt 仅失败 run 存在：失败原因（识别失败目录的标志）
    """

    def __init__(self, workdir):
        self.workdir = workdir
        self.dir = os.path.join(workdir, "lineage")
        os.makedirs(self.dir, exist_ok=True)
        self.db = sqlite3.connect(os.path.join(self.dir, "runs.db"),
                                  check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        with self.db:
            self.db.execute(
                "CREATE TABLE IF NOT EXISTS runs("
                " run_id TEXT PRIMARY KEY, view_id TEXT, name TEXT, state TEXT,"
                " created TEXT, finished TEXT, columns_json TEXT,"
                " rows_out INTEGER, error TEXT, query_json TEXT,"
                " sources_json TEXT, results_sha256 TEXT, run_dir TEXT)")

    # ---------------- 开始带血缘执行 ----------------
    def start_run(self, view_id, qjson, view_sources, frozen_path,
                  name="lineage", fail_hook=None):
        """执行查询树并留存结果与证据。成功返回 run 信息；失败抛 LineageError
        （run 目录与目录库都标记 error，不得当作完整血缘）。"""
        from .coordinator import frozen_caps   # 延迟导入避免环
        q = Q.from_json(qjson)                 # 校验失败：不产生任何 run 记录
        caps = {r["source"]: frozen_caps(r["dialect"], r["source"],
                                         json.loads(r["schema_json"]))
                for r in view_sources}
        schema = q.validate(caps)

        run_id = "lr-" + uuid.uuid4().hex[:16]
        run_dir = os.path.join(self.dir, run_id)
        os.makedirs(run_dir)
        created = _now()
        sources_meta = {
            r["source"]: {"instance_id": r["instance_id"], "at": r["at"],
                          "dialect": r["dialect"],
                          "schema_json": json.loads(r["schema_json"])}
            for r in view_sources}
        coords = [{"source": s, "instance_id": m["instance_id"], "at": m["at"]}
                  for s, m in sorted(sources_meta.items())]
        record = {"run_id": run_id, "view_id": view_id, "name": name,
                  "state": "building", "created": created, "finished": None,
                  "columns": schema.names(), "rows_out": None, "error": None,
                  "query": qjson, "sources": coords, "results_sha256": None,
                  "run_dir": run_dir}
        self._catalog_insert(record)
        self._write_run_json(run_dir, record)

        ev_path = os.path.join(run_dir, "evidence.db")
        con = sqlite3.connect(ev_path)
        con.row_factory = sqlite3.Row
        try:
            con.execute("PRAGMA journal_mode=OFF")
            con.execute("PRAGMA synchronous=OFF")
            con.create_collation("NUMTEXT", _numtext_cmp)
            con.executescript(EVIDENCE_SCHEMA)
            engine = _Engine(con, frozen_path, sources_meta, fail_hook)
            try:
                self._evidence_meta(con, {
                    "state": "building", "run_id": run_id, "view_id": view_id,
                    "name": name, "created": created, "query": qjson,
                    "sources": coords, "columns": schema.names()})
                root = engine.execute(q)
                engine.compute_usage(root)
                sha = engine.results_sha256(root)
                rows_out = engine.rows_out(root)
            finally:
                engine.close()
            finished = _now()
            self._evidence_meta(con, {
                "state": "ok", "finished": finished, "root_node": root,
                "rows_out": rows_out, "results_sha256": sha})
            con.commit()
            con.close()
            # 结果与证据都完整后，才允许标成可追溯成功（先 run.json 后目录库）
            record.update({"state": "ok", "finished": finished,
                           "rows_out": rows_out, "results_sha256": sha})
            self._write_run_json(run_dir, record)
            self._catalog_update(record)
            return {"run_id": run_id, "state": "ok",
                    "columns": schema.names(), "rows_out": rows_out,
                    "results_sha256": sha, "run_dir": run_dir}
        except Exception as e:
            try:
                con.close()
            except Exception:
                pass
            record.update({"state": "error", "finished": _now(),
                           "error": str(e)})
            self._write_run_json(run_dir, record)
            with open(os.path.join(run_dir, "INCOMPLETE.txt"), "w",
                      encoding="utf-8") as f:
                f.write(f"该 run 未完成，血缘不完整，不得使用。\n"
                        f"run_id: {run_id}\n时间: {record['finished']}\n"
                        f"失败: {e}\n")
            self._catalog_update(record)
            err = LineageError(
                f"带血缘执行失败：run {run_id} 不完整（{e}）；"
                f"目录 {run_dir} 已标记 error（见 INCOMPLETE.txt），"
                f"不自动续算")
            err.run_id = run_id
            raise err from e

    @staticmethod
    def _evidence_meta(con, kv):
        con.executemany("INSERT OR REPLACE INTO meta(key,value) VALUES(?,?)",
                        [(k, _j(v)) for k, v in kv.items()])
        con.commit()

    def _catalog_insert(self, r):
        with self.db:
            self.db.execute(
                "INSERT INTO runs(run_id,view_id,name,state,created,finished,"
                "columns_json,rows_out,error,query_json,sources_json,"
                "results_sha256,run_dir) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (r["run_id"], r["view_id"], r["name"], r["state"], r["created"],
                 r["finished"], _j(r["columns"]), r["rows_out"], r["error"],
                 _j(r["query"]), _j(r["sources"]), r["results_sha256"],
                 r["run_dir"]))

    def _catalog_update(self, r):
        with self.db:
            self.db.execute(
                "UPDATE runs SET state=?, finished=?, rows_out=?, error=?,"
                " results_sha256=? WHERE run_id=?",
                (r["state"], r["finished"], r["rows_out"], r["error"],
                 r["results_sha256"], r["run_id"]))

    @staticmethod
    def _write_run_json(run_dir, record):
        tmp = os.path.join(run_dir, "run.json.tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=1)
        os.replace(tmp, os.path.join(run_dir, "run.json"))

    # ---------------- 目录与核对 ----------------
    def list_runs(self):
        rows = self.db.execute(
            "SELECT run_id,view_id,name,state,created,finished,rows_out,error"
            " FROM runs ORDER BY rowid").fetchall()
        return [dict(r) for r in rows]

    def _catalog_row(self, run_id):
        row = self.db.execute("SELECT * FROM runs WHERE run_id=?",
                              (run_id,)).fetchone()
        if row is None:
            raise LineageNotFound(f"未知 run_id: {run_id}")
        return dict(row)

    def show(self, run_id):
        """run 元数据 + 完整性核对（重算结果校验和与留存值比对）。"""
        cat = self._catalog_row(run_id)
        out = {k: cat[k] for k in
               ("run_id", "view_id", "name", "state", "created", "finished",
                "rows_out", "error", "results_sha256", "run_dir")}
        out["columns"] = json.loads(cat["columns_json"])
        out["query"] = json.loads(cat["query_json"])
        out["sources"] = json.loads(cat["sources_json"])
        rj = os.path.join(cat["run_dir"], "run.json")
        out["run_json"] = None
        if os.path.exists(rj):
            with open(rj, encoding="utf-8") as f:
                out["run_json"] = json.load(f)
        out["verified"] = False
        if cat["state"] == "ok":
            con, _, meta = self._open_ok(run_id)
            try:
                root = meta["root_node"]
                cols = self._node_cols(con, root)
                colnames = ", ".join(_q(c[0]) for c in cols)
                h = hashlib.sha256()
                for row in con.execute(
                        f"SELECT {colnames} FROM n{root} ORDER BY seq"):
                    vals = [_dec(v, c[2]) for v, c in zip(row, cols)]
                    h.update((_j(vals) + "\n").encode("utf-8"))
                out["verified"] = (h.hexdigest() == cat["results_sha256"]
                                   == meta["results_sha256"])
            finally:
                con.close()
        return out

    # ---------------- 内部：打开完整证据 ----------------
    def _open_ok(self, run_id):
        cat = self._catalog_row(run_id)
        if cat["state"] != "ok":
            raise LineageStateError(
                f"run {run_id} 状态为 {cat['state']}"
                f"（{cat['error'] or '执行未完成'}）：结果与证据不完整，"
                f"拒绝提供看似完整的血缘")
        path = os.path.join(cat["run_dir"], "evidence.db")
        if not os.path.exists(path):
            raise LineageStateError(f"run {run_id} 的证据库缺失：{path}")
        con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        con.row_factory = sqlite3.Row
        con.create_collation("NUMTEXT", _numtext_cmp)
        meta = {r["key"]: json.loads(r["value"])
                for r in con.execute("SELECT key,value FROM meta")}
        if meta.get("state") != "ok":
            con.close()
            raise LineageStateError(
                f"run {run_id} 证据库内部状态不完整（{meta.get('state')}）")
        return con, cat, meta

    @staticmethod
    def _check_page(page, page_size):
        if not isinstance(page, int) or isinstance(page, bool) or page < 0:
            raise LineageError(f"page 必须是非负整数: {page!r}")
        if not isinstance(page_size, int) or isinstance(page_size, bool) \
                or not (1 <= page_size <= MAX_PAGE_SIZE):
            raise LineageError(
                f"page_size 必须是 1..{MAX_PAGE_SIZE} 的整数: {page_size!r}")

    @staticmethod
    def _node_cols(con, nid):
        row = con.execute("SELECT cols FROM nodes WHERE node_id=?",
                          (nid,)).fetchone()
        return [tuple(c) for c in json.loads(row["cols"])]

    def _node(self, con, nid):
        row = con.execute("SELECT * FROM nodes WHERE node_id=?",
                          (nid,)).fetchone()
        return {"node_id": nid, "path": row["path"], "op": row["op"],
                "detail": json.loads(row["detail"]),
                "cols": [tuple(c) for c in json.loads(row["cols"])]}

    def _values(self, con, nid, occ_id):
        cols = self._node_cols(con, nid)
        colnames = ", ".join(_q(c[0]) for c in cols)
        row = con.execute(f"SELECT {colnames} FROM n{nid} WHERE occ_id=?",
                          (occ_id,)).fetchone()
        return [_dec(v, c[2]) for v, c in zip(tuple(row), cols)]

    def _child_brief(self, con, run_id, occ_id):
        nid = con.execute("SELECT node_id FROM occs WHERE occ_id=?",
                          (occ_id,)).fetchone()["node_id"]
        node = self._node(con, nid)
        return {"row_ref": f"{run_id}:{occ_id}", "node_path": node["path"],
                "op": node["op"], "values": self._values(con, nid, occ_id)}

    # ---------------- 读取结果（分页） ----------------
    def rows(self, run_id, page=0, page_size=100):
        """最终结果分页：每项带稳定 row_ref；页大小由调用方指定。"""
        self._check_page(page, page_size)
        con, cat, meta = self._open_ok(run_id)
        try:
            root = meta["root_node"]
            cols = self._node_cols(con, root)
            total = con.execute("SELECT COUNT(*) FROM occs WHERE node_id=?",
                                (root,)).fetchone()[0]
            cur = con.execute(
                f"SELECT v.* FROM occs o JOIN n{root} v ON v.occ_id=o.occ_id"
                f" WHERE o.node_id=? ORDER BY o.seq LIMIT ? OFFSET ?",
                (root, page_size, page * page_size))
            items = []
            for r in cur:
                r = tuple(r)
                items.append({"row_ref": f"{run_id}:{r[0]}", "seq": r[1],
                              "values": [_dec(v, c[2])
                                         for v, c in zip(r[2:], cols)]})
            return {"run_id": run_id, "state": "ok",
                    "columns": [c[0] for c in cols], "total": total,
                    "page": page, "page_size": page_size, "items": items}
        finally:
            con.close()

    # ---------------- 展开证据（逐层、分页） ----------------
    @staticmethod
    def parse_ref(row_ref):
        run_id, sep, occ = row_ref.rpartition(":")
        if not sep or not run_id or not occ.isdigit():
            raise LineageError(f"非法 row_ref: {row_ref!r}（形如 run_id:occ_id）")
        return run_id, int(occ)

    def expand(self, row_ref, page=0, page_size=100):
        """展开一个出现的直接推导（一层）。备选/成员分页返回；
        返回的子 row_ref 可继续逐步展开，不把全部后代塞进一页。"""
        self._check_page(page, page_size)
        run_id, occ_id = self.parse_ref(row_ref)
        con, cat, meta = self._open_ok(run_id)
        try:
            row = con.execute("SELECT node_id, seq FROM occs WHERE occ_id=?",
                              (occ_id,)).fetchone()
            if row is None:
                raise LineageNotFound(f"run {run_id} 中无此出现: {row_ref}")
            nid = row["node_id"]
            node = self._node(con, nid)
            deriv = self._derivation(con, run_id, nid, node, occ_id,
                                     page, page_size)
            return {"run_id": run_id, "row_ref": row_ref, "seq": row["seq"],
                    "node": {"path": node["path"], "op": node["op"],
                             "detail": node["detail"]},
                    "columns": [c[0] for c in node["cols"]],
                    "values": self._values(con, nid, occ_id),
                    "derivation": deriv}
        finally:
            con.close()

    def _derivation(self, con, run_id, nid, node, occ_id, page, page_size):
        op = node["op"]
        if op == "source":
            r = con.execute(
                "SELECT s.* FROM occ_src os JOIN src_rows s ON s.src_id=os.src_id"
                " WHERE os.occ_id=?", (occ_id,)).fetchone()
            return {"kind": "scan",
                    "source_row": {"source": r["source"],
                                   "instance_id": r["instance_id"],
                                   "table": r["tbl"], "at": r["at"],
                                   "pk": json.loads(r["pk"]),
                                   "path": r["path"]},
                    "note": "叶子：源行身份（逻辑源/实例/表/读取版本/主键/输入位置）"}
        if op in ("filter", "project", "sort", "limit"):
            e = con.execute(
                "SELECT to_occ FROM edges WHERE from_occ=? AND slot='input'",
                (occ_id,)).fetchone()
            return {"kind": "pass", "op": op,
                    "input": self._child_brief(con, run_id, e["to_occ"]),
                    "note": "直通：该出现由其唯一输入出现原样推出（all，共同成立）"}
        if op == "join":
            edges = con.execute(
                "SELECT slot,to_occ,aux FROM edges WHERE from_occ=?"
                " ORDER BY edge_id", (occ_id,)).fetchall()
            items = []
            for e in edges:
                if e["to_occ"] is None:
                    aux = json.loads(e["aux"])
                    items.append({"slot": e["slot"],
                                  "marker": aux["marker"], "note": aux["note"]})
                else:
                    item = {"slot": e["slot"]}
                    item.update(self._child_brief(con, run_id, e["to_occ"]))
                    items.append(item)
            return {"kind": "join", "join_kind": node["detail"]["kind"],
                    "edges": items,
                    "note": "共同成立：左、右依据必须同时存在（all）；"
                            "无匹配的右侧是 null_fill 标记而非虚构源行"}
        if op == "distinct":
            total = con.execute(
                "SELECT COUNT(*) FROM edges WHERE from_occ=? AND slot='alt'",
                (occ_id,)).fetchone()[0]
            cur = con.execute(
                "SELECT to_occ FROM edges WHERE from_occ=? AND slot='alt'"
                " ORDER BY edge_id LIMIT ? OFFSET ?",
                (occ_id, page_size, page * page_size))
            items = [self._child_brief(con, run_id, r["to_occ"]) for r in cur]
            return {"kind": "distinct",
                    "alternatives": {"total": total, "page": page,
                                     "page_size": page_size, "items": items},
                    "note": "备选推导：任一输入出现都足以推出该结果（any）；"
                            "它们是同一业务行的备选依据，不是多份业务记录"}
        if op == "aggregate":
            detail = node["detail"]
            values = self._values(con, nid, occ_id)
            group_key = {g: v for g, v in zip(detail["group_by"], values)}
            aggs = []
            for r in con.execute(
                    "SELECT * FROM agg_summ WHERE group_occ=? ORDER BY out",
                    (occ_id,)).fetchall():
                f = r["f"]
                value = r["value"]
                if value is not None:
                    value = int(value)
                aggs.append({"out": r["out"], "f": f, "field": r["field"],
                             "value": value, "counted": r["counted"],
                             "null_skipped": r["null_skipped"]})
            total = con.execute(
                "SELECT COUNT(*) FROM edges WHERE from_occ=? AND slot='member'",
                (occ_id,)).fetchone()[0]
            cur = con.execute(
                "SELECT to_occ, aux FROM edges WHERE from_occ=?"
                " AND slot='member' ORDER BY edge_id LIMIT ? OFFSET ?",
                (occ_id, page_size, page * page_size))
            items = []
            for r in cur:
                brief = self._child_brief(con, run_id, r["to_occ"])
                brief["contribs"] = json.loads(r["aux"])
                items.append(brief)
            return {"kind": "aggregate",
                    "group_by": detail["group_by"], "group_key": group_key,
                    "aggregates": aggs,
                    "empty_input": total == 0,
                    "members": {"total": total, "page": page,
                                "page_size": page_size, "items": items},
                    "note": "逐份计入：每个成员出现都实际计入汇总（agg）；"
                            "counted 提供非null值，null_skipped 因null未计入；"
                            "sum 边上保留每个非null贡献的精确值"}
        raise LineageError(f"未知节点算子: {op}")

    # ---------------- 按源行反查（分页） ----------------
    def lookup(self, run_id, source, table, pk, path=None, instance_id=None,
               at=None, page=0, page_size=100):
        """拿源行身份反查本次哪些**最终结果**用到了它（只含实际保留的出现）。

        pk: 主键值列表（如 [3]）。同一张表在查询中出现两次时，可用 path
        限定输入位置；不限定时每个命中项都带 via.path 标明走的是哪个位置。
        """
        self._check_page(page, page_size)
        con, cat, meta = self._open_ok(run_id)
        try:
            where = ["source=?", "tbl=?", "pk=?"]
            args = [source, table, _j(list(pk))]
            if path is not None:
                where.append("path=?")
                args.append(path)
            if instance_id is not None:
                where.append("instance_id=?")
                args.append(instance_id)
            if at is not None:
                where.append("at=?")
                args.append(at)
            srcs = con.execute(
                f"SELECT * FROM src_rows WHERE {' AND '.join(where)}",
                args).fetchall()
            identities = [{"source": s["source"], "instance_id": s["instance_id"],
                           "table": s["tbl"], "at": s["at"],
                           "pk": json.loads(s["pk"]), "path": s["path"]}
                          for s in srcs]
            if not srcs:
                return {"run_id": run_id, "state": "ok",
                        "matched_identities": [], "total": 0, "page": page,
                        "page_size": page_size, "items": []}
            ids = [s["src_id"] for s in srcs]
            ph = ",".join("?" for _ in ids)
            total = con.execute(
                f"SELECT COUNT(*) FROM usage u JOIN src_rows s"
                f" ON s.src_id=u.src_id WHERE u.src_id IN ({ph})",
                ids).fetchone()[0]
            rows = con.execute(
                f"SELECT u.root_occ, s.path AS via_path FROM usage u"
                f" JOIN src_rows s ON s.src_id=u.src_id"
                f" WHERE u.src_id IN ({ph})"
                f" ORDER BY u.root_occ, s.path LIMIT ? OFFSET ?",
                ids + [page_size, page * page_size]).fetchall()
            root = meta["root_node"]
            items = []
            for r in rows:
                occ = r["root_occ"]
                seq = con.execute("SELECT seq FROM occs WHERE occ_id=?",
                                  (occ,)).fetchone()["seq"]
                items.append({"row_ref": f"{run_id}:{occ}", "seq": seq,
                              "values": self._values(con, root, occ),
                              "via": {"path": r["via_path"]}})
            return {"run_id": run_id, "state": "ok",
                    "matched_identities": identities,
                    "total": total, "page": page, "page_size": page_size,
                    "items": items}
        finally:
            con.close()
