"""本地执行器：远端子计划经 HTTP 流入 SQLite（落盘），剩余语义在本地完成。

- 连接、distinct、排序/limit 走 SQLite（临时库落盘，不把全部联查结果常驻 Python）；
- 分组聚合在 Python 流式完成（sum 用任意精度整数），结果回注 SQLite 表；
  sum 列以十进制文本存储并注册 NUMTEXT 排序规则，保证大整数精确与正确排序；
- 输出为 JSONL 流：首行 {"status":"incomplete"}，成功结束才追加 {"status":"ok",...}；
  中途失败只写 {"status":"error",...} —— 部分流永远没有成功结束标记。
"""

from __future__ import annotations

import json
import os
import sqlite3
import time

from . import expr as E
from . import semantics as S


class QueryFailed(Exception):
    def __init__(self, msg, stats=None):
        super().__init__(msg)
        self.stats = stats or {}


def _q(name):
    return '"' + name.replace('"', '""') + '"'


def _numtext_cmp(a, b):
    x, y = int(a), int(b)
    return (x > y) - (y > x)


# 存储类型：int/bool → INTEGER，text → TEXT，sumtext → TEXT COLLATE NUMTEXT（大整数精确）
def _storage(logical):
    return {"int": "int", "text": "text", "bool": "bool"}[logical]


class Executor:
    def __init__(self, clients, workdir):
        self.clients = clients          # {'A': Client, 'B': Client}
        self.workdir = workdir
        os.makedirs(workdir, exist_ok=True)
        db_path = os.path.join(workdir, "local.db")
        if os.path.exists(db_path):
            os.remove(db_path)
        if tuple(int(x) for x in sqlite3.sqlite_version.split(".")) < (3, 30):
            raise RuntimeError("需要 SQLite >= 3.30（NULLS FIRST/LAST）")
        self.db = sqlite3.connect(db_path)
        self.db.execute("PRAGMA journal_mode=OFF")
        self.db.execute("PRAGMA synchronous=OFF")
        self.db.create_collation("NUMTEXT", _numtext_cmp)
        self._t = 0

    # ---------------- 远端扫描物化 ----------------
    def _fresh(self):
        self._t += 1
        return f"t{self._t}"

    def _create_table(self, name, cols, storage):
        defs = []
        for c in cols:
            st = storage[c]
            if st == "text":
                defs.append(f"{_q(c)} TEXT")
            elif st == "sumtext":
                defs.append(f"{_q(c)} TEXT COLLATE NUMTEXT")
            else:
                defs.append(f"{_q(c)} INTEGER")
        self.db.execute(f"CREATE TABLE {_q(name)} ({', '.join(defs)})")

    def _materialize(self, node):
        sub = node.remote
        client = self.clients[sub.source]
        table = self._fresh()
        cols = node.schema.names()
        storage = {}
        for c, t in node.schema.cols:
            if sub.aggregate is not None and c not in sub.aggregate.group_by \
                    and any(a.out == c and a.f == "sum" for a in sub.aggregate.aggs):
                storage[c] = "sumtext"
            else:
                storage[c] = _storage(t)
        self._create_table(table, cols, storage)
        marks = ", ".join("?" for _ in cols)
        sql = (f"INSERT INTO {_q(table)} "
               f"({', '.join(_q(c) for c in cols)}) VALUES ({marks})")
        batch = []
        for row in client.stream(sub.to_json()):
            batch.append(tuple(self._encode(row[c], storage[c]) for c in cols))
            if len(batch) >= 500:
                self.db.executemany(sql, batch)
                batch.clear()
        if batch:
            self.db.executemany(sql, batch)
        self.db.commit()
        return table, storage

    @staticmethod
    def _encode(v, st):
        if v is None:
            return None
        if st == "bool":
            return 1 if v else 0
        if st == "sumtext":
            return str(v)
        return v

    # ---------------- 计划编译为 SQL ----------------
    def _compile(self, node):
        """返回 (sql, params, schema, storage_map)。"""
        k = node.kind
        if k == "remote_scan":
            table, storage = self._materialize(node)
            cols = ", ".join(_q(c) for c in node.schema.names())
            return f"SELECT {cols} FROM {_q(table)}", [], node.schema, storage
        if k == "filter":
            sql, params, sch, st = self._compile(node.children[0])
            psql, pparams = E.to_sql(node.pred)
            return (f"SELECT * FROM ({sql}) WHERE {psql}",
                    params + pparams, sch, st)
        if k == "project":
            sql, params, sch, st = self._compile(node.children[0])
            cols = ", ".join(_q(c) for c in node.cols)
            return f"SELECT {cols} FROM ({sql})", params, node.schema, st
        if k == "join":
            lsql, lparams, lsch, lst = self._compile(node.children[0])
            rsql, rparams, rsch, rst = self._compile(node.children[1])
            jt = "LEFT JOIN" if node.jkind == "left" else "JOIN"
            sel = [f"l.{_q(c)} AS {_q(c)}" for c in lsch.names()]
            for out_name, orig in node.right_name_map.items():
                sel.append(f"r.{_q(orig)} AS {_q(out_name)}")
            sql = (f"SELECT {', '.join(sel)} FROM ({lsql}) l {jt} ({rsql}) r "
                   f"ON l.{_q(node.lkey)} = r.{_q(node.rkey)}")
            st = dict(lst)
            for out_name, orig in node.right_name_map.items():
                st[out_name] = rst[orig]
            return sql, lparams + rparams, node.schema, st
        if k == "aggregate":
            return self._aggregate(node)
        if k == "distinct":
            sql, params, sch, st = self._compile(node.children[0])
            return f"SELECT DISTINCT * FROM ({sql})", params, node.schema, st
        if k == "sort":
            sql, params, sch, st = self._compile(node.children[0])
            terms = []
            for key in node.keys:
                t = _q(key.field)
                if st[key.field] == "sumtext":
                    t += " COLLATE NUMTEXT"
                t += " ASC" if key.asc else " DESC"
                t += " NULLS FIRST" if key.nulls_first else " NULLS LAST"
                terms.append(t)
            return (f"SELECT * FROM ({sql}) ORDER BY {', '.join(terms)}",
                    params, sch, st)
        if k == "limit":
            sql, params, sch, st = self._compile(node.children[0])
            return f"SELECT * FROM ({sql}) LIMIT {node.n}", params, sch, st
        raise QueryFailed(f"执行器未支持的节点: {k}")

    def _aggregate(self, node):
        sql, params, sch, st = self._compile(node.children[0])
        names = sch.names()
        gidx = [names.index(g) for g in node.group_by]
        aidx = [(a, names.index(a.field) if a.field else None) for a in node.aggs]
        groups = {}
        order = []
        for row in self.db.execute(sql, params):
            key = tuple(row[i] for i in gidx)
            g = groups.get(key)
            if g is None:
                g = groups[key] = {a.out: [0, 0, False] for a in node.aggs}
                order.append(key)
            for a, i in aidx:
                acc = g[a.out]
                if a.f == "count_star":
                    acc[0] += 1
                else:
                    v = row[i]
                    if v is not None:
                        acc[1] += 1
                        acc[0] += v if a.f == "sum" else 1
                        acc[2] = True
        if not node.group_by and not order:
            order.append(())
            groups[()] = {a.out: [0, 0, False] for a in node.aggs}
        table = self._fresh()
        storage = {}
        for c, t in node.schema.cols:
            storage[c] = "sumtext" if any(a.out == c and a.f == "sum"
                                          for a in node.aggs) else _storage(t)
        self._create_table(table, node.schema.names(), storage)
        marks = ", ".join("?" for _ in node.schema.cols)
        ins = f"INSERT INTO {_q(table)} VALUES ({marks})"
        for key in order:
            g = groups[key]
            vals = list(key)
            for a in node.aggs:
                acc = g[a.out]
                if a.f == "sum":
                    vals.append(str(acc[0]) if acc[2] else None)
                else:
                    vals.append(acc[0])
            self.db.execute(ins, vals)
        self.db.commit()
        cols = ", ".join(_q(c) for c in node.schema.names())
        return f"SELECT {cols} FROM {_q(table)}", [], node.schema, storage

    # ---------------- 输出 ----------------
    @staticmethod
    def _decode(v, st):
        if v is None:
            return None
        if st == "bool":
            return bool(v)
        if st == "sumtext":
            return int(v)
        return v

    def execute(self, plan, out_path, collect=1000):
        """执行物理计划，流式写出 JSONL。返回 (stats, rows_or_None)。"""
        t0 = time.time()
        rows_out = 0
        collected = [] if collect else None
        stats = {}
        try:
            sql, params, sch, storage = self._compile(plan)
            with open(out_path, "w", encoding="utf-8") as f:
                f.write(json.dumps({"status": "incomplete",
                                    "columns": sch.names()},
                                   ensure_ascii=False) + "\n")
                for row in self.db.execute(sql, params):
                    vals = [self._decode(v, storage[c])
                            for v, c in zip(row, sch.names())]
                    f.write(json.dumps({"row": vals}, ensure_ascii=False) + "\n")
                    rows_out += 1
                    if collected is not None and len(collected) < collect:
                        collected.append(vals)
                stats = self._stats(rows_out, t0)
                f.write(json.dumps({"status": "ok", "rows": rows_out,
                                    "stats": stats}, ensure_ascii=False) + "\n")
            return stats, collected
        except Exception as e:
            stats = self._stats(rows_out, t0)
            try:
                with open(out_path, "a", encoding="utf-8") as f:
                    f.write(json.dumps({"status": "error", "error": str(e),
                                        "stats": stats}, ensure_ascii=False) + "\n")
            except OSError:
                pass
            if isinstance(e, QueryFailed):
                raise
            raise QueryFailed(str(e), stats) from e

    def _stats(self, rows_out, t0):
        return {"sources": {name: c.stats.to_json()
                            for name, c in self.clients.items()},
                "rows_out": rows_out,
                "elapsed_ms": int((time.time() - t0) * 1000)}


def read_result(path):
    """读取 JSONL 结果文件；无 ok 结束标记 → complete=False（部分流不得视为完整）。"""
    cols, rows, status, stats = None, [], "missing", {}
    with open(path, encoding="utf-8") as f:
        for line in f:
            rec = json.loads(line)
            if "row" in rec:
                rows.append(rec["row"])
            elif rec.get("status") == "incomplete":
                cols = rec["columns"]
            elif rec.get("status") == "ok":
                status, stats = "ok", rec.get("stats", {})
            elif rec.get("status") == "error":
                status, stats = "error", rec.get("stats", {})
    return {"complete": status == "ok", "status": status,
            "columns": cols, "rows": rows, "stats": stats}
