"""视图持久化：目录库 + 每视图冻结库（SQLite 落盘）。

保证方式选择**本地完整冻结**：打开视图时把各源恰好截至 F[s] 的真实表行拉回，
按视图分库写入 SQLite；之后查询不再接触源，源继续发布、删除、淘汰都不影响结果。
整批源表不常驻 Python 内存——按页读取、批量 INSERT，查询时再流式读出。

目录库 views.db 保存：
- 视图身份、状态（open/closed/failed）、选择向量 F、观察盒 H/L、退回解释；
- 每个源的实例身份、提交位置、字段声明、冻结库名与源端快照号（读取依据）；
- 建立期每一页真实 HTTP 读取（可核对实例、提交位置、视图身份）；
- 每次查询运行记录。
协调进程重启后仅凭目录库与冻结库即可继续执行原 view_id 的查询。
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
import uuid


class ViewError(Exception):
    pass


class ViewClosed(ViewError):
    pass


def _now():
    return time.strftime("%Y-%m-%dT%H:%M:%S")


class ViewStore:
    def __init__(self, workdir):
        self.workdir = workdir
        self.viewdir = os.path.join(workdir, "views")
        os.makedirs(self.viewdir, exist_ok=True)
        self.db_path = os.path.join(workdir, "views.db")
        self.lock = threading.RLock()
        self._active = {}          # view_id -> 进行中的查询数
        self.db = sqlite3.connect(self.db_path, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self):
        with self.db:
            self.db.executescript(
                """
                CREATE TABLE IF NOT EXISTS views(
                    view_id TEXT PRIMARY KEY,
                    state TEXT NOT NULL,                 -- open | closed | failed
                    created TEXT, closed TEXT,
                    f TEXT, h TEXT, l TEXT, trace TEXT, retreat TEXT,
                    config TEXT, note TEXT
                );
                CREATE TABLE IF NOT EXISTS view_sources(
                    view_id TEXT, source TEXT,
                    instance_id TEXT, at INTEGER, head INTEGER, earliest INTEGER,
                    url TEXT, dialect TEXT, schema_json TEXT,
                    snapshot_id TEXT, frozen_db TEXT, rows_total INTEGER,
                    PRIMARY KEY(view_id, source)
                );
                CREATE TABLE IF NOT EXISTS http_reads(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    view_id TEXT, source TEXT, instance_id TEXT,
                    at INTEGER, snapshot_id TEXT, "table" TEXT,
                    page INTEGER, rows INTEGER, bytes INTEGER, ts TEXT
                );
                CREATE TABLE IF NOT EXISTS query_runs(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    view_id TEXT, name TEXT, state TEXT,
                    columns TEXT, rows_out INTEGER, detail TEXT, ts TEXT
                );
                CREATE TABLE IF NOT EXISTS build_staging(
                    view_id TEXT, source TEXT, url TEXT,
                    snapshot_id TEXT, frozen_db TEXT
                );
                """)

    # ------------------------------------------------------------ 建立期
    def begin_build(self, config, note=""):
        view_id = "v-" + uuid.uuid4().hex[:16]
        with self.lock:
            with self.db:
                self.db.execute(
                    "INSERT INTO views(view_id,state,created,config,note) "
                    "VALUES(?,?,?,?,?)",
                    (view_id, "failed", _now(), json.dumps(config, ensure_ascii=False), note))
        return view_id

    def new_frozen_db(self, view_id):
        path = os.path.join(self.viewdir, f"{view_id}.frozen.db")
        con = sqlite3.connect(path)
        con.execute("CREATE TABLE frozen_rows("
                    "tbl TEXT, page INTEGER, rowidx INTEGER, jrow TEXT)")
        return path, con

    def insert_frozen_rows(self, con, table, page, rows):
        con.executemany(
            "INSERT INTO frozen_rows(tbl,page,rowidx,jrow) VALUES(?,?,?,?)",
            [(table, page, i, json.dumps(r, ensure_ascii=False))
             for i, r in enumerate(rows)])

    def stage_protection(self, view_id, source, url, snapshot_id, frozen_db):
        """建立期每取得一个源端保护就登记，保证失败清理能按记录释放。"""
        with self.lock:
            with self.db:
                self.db.execute(
                    "INSERT INTO build_staging(view_id,source,url,snapshot_id,"
                    "frozen_db) VALUES(?,?,?,?,?)",
                    (view_id, source, url, snapshot_id, frozen_db))

    def staging(self, view_id):
        with self.lock:
            rows = self.db.execute(
                "SELECT source,url,snapshot_id,frozen_db FROM build_staging "
                "WHERE view_id=?", (view_id,)).fetchall()
            return [dict(r) for r in rows]

    def record_http_read(self, view_id, source, instance_id, at, snapshot_id,
                         table, page, rows, nbytes):
        with self.lock:
            with self.db:
                self.db.execute(
                    "INSERT INTO http_reads(view_id,source,instance_id,at,"
                    "snapshot_id,\"table\",page,rows,bytes,ts) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (view_id, source, instance_id, at, snapshot_id, table,
                     page, rows, nbytes, _now()))

    def commit_view(self, view_id, F, H, L, trace, retreat, source_records):
        """全部源冻结成功后原子登记为 open。"""
        with self.lock:
            with self.db:
                self.db.execute(
                    "UPDATE views SET state='open', f=?, h=?, l=?, trace=?, retreat=? "
                    "WHERE view_id=?",
                    (json.dumps(F), json.dumps(H), json.dumps(L),
                     json.dumps(trace, ensure_ascii=False),
                     json.dumps(retreat, ensure_ascii=False), view_id))
                for rec in source_records:
                    self.db.execute(
                        "INSERT INTO view_sources(view_id,source,instance_id,at,head,"
                        "earliest,url,dialect,schema_json,snapshot_id,frozen_db,rows_total)"
                        " VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                        (view_id, rec["source"], rec["instance_id"], rec["at"],
                         rec["head"], rec["earliest"], rec["url"], rec["dialect"],
                         json.dumps(rec["schema_json"], ensure_ascii=False),
                         rec["snapshot_id"], rec["frozen_db"], rec["rows_total"]))
                self.db.execute("DELETE FROM build_staging WHERE view_id=?",
                                (view_id,))

    def fail_build(self, view_id, error):
        with self.lock:
            with self.db:
                self.db.execute("UPDATE views SET state='failed', note=? WHERE view_id=?",
                                (f"建立失败: {error}", view_id))

    def cleanup_failed(self, view_id, release_cb=None):
        """清理失败建立遗留：释放已取得的源保护、删除半成品冻结库。不碰其他视图。"""
        with self.lock:
            row = self.db.execute("SELECT state FROM views WHERE view_id=?",
                                  (view_id,)).fetchone()
            if row is None:
                raise ViewError(f"未知 view_id: {view_id}")
            if row["state"] != "failed":
                raise ViewError(f"视图 {view_id} 状态为 {row['state']}，"
                                f"清理入口只处理 failed 的建立遗留")
            staging = self.staging(view_id)
            recs = self.db.execute(
                "SELECT source,snapshot_id,frozen_db FROM view_sources WHERE view_id=?",
                (view_id,)).fetchall()
            released = []
            for st in staging:
                if st["snapshot_id"] and release_cb is not None:
                    release_cb(st["source"], st["snapshot_id"])
                    released.append(st["snapshot_id"])
            for r in recs:
                if r["snapshot_id"] and release_cb is not None:
                    release_cb(r["source"], r["snapshot_id"])
                    released.append(r["snapshot_id"])
            # 失败建立通常没有 view_sources；冻结库若已建则删除
            for r in recs:
                if r["frozen_db"]:
                    self._safe_remove(r["frozen_db"])
            stray = os.path.join(self.viewdir, f"{view_id}.frozen.db")
            self._safe_remove(stray)
            with self.db:
                self.db.execute("DELETE FROM http_reads WHERE view_id=?", (view_id,))
                self.db.execute("DELETE FROM view_sources WHERE view_id=?", (view_id,))
                self.db.execute("DELETE FROM build_staging WHERE view_id=?", (view_id,))
                self.db.execute("DELETE FROM views WHERE view_id=?", (view_id,))
            return {"released_snapshots": released, "removed": [stray]}

    @staticmethod
    def _safe_remove(path):
        try:
            if path and os.path.exists(path):
                os.remove(path)
                return True
        except OSError:
            return False
        return False

    # ------------------------------------------------------------ 查询期
    def get_open_view(self, view_id):
        with self.lock:
            row = self.db.execute("SELECT * FROM views WHERE view_id=?",
                                  (view_id,)).fetchone()
            if row is None:
                raise ViewError(f"未知 view_id: {view_id}")
            if row["state"] != "open":
                raise ViewClosed(f"视图 {view_id} 状态为 {row['state']}，拒绝新查询")
            return dict(row)

    def view_sources(self, view_id):
        with self.lock:
            rows = self.db.execute(
                "SELECT * FROM view_sources WHERE view_id=? ORDER BY source",
                (view_id,)).fetchall()
            return [dict(r) for r in rows]

    def frozen_db_path(self, view_id):
        return os.path.join(self.viewdir, f"{view_id}.frozen.db")

    def begin_query(self, view_id, name):
        """登记查询开始；若视图已关闭则拒绝。进行中的计数保证关闭不打断在跑查询。"""
        with self.lock:
            row = self.db.execute("SELECT state FROM views WHERE view_id=?",
                                  (view_id,)).fetchone()
            if row is None:
                raise ViewError(f"未知 view_id: {view_id}")
            if row["state"] != "open":
                raise ViewClosed(f"视图 {view_id} 已 {row['state']}，拒绝新查询")
            self._active[view_id] = self._active.get(view_id, 0) + 1

    def end_query(self, view_id, name, state, columns, rows_out, detail):
        with self.lock:
            self._active[view_id] = max(0, self._active.get(view_id, 1) - 1)
            with self.db:
                self.db.execute(
                    "INSERT INTO query_runs(view_id,name,state,columns,rows_out,detail,ts)"
                    " VALUES(?,?,?,?,?,?,?)",
                    (view_id, name, state,
                     json.dumps(columns, ensure_ascii=False), rows_out, detail, _now()))

    def active_queries(self, view_id):
        with self.lock:
            return self._active.get(view_id, 0)

    def close(self, view_id):
        """关闭：拒绝新查询；进行中的查询允许跑完，再回收冻结副本。

        POSIX 下已打开的 sqlite 连接在文件 unlink 后仍可读完，因此这里立即解除链接，
        既保证关闭后没有新连接能打开该副本，又不打断正在跑的查询句柄。
        """
        with self.lock:
            row = self.db.execute("SELECT * FROM views WHERE view_id=?",
                                  (view_id,)).fetchone()
            if row is None:
                raise ViewError(f"未知 view_id: {view_id}")
            if row["state"] == "closed":
                return {"already": True}
            if row["state"] != "open":
                raise ViewError(f"视图 {view_id} 状态 {row['state']} 不可关闭")
            active = self.active_queries(view_id)
            with self.db:
                self.db.execute(
                    "UPDATE views SET state='closed', closed=? WHERE view_id=?",
                    (_now(), view_id))
            removed = self._safe_remove(self.frozen_db_path(view_id))
            return {"closed": True, "frozen_db_removed": removed,
                    "active_at_close": active}

    def list_views(self):
        with self.lock:
            rows = self.db.execute(
                "SELECT view_id,state,created,closed,f,h,l,note FROM views "
                "ORDER BY rowid").fetchall()
            return [dict(r) for r in rows]

    def describe(self, view_id):
        with self.lock:
            v = self.db.execute("SELECT * FROM views WHERE view_id=?",
                                (view_id,)).fetchone()
            if v is None:
                raise ViewError(f"未知 view_id: {view_id}")
            srcs = self.view_sources(view_id)
            reads = self.db.execute(
                "SELECT source,instance_id,at,\"table\",page,rows,bytes "
                "FROM http_reads WHERE view_id=? ORDER BY id", (view_id,)).fetchall()
            runs = self.db.execute(
                "SELECT name,state,rows_out,ts FROM query_runs WHERE view_id=? "
                "ORDER BY id", (view_id,)).fetchall()
            d = dict(v)
            for key in ("f", "h", "l", "trace", "retreat", "config"):
                if d.get(key):
                    d[key] = json.loads(d[key])
            d["sources"] = [{**r, "schema_json": json.loads(r["schema_json"])}
                            for r in srcs]
            d["http_reads"] = [dict(r) for r in reads]
            d["query_runs"] = [dict(r) for r in runs]
            return d
