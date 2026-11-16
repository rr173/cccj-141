"""版本化 HTTP 模拟数据源。

与第一版的固定快照服务（server.py）不同，本服务持有一份**真实的提交历史**：
- GET  /metadata                → 开放前缀的实例身份、head/earliest、表声明与每笔提交依赖；
- POST /snapshot/open  {"at": k, "view_id": ...}
        对版本 k 建立读取保护并返回分页清单（同源多表、整个版本一体，不可拆开）；
- POST /snapshot/page  {"snapshot_id", "table", "page"}
        按页返回版本 k 的真实表行；
- POST /snapshot/release {"snapshot_id"}
        释放该保护（引用计数；多个视图/快照钉住同一版本时互不影响）。
- POST /changes/open  {"after": k, "subscription_id": ...}
        打开变更会话：覆盖 k+1..head 的完整提交（upsert/delete + 提交号），
        按提交分页；after 已淘汰（历史断档）→ 409。
- POST /changes/page  {"change_id", "page"}
        取一页变更；同一提交可跨页，commit_complete 标明该提交何时读完，
        订阅者不会把跨页的一笔提交当成多个可见状态。
- POST /changes/release {"change_id"} 释放变更会话。

管理接口（仅用于可复做演示，不参与查询协议）：
- POST /admin/publish   {"head": n}        向前推进开放前缀；
- POST /admin/earliest  {"earliest": n}    淘汰旧状态（受保护版本钉住不失效）；
- POST /admin/replace-instance {...}       同地址换 instance_id（旧快照全部作废）；
- POST /admin/fail      {"route": ...}     注入下一次请求失败；
- GET  /admin/state                         查看 head/earliest/保护计数/实例身份。

保护语义：版本 at 被快照持有期间 earliest 推进不能淘汰它；保护按引用计数，
多个视图引用同一版本时关闭其中一个不破坏另一个。
"""

from __future__ import annotations

import json
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .versions import HistoryError


class VersionSource:
    """持有历史与读取保护的版本源核心（线程安全）。"""

    def __init__(self, history, log_path, max_page_bytes=8192):
        self.hist = history
        self.log_path = log_path
        self.max_page_bytes = max_page_bytes
        self.lock = threading.RLock()
        self.snapshots = {}          # snapshot_id -> {"at", "view_id", "tables", "pages":{..}}
        self.change_sessions = {}    # change_id -> {"after", "subscription_id", "pages"}
        self.fail_next = None        # 注入：下一次某路由失败
        self.request_log = []        # 进程内留一份，方便演示脚本核对

    # ---- 日志 ----
    def log(self, rec):
        rec["ts"] = time.strftime("%Y-%m-%dT%H:%M:%S")
        rec["source"] = self.hist.name
        line = json.dumps(rec, ensure_ascii=False)
        with self.lock:
            self.request_log.append(rec)
            with open(self.log_path, "a", encoding="utf-8") as f:
                f.write(line + "\n")

    # ---- 保护型快照 ----
    def open_snapshot(self, at, view_id):
        with self.lock:
            if self.fail_next == "snapshot/open":
                self.fail_next = None
                raise RuntimeError("injected failure on snapshot/open")
            if not isinstance(at, int) or isinstance(at, bool):
                raise HistoryError("at 必须是整数（bool 不允许）")
            if at < 0 or at > self.hist.head:
                raise HistoryError(
                    f"版本 {at} 未开放（head={self.hist.head}）")
            self.hist.protect(at)          # 不可读（已淘汰无保护）时抛 HistoryError
            state = self.hist.state_at(at)
            tables, pages_map = {}, {}
            for t, rows_by_key in state.items():
                cols = list(self.hist.schemas[t]["columns"].keys())
                types = [self.hist.schemas[t]["columns"][c] for c in cols]
                rows = [list(v) for v in rows_by_key.values()]
                pages = self._paginate(cols, rows)
                tables[t] = {"columns": cols, "types": types,
                             "rows_total": len(rows), "pages": len(pages)}
                pages_map[t] = pages
            snap_id = uuid.uuid4().hex
            self.snapshots[snap_id] = {"at": at, "view_id": view_id,
                                       "tables": tables, "pages": pages_map}
            self.log({"route": "snapshot/open", "snapshot_id": snap_id,
                      "view_id": view_id, "at": at,
                      "instance_id": self.hist.instance_id,
                      "tables": {t: d["rows_total"] for t, d in tables.items()}})
            return snap_id, at, tables

    def page(self, snap_id, table, page):
        with self.lock:
            if self.fail_next == "/snapshot/page":
                self.fail_next = None
                raise RuntimeError("injected failure on snapshot/page")
            snap = self.snapshots.get(snap_id)
            if snap is None:
                raise KeyError(f"未知快照 {snap_id}（实例可能已被替换）")
            if table not in snap["pages"]:
                raise KeyError(f"快照无表 {table}")
            pages = snap["pages"][table]
            if not isinstance(page, int) or isinstance(page, bool) \
                    or not (0 <= page < len(pages)):
                raise IndexError(f"页码 {page} 越界 0..{len(pages) - 1}")
            rows = pages[page]
            meta = snap["tables"][table]
            self.log({"route": "snapshot/page", "snapshot_id": snap_id,
                      "view_id": snap["view_id"], "at": snap["at"],
                      "instance_id": self.hist.instance_id,
                      "table": table, "page": page, "rows": len(rows)})
            return {"snapshot_id": snap_id, "table": table, "at": snap["at"],
                    "view_id": snap["view_id"],
                    "instance_id": self.hist.instance_id,
                    "columns": meta["columns"], "types": meta["types"],
                    "rows": rows, "page": page,
                    "next_page": page + 1 if page + 1 < len(pages) else None}

    def release(self, snap_id):
        with self.lock:
            snap = self.snapshots.pop(snap_id, None)
            if snap is None:
                return False
            self.hist.release(snap["at"])
            self.log({"route": "snapshot/release", "snapshot_id": snap_id,
                      "view_id": snap["view_id"], "at": snap["at"],
                      "instance_id": self.hist.instance_id})
            return True

    # ---- 按位置取完整提交（持续查询订阅的变更入口）----
    def open_changes(self, after, subscription_id):
        """打开一个变更读取会话：覆盖 after+1..head 的完整提交，按提交分页。

        每笔提交是不可分割的可见状态：一页只属于一笔提交，响应显式带
        commit_complete；同一提交跨多页时订阅者不会把它当成几个状态。
        after < earliest（历史断档）或 after > head → HistoryError（HTTP 409）。
        """
        with self.lock:
            if self.fail_next == "changes/open":
                self.fail_next = None
                raise RuntimeError("injected failure on changes/open")
            if not isinstance(after, int) or isinstance(after, bool):
                raise HistoryError("after 必须是整数（bool 不允许）")
            if after < 0 or after > self.hist.head:
                raise HistoryError(
                    f"变更起点 after={after} 越界（head={self.hist.head}）")
            if after < self.hist.earliest:
                raise HistoryError(
                    f"变更起点 after={after} 已淘汰：当前 earliest="
                    f"{self.hist.earliest}，无法提供 {after + 1} 的完整提交（历史断档）")
            pages = []
            for commit in self.hist.commits[after: self.hist.head]:
                # 每页只承载一笔提交；过大的提交按表/行再切成多页，但仍带同一 seq
                for chunk in self._paginate_commit(commit):
                    pages.append({"seq": commit.seq,
                                  "requires": dict(commit.requires),
                                  "note": commit.note,
                                  "changes": chunk})
            change_id = uuid.uuid4().hex
            self.change_sessions[change_id] = {
                "after": after, "subscription_id": subscription_id, "pages": pages}
            self.log({"route": "changes/open", "change_id": change_id,
                      "subscription_id": subscription_id, "after": after,
                      "head": self.hist.head, "pages": len(pages),
                      "instance_id": self.hist.instance_id})
            return {"change_id": change_id, "after": after,
                    "head": self.hist.head,
                    "instance_id": self.hist.instance_id, "pages": len(pages)}

    def change_page(self, change_id, page):
        with self.lock:
            if self.fail_next == "/changes/page":
                self.fail_next = None
                raise RuntimeError("injected failure on changes/page")
            sess = self.change_sessions.get(change_id)
            if sess is None:
                raise KeyError(f"未知变更会话 {change_id}（实例可能已被替换）")
            pages = sess["pages"]
            if not isinstance(page, int) or isinstance(page, bool) \
                    or not (0 <= page < len(pages)):
                raise IndexError(f"页码 {page} 越界 0..{len(pages) - 1}")
            item = pages[page]
            nxt = page + 1 if page + 1 < len(pages) else None
            # 同一提交可能跨页：下一页仍属同一 seq 时 commit_complete=False
            commit_complete = (nxt is None or pages[nxt]["seq"] != item["seq"])
            self.log({"route": "changes/page", "change_id": change_id,
                      "subscription_id": sess["subscription_id"],
                      "instance_id": self.hist.instance_id,
                      "page": page, "seq": item["seq"],
                      "tables": sorted(item["changes"]),
                      "commit_complete": commit_complete})
            return {"change_id": change_id, "page": page,
                    "next_page": nxt, "seq": item["seq"],
                    "head": self.hist.head,
                    "instance_id": self.hist.instance_id,
                    "requires": item["requires"], "note": item["note"],
                    "changes": item["changes"],
                    "commit_complete": commit_complete,
                    "session_complete": nxt is None}

    def release_changes(self, change_id):
        with self.lock:
            sess = self.change_sessions.pop(change_id, None)
            if sess is None:
                return False
            self.log({"route": "changes/release", "change_id": change_id,
                      "subscription_id": sess["subscription_id"],
                      "instance_id": self.hist.instance_id})
            return True

    def _paginate_commit(self, commit):
        """把一笔提交切成一页或多页；每页是 {table: {"upsert":[..], "delete":[..]}}。

        按行预算切分（与快照分页同一口径）：单页超预算就另开一页，新页重复表名。
        切分只在页边界发生，不改变行内容；同一 seq 的多页由 change_page 的
        commit_complete 标记区分，订阅者必须收齐同一 seq 的全部页才可见该提交。
        """
        budget = self.max_page_bytes
        pages = []
        cur, size = {}, 2

        def flush():
            nonlocal cur, size
            if cur:
                pages.append(cur)
            cur, size = {}, 2

        for t, ch in commit.changes.items():
            part = cur.setdefault(t, {"upsert": [], "delete": []})
            for row in ch.upserts:
                n = len(json.dumps(row, ensure_ascii=False)) + 2
                if cur and size + n > budget:
                    flush()
                    part = cur.setdefault(t, {"upsert": [], "delete": []})
                part["upsert"].append(list(row))
                size += n
            for rid in ch.deletes:
                n = len(json.dumps(rid, ensure_ascii=False)) + 2
                if cur and size + n > budget:
                    flush()
                    part = cur.setdefault(t, {"upsert": [], "delete": []})
                part["delete"].append(rid)
                size += n
        if not cur:
            t0 = next(iter(commit.changes))
            cur = {t0: {"upsert": [], "delete": []}}
        flush()
        return pages

    def _paginate(self, cols, rows):
        budget = self.max_page_bytes
        pages, cur, size = [], [], 2
        for r in rows:
            row = list(r)
            n = len(json.dumps(row, ensure_ascii=False)) + 1
            if cur and size + n > budget:
                pages.append(cur)
                cur, size = [], 2
            cur.append(row)
            size += n
        pages.append(cur)
        return pages

    # ---- 管理动作 ----
    def publish(self, head):
        with self.lock:
            if not isinstance(head, int) or isinstance(head, bool) \
                    or not (self.hist.head <= head <= len(self.hist.commits)):
                raise HistoryError(f"head 只能向前推进到 <= {len(self.hist.commits)}")
            old = self.hist.head
            self.hist.head = head
            self.log({"route": "admin/publish", "from": old, "to": head,
                      "instance_id": self.hist.instance_id})
            return self.hist.head

    def set_earliest(self, earliest):
        with self.lock:
            self.hist.advance_earliest(earliest)
            self.log({"route": "admin/earliest", "earliest": earliest,
                      "protected": dict(self.hist._protections),
                      "instance_id": self.hist.instance_id})
            return self.hist.earliest

    def replace_instance(self, new_instance_id, head=None, earliest=None):
        """同地址换实例：身份改变、历史保护全部作废（新实例不承认旧快照）。"""
        with self.lock:
            old = self.hist.instance_id
            dropped = list(self.snapshots)
            self.snapshots.clear()
            dropped_changes = list(self.change_sessions)
            self.change_sessions.clear()
            self.hist._protections.clear()
            self.hist.instance_id = new_instance_id
            if head is not None:
                self.hist.head = head
            if earliest is not None:
                self.hist.advance_earliest(earliest)
            self.log({"route": "admin/replace-instance", "from": old,
                      "to": new_instance_id, "dropped_snapshots": len(dropped),
                      "dropped_change_sessions": len(dropped_changes)})
            return old, new_instance_id

    def admin_state(self):
        with self.lock:
            return {"source": self.hist.name,
                    "instance_id": self.hist.instance_id,
                    "head": self.hist.head, "earliest": self.hist.earliest,
                    "protected": dict(self.hist._protections),
                    "snapshots": {sid: {"at": s["at"], "view_id": s["view_id"]}
                                  for sid, s in self.snapshots.items()},
                    "change_sessions": {
                        cid: {"after": s["after"],
                              "subscription_id": s["subscription_id"]}
                        for cid, s in self.change_sessions.items()},
                    "commits_total": len(self.hist.commits)}


# ---------------------------------------------------------------- HTTP 封装

def _json_response(h, code, obj):
    body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
    h.send_response(code)
    h.send_header("Content-Type", "application/json; charset=utf-8")
    h.send_header("Content-Length", str(len(body)))
    h.end_headers()
    h.wfile.write(body)


def make_version_handler(src: VersionSource):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def do_GET(self):
            if self.path == "/metadata":
                _json_response(self, 200, src.hist.metadata())
            elif self.path == "/admin/state":
                _json_response(self, 200, src.admin_state())
            else:
                _json_response(self, 404, {"error": "unknown route"})

        def do_POST(self):
            try:
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            except Exception:
                _json_response(self, 400, {"error": "bad json"})
                return
            route = self.path
            try:
                if route == "/snapshot/open":
                    snap_id, at, tables = src.open_snapshot(
                        body["at"], body.get("view_id", "anon"))
                    _json_response(self, 200, {"snapshot_id": snap_id, "at": at,
                                               "instance_id": src.hist.instance_id,
                                               "tables": tables})
                elif route == "/snapshot/page":
                    _json_response(self, 200, src.page(
                        body["snapshot_id"], body["table"], body["page"]))
                elif route == "/snapshot/release":
                    ok = src.release(body["snapshot_id"])
                    _json_response(self, 200 if ok else 409,
                                   {"released": ok})
                elif route == "/changes/open":
                    info = src.open_changes(body["after"],
                                            body.get("subscription_id", "anon"))
                    _json_response(self, 200, info)
                elif route == "/changes/page":
                    _json_response(self, 200, src.change_page(
                        body["change_id"], body["page"]))
                elif route == "/changes/release":
                    ok = src.release_changes(body["change_id"])
                    _json_response(self, 200 if ok else 409,
                                   {"released": ok})
                elif route == "/admin/publish":
                    _json_response(self, 200, {"head": src.publish(body["head"])})
                elif route == "/admin/earliest":
                    _json_response(self, 200,
                                   {"earliest": src.set_earliest(body["earliest"])})
                elif route == "/admin/replace-instance":
                    old, new = src.replace_instance(
                        body["instance_id"], body.get("head"), body.get("earliest"))
                    _json_response(self, 200, {"old": old, "new": new})
                elif route == "/admin/fail":
                    src.fail_next = body["route"]
                    _json_response(self, 200, {"fail_next": src.fail_next})
                else:
                    _json_response(self, 404, {"error": "unknown route"})
            except HistoryError as e:
                src.log({"route": route, "error": str(e),
                         "instance_id": src.hist.instance_id})
                _json_response(self, 409, {"error": str(e)})
            except (KeyError, IndexError) as e:
                _json_response(self, 409, {"error": str(e)})
            except RuntimeError as e:
                src.log({"route": route, "error": str(e)})
                _json_response(self, 500, {"error": str(e)})

    return Handler


def make_version_server(history, log_path, host="127.0.0.1", port=0,
                        max_page_bytes=8192):
    src = VersionSource(history, log_path, max_page_bytes)
    httpd = ThreadingHTTPServer((host, port), make_version_handler(src))
    httpd.version_source = src
    return httpd


def serve_version(history, log_path, host, port, max_page_bytes=8192):
    httpd = make_version_server(history, log_path, host, port, max_page_bytes)
    print(f"版本源 {history.name}（实例 {history.instance_id}，方言 {history.dialect}）"
          f"监听 http://{host}:{httpd.server_address[1]}  日志 {log_path}")
    httpd.serve_forever()
