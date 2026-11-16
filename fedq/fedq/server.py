"""本地 HTTP 模拟数据源。

每个源按自己公开的能力规则**真实执行**远端子计划（scan/filter/project/aggregate/sort/limit），
不假装它们与统一语义等价：
- A：文本先折 A-Z→a-z、去尾部 ASCII 空格再比较；普通比较遇 null 为 unknown（三值）。
- B：文本等值精确；lt 按 UTF-8 字节比较；普通比较遇 null 为 false，NOT 在二值结果上执行。

协议（JSON over HTTP）：
- GET  /capabilities            → 能力声明
- POST /query  {"plan": {...}}  → 建立查询会话，返回第 0 页与稳定令牌 token
- POST /query  {"token": t, "page": k} → 取第 k 页（固定令牌下页次可重试、数据不变）

单条响应有公开大小上限 max_page_bytes；会话在首次请求时快照结果，之后翻页数据不变。
本服务只接受声明过的操作；收到未声明的算子返回 400。
"""

from __future__ import annotations

import json
import threading
import time
import uuid
from functools import cmp_to_key
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from . import caps as C

UNKNOWN = "?"  # 服务端三值标记（仅 A 方言内部使用）


class PlanRejected(Exception):
    pass


# ---------------------------------------------------------------- 方言求值

def a_normalize(s):
    """A 的文本规整：ASCII A-Z 折为 a-z，去掉尾部 ASCII 空格。"""
    s = "".join(chr(ord(c) + 32) if "A" <= c <= "Z" else c for c in s)
    return s.rstrip(" ")


def _cmp_vals(a, b, dialect):
    """返回方言下的比较真值：A → True/False/UNKNOWN；B → True/False。"""
    if a is None or b is None:
        return UNKNOWN if dialect == "A" else False
    return None  # 由调用方继续


class SourceCore:
    """按公开规则执行远端子计划。"""

    def __init__(self, caps, tables):
        self.caps = caps
        self.tables = tables
        self.dialect = caps.source  # 'A' | 'B'

    # ---- 谓词（服务端独立实现，不与执行器共享代码）----
    def _eq(self, a, b):
        r = _cmp_vals(a, b, self.dialect)
        if r is not None:
            return r
        if isinstance(a, str):
            if self.caps.text_eq == C.EQ_FOLD_AZ_RSTRIP:
                return a_normalize(a) == a_normalize(b)
            return a == b
        return a == b

    def _lt(self, a, b):
        r = _cmp_vals(a, b, self.dialect)
        if r is not None:
            return r
        if isinstance(a, str):
            if self.caps.text_order == C.ORD_NORMALIZED:
                a, b = a_normalize(a), a_normalize(b)
            elif self.caps.text_order == C.ORD_UTF8_BYTES:
                a, b = a.encode("utf-8"), b.encode("utf-8")
            return a < b
        return a < b

    def pred_value(self, p, row):
        k = p["k"]
        if k == "field":
            return row[p["name"]]
        if k == "lit":
            v = p["v"]
            if p["t"] == "int" and v is not None:
                return int(v)  # 线上 int 字面量按字符串传输（任意精度安全）
            return v
        if k == "eq":
            return self._eq(self.pred_value(p["a"], row), self.pred_value(p["b"], row))
        if k == "lt":
            return self._lt(self.pred_value(p["a"], row), self.pred_value(p["b"], row))
        if k == "is_null":
            return self.pred_value(p["x"], row) is None
        if k == "and":
            vals = [self.pred_value(x, row) for x in p["args"]]
            if self.dialect == "A":
                if any(v is False for v in vals):
                    return False
                return UNKNOWN if any(v == UNKNOWN for v in vals) else True
            return all(vals)
        if k == "or":
            vals = [self.pred_value(x, row) for x in p["args"]]
            if self.dialect == "A":
                if any(v is True for v in vals):
                    return True
                return UNKNOWN if any(v == UNKNOWN for v in vals) else False
            return any(vals)
        if k == "not":
            v = self.pred_value(p["x"], row)
            if self.dialect == "A":
                return UNKNOWN if v == UNKNOWN else (not v)
            return not v  # B：在二值结果上取反
        raise PlanRejected(f"未声明的谓词算子: {k!r}")

    def _check_pred_ops(self, p):
        k = p["k"]
        if k in ("field", "lit"):
            return
        if k not in self.caps.filter_ops:
            raise PlanRejected(f"源 {self.dialect} 不支持谓词算子 {k!r}")
        for key in ("a", "b", "x"):
            if key in p:
                self._check_pred_ops(p[key])
        for x in p.get("args", []):
            self._check_pred_ops(x)

    # ---- 计划执行 ----
    def execute(self, plan):
        op = plan["op"]
        if op == "scan":
            t = self.tables[plan["table"]]
            cols = list(t["columns"].keys())
            rows = [dict(zip(cols, r)) for r in t["rows"]]
            return cols, rows
        if op == "filter":
            if "filter" not in self.caps.filter_ops and not self.caps.filter_ops:
                raise PlanRejected("源不支持过滤")
            self._check_pred_ops(plan["pred"])
            cols, rows = self.execute(plan["input"])
            return cols, [r for r in rows if self.pred_value(plan["pred"], r) is True]
        if op == "project":
            if not self.caps.project:
                raise PlanRejected("源不支持投影")
            cols, rows = self.execute(plan["input"])
            want = plan["cols"]
            return want, [{c: r[c] for c in want} for r in rows]
        if op == "aggregate":
            return self._aggregate(plan)
        if op == "sort":
            if not self.caps.sort:
                raise PlanRejected("源不支持排序")
            cols, rows = self.execute(plan["input"])
            keys = [(k["field"], k["asc"], k["nulls_first"]) for k in plan["keys"]]
            idx = {c: i for i, c in enumerate(cols)}
            coltypes = self._coltypes(plan["input"], cols)
            rows = sorted(rows, key=cmp_to_key(self._make_cmp(keys, idx, coltypes)))
            return cols, rows
        if op == "limit":
            if not self.caps.limit:
                raise PlanRejected("源不支持 limit")
            cols, rows = self.execute(plan["input"])
            return cols, rows[: plan["n"]]
        raise PlanRejected(f"未声明的计划算子: {op!r}")

    def _coltypes(self, plan, cols):
        # 排序键类型取自扫描表（聚合输出另有约定）
        node = plan
        while node["op"] != "scan":
            node = node["input"]
        tcols = self.tables[node["table"]]["columns"]
        return {c: tcols.get(c, "int") for c in cols}

    def _make_cmp(self, keys, idx, coltypes):
        def cmp(r1, r2):
            for name, asc, nulls_first in keys:
                i = idx[name]
                a, b = r1[name], r2[name]
                if a is None and b is None:
                    continue
                if a is None:
                    c = -1 if nulls_first else 1
                elif b is None:
                    c = 1 if nulls_first else -1
                else:
                    if coltypes.get(name) == "text":
                        if self.caps.text_order == C.ORD_NORMALIZED:
                            a2, b2 = a_normalize(a), a_normalize(b)
                        elif self.caps.text_order == C.ORD_UTF8_BYTES:
                            a2, b2 = a.encode("utf-8"), b.encode("utf-8")
                        else:
                            a2, b2 = a, b
                    else:
                        a2, b2 = a, b
                    c = (a2 > b2) - (a2 < b2)
                if c:
                    return c if asc else -c
            return 0
        return cmp

    def _aggregate(self, plan):
        for a in plan["aggs"]:
            f = a["f"]
            if f in ("count_star", "count") and "count" not in self.caps.aggregates:
                raise PlanRejected("源不支持 count")
            if f == "sum" and "sum_int" not in self.caps.aggregates:
                raise PlanRejected("源不支持 sum_int")
        cols, rows = self.execute(plan["input"])
        group_by = plan["group_by"]
        aggs = plan["aggs"]
        groups = {}
        order = []
        for r in rows:
            key = tuple(r[g] for g in group_by)
            if key not in groups:
                groups[key] = {"n": 0, "acc": {a["out"]: [0, 0, False] for a in aggs}}
                order.append(key)
            g = groups[key]
            g["n"] += 1
            for a in aggs:
                st = g["acc"][a["out"]]
                if a["f"] == "count_star":
                    st[0] += 1
                else:
                    v = r[a["field"]]
                    if v is not None:  # 聚合的 null 规则：跳过 null；全 null 的 sum 为 null
                        st[1] += 1
                        st[0] += v if a["f"] == "sum" else 1
                        st[2] = True
        out_cols = list(group_by) + [a["out"] for a in aggs]
        out_rows = []
        if not group_by and not order:
            order.append(())  # 全局聚合空集也返回一行：count=0，sum=null
            groups[()] = {"n": 0, "acc": {a["out"]: [0, 0, False] for a in aggs}}
        for key in order:
            g = groups[key]
            row = dict(zip(group_by, key))
            for a in aggs:
                st = g["acc"][a["out"]]
                if a["f"] == "sum":
                    row[a["out"]] = st[0] if st[2] else None
                else:
                    row[a["out"]] = st[0]
            out_rows.append(row)
        return out_cols, out_rows


# ---------------------------------------------------------------- HTTP 服务

class _State:
    def __init__(self, caps, tables, log_path):
        self.caps = caps
        self.core = SourceCore(caps, tables)
        self.tables = tables
        self.sessions = {}
        self.lock = threading.Lock()
        self.log_path = log_path

    def log(self, rec):
        rec["ts"] = time.strftime("%Y-%m-%dT%H:%M:%S")
        rec["source"] = self.caps.source
        line = json.dumps(rec, ensure_ascii=False)
        with self.lock:
            with open(self.log_path, "a", encoding="utf-8") as f:
                f.write(line + "\n")


def _json_response(h, code, obj):
    body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
    h.send_response(code)
    h.send_header("Content-Type", "application/json; charset=utf-8")
    h.send_header("Content-Length", str(len(body)))
    h.end_headers()
    h.wfile.write(body)


def make_handler(state):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def do_GET(self):
            if self.path == "/capabilities":
                _json_response(self, 200, state.caps.to_json())
            else:
                _json_response(self, 404, {"error": "unknown route"})

        def do_POST(self):
            if self.path != "/query":
                _json_response(self, 404, {"error": "unknown route"})
                return
            try:
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            except Exception:
                _json_response(self, 400, {"error": "bad json"})
                return
            if "plan" in body and body.get("plan") is not None:
                self._open_session(body["plan"])
            elif "token" in body:
                self._fetch_page(body["token"], body.get("page"))
            else:
                _json_response(self, 400, {"error": "need plan or token"})

        def _open_session(self, plan):
            try:
                cols, rows = state.core.execute(plan)
            except PlanRejected as e:
                state.log({"route": "query", "error": str(e), "plan": plan})
                _json_response(self, 400, {"error": str(e)})
                return
            except KeyError as e:
                _json_response(self, 400, {"error": f"bad plan: {e}"})
                return
            table = _scan_table(plan)
            types = self._types_for(plan, cols)
            pages = self._paginate(cols, rows)
            opts = state.tables.get(table, {}).get("opts", {})
            if opts.get("duplicate_across_pages") and len(pages) >= 2 and pages[0]:
                pages[1] = [pages[0][0]] + pages[1]  # 模拟分页重叠缺陷
            token = uuid.uuid4().hex
            state.sessions[token] = {"pages": pages, "columns": cols, "types": types,
                                     "table": table, "opts": opts}
            state.log({"route": "query", "token": token, "plan": plan,
                       "columns": cols, "rows_total": len(rows)})
            self._send_page(token, 0)

        def _fetch_page(self, token, page):
            sess = state.sessions.get(token)
            if sess is None:
                state.log({"route": "page", "token": token, "error": "invalid token"})
                _json_response(self, 409, {"error": "invalid or expired token"})
                return
            if not isinstance(page, int) or page < 0 or page >= len(sess["pages"]):
                _json_response(self, 409, {"error": f"bad page {page}"})
                return
            self._send_page(token, page)

        def _send_page(self, token, page):
            sess = state.sessions[token]
            fail = sess["opts"].get("fail_on_page")
            if fail is not None and page == fail:
                state.log({"route": "page", "token": token, "page": page,
                           "error": "injected http 500"})
                _json_response(self, 500, {"error": "injected failure"})
                return
            rows = sess["pages"][page]
            payload = {"token": token, "columns": sess["columns"],
                       "types": sess["types"], "rows": rows, "page": page,
                       "next_page": page + 1 if page + 1 < len(sess["pages"]) else None}
            state.log({"route": "page", "token": token, "page": page,
                       "columns": sess["columns"], "rows": len(rows),
                       "bytes": len(json.dumps(payload))})
            _json_response(self, 200, payload)

        def _types_for(self, plan, cols):
            node = plan
            while node["op"] not in ("scan", "aggregate"):
                node = node["input"]
            if node["op"] == "scan":
                tcols = state.tables[node["table"]]["columns"]
                return [tcols[c] for c in cols]
            out = []
            gby = node["group_by"]
            for c in cols:
                if c in gby:
                    node2 = node["input"]
                    while node2["op"] != "scan":
                        node2 = node2["input"]
                    out.append(state.tables[node2["table"]]["columns"][c])
                else:
                    out.append("int")  # count/sum 输出；sum 可为任意精度整数
            return out

        def _paginate(self, cols, rows):
            budget = state.caps.max_page_bytes
            pages, cur, size = [], [], 2
            for r in rows:
                row = [r[c] for c in cols]
                n = len(json.dumps(row, ensure_ascii=False)) + 1
                if cur and size + n > budget:
                    pages.append(cur)
                    cur, size = [], 2
                cur.append(row)
                size += n
            pages.append(cur)
            return pages

    return Handler


def _scan_table(plan):
    node = plan
    while node["op"] != "scan":
        node = node["input"]
    return node["table"]


def make_server(caps, tables, log_path, host="127.0.0.1", port=0):
    state = _State(caps, tables, log_path)
    httpd = ThreadingHTTPServer((host, port), make_handler(state))
    httpd.state = state
    return httpd


def serve(caps, tables, log_path, host, port):
    httpd = make_server(caps, tables, log_path, host, port)
    print(f"源 {caps.source} 监听 http://{host}:{httpd.server_address[1]}  "
          f"日志 {log_path}")
    httpd.serve_forever()
