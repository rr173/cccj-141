"""连接器客户端：分页读取、稳定令牌、类型校验、重复 id 检测、统计与明确失败。

失败边界（都明确抛出，不自动恢复、不产出成功标记）：
- TokenError      令牌失效/页码非法（HTTP 409）
- PlanRejected    计划超出连接器声明能力（HTTP 400）
- TypeViolation   返回值的类型与公开字段类型不符
- DuplicateId     分页流中出现重复行 id
- SourceError     中途 HTTP/连接错误（重试同页数次后仍失败）
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request

from .caps import Caps


class SourceError(Exception):
    pass


class TokenError(SourceError):
    pass


class PlanRejected(SourceError):
    pass


class TypeViolation(SourceError):
    pass


class DuplicateId(SourceError):
    pass


class Stats:
    def __init__(self):
        self.requests = 0
        self.rows = 0
        self.bytes = 0
        self.fields = set()

    def to_json(self):
        return {"requests": self.requests, "rows": self.rows,
                "bytes": self.bytes, "fields": sorted(self.fields)}


def _check_type(col, typ, v):
    ok = v is None
    if not ok:
        if typ == "int":
            ok = isinstance(v, int) and not isinstance(v, bool)
        elif typ == "text":
            ok = isinstance(v, str)
        elif typ == "bool":
            ok = isinstance(v, bool)
    if not ok:
        raise TypeViolation(
            f"列 {col!r} 声明类型 {typ}，收到 {type(v).__name__}: {v!r}")


class Client:
    def __init__(self, name, base_url, timeout=10, max_retries=3):
        self.name = name
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self._caps = None
        self.stats = Stats()

    def capabilities(self):
        if self._caps is None:
            body = self._request("GET", "/capabilities", None, retry=False)
            self._caps = Caps.from_json(body)
        return self._caps

    def _request(self, method, path, payload, retry=True):
        url = self.base_url + path
        data = None if payload is None else json.dumps(payload).encode("utf-8")
        attempts = self.max_retries if retry else 1
        last = None
        for i in range(attempts):
            self.stats.requests += 1
            req = urllib.request.Request(url, data=data, method=method,
                                         headers={"Content-Type": "application/json"})
            try:
                with urllib.request.urlopen(req, timeout=self.timeout) as r:
                    raw = r.read()
                    self.stats.bytes += len(raw)
                    return json.loads(raw)
            except urllib.error.HTTPError as e:
                raw = e.read()
                self.stats.bytes += len(raw)
                try:
                    msg = json.loads(raw).get("error", str(e))
                except Exception:
                    msg = str(e)
                if e.code == 409:
                    raise TokenError(f"源 {self.name}: {msg}")
                if e.code == 400:
                    raise PlanRejected(f"源 {self.name}: {msg}")
                last = SourceError(f"源 {self.name}: HTTP {e.code}: {msg}")
            except (urllib.error.URLError, ConnectionError, TimeoutError) as e:
                last = SourceError(f"源 {self.name}: 连接错误: {e}")
            if i + 1 < attempts:
                time.sleep(0.05 * (i + 1))  # 固定令牌下同页可安全重试
        raise last

    def stream(self, plan):
        """执行远端子计划，逐行产出 dict。中途失败抛异常；已产出的行不构成完整结果。"""
        first = self._request("POST", "/query", {"plan": plan})
        token = first["token"]
        columns = first["columns"]
        types = first["types"]
        self.stats.fields.update(columns)
        id_idx = columns.index("id") if "id" in columns else None
        seen_ids = set()
        page = first
        while True:
            if page["columns"] != columns:
                raise SourceError(f"源 {self.name}: 分页之间列发生变化")
            for raw in page["rows"]:
                if len(raw) != len(columns):
                    raise TypeViolation(f"源 {self.name}: 行列数不符: {raw!r}")
                for c, t, v in zip(columns, types, raw):
                    _check_type(c, t, v)
                if id_idx is not None:
                    rid = raw[id_idx]
                    if rid in seen_ids:
                        raise DuplicateId(f"源 {self.name}: 分页流中重复 id={rid}")
                    seen_ids.add(rid)
                self.stats.rows += 1
                yield dict(zip(columns, raw))
            nxt = page["next_page"]
            if nxt is None:
                return
            page = self._request("POST", "/query", {"token": token, "page": nxt})
