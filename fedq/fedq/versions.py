"""版本化提交历史模型与依赖闭合选择。

每个逻辑源拥有：
- 持久 instance_id；
- 从 1 连续递增的本地提交号，第 0 版为初始表；
- 一笔提交含同源多表的 upsert/delete 和 requires 映射
  （{"A": 3} 表示包含这笔提交的读取批必须同时包含 A 的第 3 笔提交）。

本模块只做**事实层**的事情：历史校验、按提交顺序重放某一版本的表状态、
在观察盒 [L, H] 内求分量最大的依赖闭合向量 F，并记录每一次“从上界退回”。
它不做查询筛选、不改写任何查询答案。
"""

from __future__ import annotations

from dataclasses import dataclass, field

VALID_TYPES = {"int", "text", "bool"}


class HistoryError(ValueError):
    """历史/元数据不合法（提交号不连续、类型不符、bool 冒充整数等）。"""


class NoClosedVector(ValueError):
    """观察盒 [L, H] 内不存在依赖闭合向量。"""


# ---------------------------------------------------------------- 数据结构

@dataclass
class Change:
    table: str
    upserts: list = field(default_factory=list)   # list[list]，行值按表列顺序
    deletes: list = field(default_factory=list)   # list[int]，主键 id


@dataclass
class Commit:
    seq: int
    changes: dict            # table -> Change
    requires: dict           # source -> 非负提交位置
    note: str = ""


@dataclass
class SourceHistory:
    name: str
    instance_id: str
    dialect: str             # 'A' | 'B'（复用已有两源方言）
    schemas: dict            # table -> {"columns": {col: type}, "key": [..]}
    initial: dict            # table -> list[行]
    commits: list            # list[Commit]，seq 从 1 连续
    head: int = None         # 当前开放前缀
    earliest: int = 0        # 当前仍可读取的最早状态

    def __post_init__(self):
        if self.head is None:
            self.head = len(self.commits)
        self.validate()
        self._states = {}          # 延迟重放并缓存的版本状态
        self._protections = {}     # at -> 持有计数（读取保护）

    # ---- 校验 ----
    def validate(self):
        if not isinstance(self.name, str) or not self.name:
            raise HistoryError("逻辑源名必须是非空字符串")
        if not isinstance(self.instance_id, str) or not self.instance_id:
            raise HistoryError(f"源 {self.name}: instance_id 必须是非空字符串")
        if self.dialect not in ("A", "B"):
            raise HistoryError(f"源 {self.name}: 方言必须是 A 或 B，得到 {self.dialect!r}")
        if not isinstance(self.schemas, dict) or not self.schemas:
            raise HistoryError(f"源 {self.name}: 至少声明一张表")
        for t, sch in self.schemas.items():
            cols = sch.get("columns")
            if not isinstance(cols, dict) or not cols:
                raise HistoryError(f"源 {self.name} 表 {t}: columns 必须是非空映射")
            for c, ty in cols.items():
                if ty not in VALID_TYPES:
                    raise HistoryError(f"源 {self.name} 表 {t} 列 {c}: 非法类型 {ty!r}")
            key = sch.get("key", ["id"])
            if not key or any(k not in cols for k in key):
                raise HistoryError(f"源 {self.name} 表 {t}: 唯一键 {key} 不在列中")
        if set(self.initial) != set(self.schemas):
            raise HistoryError(f"源 {self.name}: 初始表集合必须与声明一致")
        for t, rows in self.initial.items():
            for r in rows:
                _check_row(self.name, t, self.schemas[t]["columns"], r)

        seqs = []
        for c in self.commits:
            if not isinstance(c, Commit):
                raise HistoryError(f"源 {self.name}: 提交必须是 Commit")
            seqs.append(c.seq)
            if not isinstance(c.seq, int) or isinstance(c.seq, bool):
                raise HistoryError(f"源 {self.name}: 提交号必须是整数，bool 不允许")
            if not isinstance(c.requires, dict):
                raise HistoryError(f"源 {self.name} 提交 {c.seq}: requires 必须是映射")
            for src, d in c.requires.items():
                if not isinstance(src, str) or not src:
                    raise HistoryError(f"源 {self.name} 提交 {c.seq}: 依赖源名非法")
                if not isinstance(d, int) or isinstance(d, bool):
                    raise HistoryError(
                        f"源 {self.name} 提交 {c.seq} requires {src!r}: "
                        f"位置必须是非负整数，bool 不能冒充位置整数（得到 {d!r}）")
                if d < 0:
                    raise HistoryError(
                        f"源 {self.name} 提交 {c.seq} requires {src!r}: 位置不能为负")
            if not isinstance(c.changes, dict) or not c.changes:
                raise HistoryError(f"源 {self.name} 提交 {c.seq}: 至少改动一张表")
            for t, ch in c.changes.items():
                if t not in self.schemas:
                    raise HistoryError(f"源 {self.name} 提交 {c.seq}: 未知表 {t}")
                cols = self.schemas[t]["columns"]
                keycols = self.schemas[t].get("key", ["id"])
                for r in ch.upserts:
                    _check_row(self.name, t, cols, r)
                for rid in ch.deletes:
                    # 单列整数主键可直接写标量；其余（text 键、复合键）用列表
                    if len(keycols) == 1 and cols[keycols[0]] == "int":
                        if not isinstance(rid, int) or isinstance(rid, bool) \
                                or rid < 0:
                            raise HistoryError(
                                f"源 {self.name} 提交 {c.seq} 表 {t}: "
                                "删除 id 必须是非负整数")
                    else:
                        _check_key(self.name, t, self.schemas[t], rid)
        if seqs != list(range(1, len(seqs) + 1)):
            raise HistoryError(f"源 {self.name}: 提交号必须从 1 连续递增，得到 {seqs}")
        if not isinstance(self.head, int) or isinstance(self.head, bool) \
                or not (0 <= self.head <= len(self.commits)):
            raise HistoryError(f"源 {self.name}: head 越界 {self.head!r}")
        if not isinstance(self.earliest, int) or isinstance(self.earliest, bool) \
                or not (0 <= self.earliest <= self.head):
            raise HistoryError(f"源 {self.name}: earliest 必须满足 0 <= earliest <= head")

    # ---- 版本重放 ----
    def state_at(self, at):
        """重放提交 1..at 后的真实表状态（同源多表改动按提交顺序整体生效）。"""
        if not isinstance(at, int) or isinstance(at, bool):
            raise HistoryError(f"源 {self.name}: 版本位置必须是整数")
        if not (0 <= at <= len(self.commits)):
            raise HistoryError(f"源 {self.name}: 版本 {at} 超出历史 0..{len(self.commits)}")
        if at in self._states:
            return self._states[at]
        # 从最近的已缓存状态向前重放
        start = max((k for k in self._states if k <= at), default=0)
        if start == 0:
            tables = {t: {_key_of(self.schemas[t], r): list(r)
                          for r in rows} for t, rows in self.initial.items()}
        else:
            tables = {t: dict(d) for t, d in self._states[start].items()}
        for commit in self.commits[start:at]:
            for t, ch in commit.changes.items():
                keycols = self.schemas[t].get("key", ["id"])
                tbl = tables[t]
                for row in ch.upserts:                 # 同行 id 更新：后者覆盖前者
                    tbl[_key_of(self.schemas[t], row)] = list(row)
                for rid in ch.deletes:
                    if isinstance(rid, list):
                        tbl.pop(tuple(rid), None)
                    else:
                        tbl.pop(tuple([rid] if len(keycols) == 1 else rid), None)
        self._states[at] = tables
        return tables

    def readable(self, at):
        """该版本当前是否可直接读取（未被淘汰，或被读取保护钉住）。"""
        if at < 0 or at > self.head:
            return False
        return at >= self.earliest or self._protections.get(at, 0) > 0

    # ---- 读取保护（源端责任）----
    def protect(self, at):
        if not self.readable(at):
            raise HistoryError(f"源 {self.name}: 版本 {at} 已淘汰且无保护，无法建立快照")
        self._protections[at] = self._protections.get(at, 0) + 1
        self.state_at(at)  # 预热：确保状态实体被保留

    def release(self, at):
        n = self._protections.get(at, 0)
        if n <= 0:
            return
        n -= 1
        if n:
            self._protections[at] = n
        else:
            self._protections.pop(at, None)
            self._evict()

    def _evict(self):
        """淘汰 earliest 之前、且无保护的状态实体（依赖元数据始终保留）。"""
        for k in list(self._states):
            if k < self.earliest and self._protections.get(k, 0) == 0:
                self._states.pop(k, None)

    def advance_earliest(self, earliest):
        if not (0 <= earliest <= self.head):
            raise HistoryError(f"源 {self.name}: earliest 越界 {earliest}")
        self.earliest = earliest
        self._evict()

    # ---- 开放元数据（只暴露 1..head；依赖即便数据淘汰也保留）----
    def metadata(self):
        return {
            "source": self.name,
            "instance_id": self.instance_id,
            "dialect": self.dialect,
            "head": self.head,
            "earliest": self.earliest,
            "tables": {t: dict(sch) for t, sch in self.schemas.items()},
            "commits": [
                {"seq": c.seq, "requires": dict(c.requires), "note": c.note,
                 "changes": {t: {"upsert": [list(r) for r in ch.upserts],
                                 "delete": list(ch.deletes)}
                             for t, ch in c.changes.items()}}
                for c in self.commits[: self.head]
            ],
        }


def _key_of(schema, row):
    cols = list(schema["columns"].keys())
    keycols = schema.get("key", ["id"])
    if len(keycols) == 1:
        return (row[cols.index(keycols[0])],)
    return tuple(row[cols.index(k)] for k in keycols)


def _check_key(source, table, schema, key):
    """校验删除键：单列主键可给标量（int/text/bool），复合键必须给等长列表。"""
    keycols = schema.get("key", ["id"])
    cols = schema["columns"]
    if len(keycols) == 1 and not isinstance(key, list):
        pairs = [(keycols[0], key)]
    else:
        if not isinstance(key, list) or len(key) != len(keycols):
            raise HistoryError(
                f"源 {source} 表 {table}: 复合删除键必须是含 {len(keycols)} "
                f"个主键值的列表，得到 {key!r}")
        pairs = list(zip(keycols, key))
    for kcol, v in pairs:
        ty = cols[kcol]
        if v is None:
            raise HistoryError(f"源 {source} 表 {table}: 删除键 {kcol} 不能为 null")
        if ty == "int":
            if not isinstance(v, int) or isinstance(v, bool):
                raise HistoryError(
                    f"源 {source} 表 {table}: 整数主键 {kcol} 收到 {type(v).__name__}")
        elif ty == "text" and not isinstance(v, str):
            raise HistoryError(
                f"源 {source} 表 {table}: text 主键 {kcol} 收到 {type(v).__name__}")
        elif ty == "bool" and not isinstance(v, bool):
            raise HistoryError(
                f"源 {source} 表 {table}: bool 主键 {kcol} 收到 {type(v).__name__}")


def _check_row(source, table, columns, row):
    if not isinstance(row, list) or len(row) != len(columns):
        raise HistoryError(f"源 {source} 表 {table}: 行列数不符 {row!r}")
    for (col, ty), v in zip(columns.items(), row):
        if v is None:
            continue
        if ty == "int":
            if not isinstance(v, int) or isinstance(v, bool):
                raise HistoryError(
                    f"源 {source} 表 {table} 列 {col}: 声明 int，"
                    f"收到 {type(v).__name__} {v!r}（bool 不算整数）")
        elif ty == "text":
            if not isinstance(v, str):
                raise HistoryError(
                    f"源 {source} 表 {table} 列 {col}: 声明 text，收到 {type(v).__name__}")
        elif ty == "bool":
            if not isinstance(v, bool):
                raise HistoryError(
                    f"源 {source} 表 {table} 列 {col}: 声明 bool，收到 {type(v).__name__}")


# ---------------------------------------------------------------- 元数据校验

def validate_metadata(meta, known_sources):
    """协调者收到 /metadata 后再做一遍独立校验（防御深度）。

    known_sources: 配置中的全部逻辑源名；requires 只允许指向它们。
    """
    name = meta.get("source")
    if name not in known_sources:
        raise HistoryError(f"元数据源 {name!r} 不在配置 {sorted(known_sources)} 中")
    if not isinstance(meta.get("instance_id"), str) or not meta["instance_id"]:
        raise HistoryError(f"源 {name}: instance_id 缺失或非字符串")
    if meta.get("dialect") not in ("A", "B"):
        raise HistoryError(f"源 {name}: dialect 非法")
    head, earliest = meta.get("head"), meta.get("earliest")
    if not isinstance(head, int) or isinstance(head, bool) or head < 0:
        raise HistoryError(f"源 {name}: head 必须是非负整数（bool 不允许）")
    if not isinstance(earliest, int) or isinstance(earliest, bool) \
            or not (0 <= earliest <= head):
        raise HistoryError(f"源 {name}: earliest 必须满足 0<=earliest<=head")
    tables = meta.get("tables", {})
    for t, sch in tables.items():
        for c, ty in sch.get("columns", {}).items():
            if ty not in VALID_TYPES:
                raise HistoryError(f"源 {name} 表 {t} 列 {c}: 非法字段类型 {ty!r}")
    seqs = []
    for c in meta.get("commits", []):
        seq = c.get("seq")
        if not isinstance(seq, int) or isinstance(seq, bool):
            raise HistoryError(f"源 {name}: 提交号必须是整数（bool 不允许）")
        seqs.append(seq)
        for src, d in c.get("requires", {}).items():
            if src not in known_sources:
                raise HistoryError(
                    f"源 {name} 提交 {seq}: requires 指向未知源 {src!r}")
            if not isinstance(d, int) or isinstance(d, bool):
                raise HistoryError(
                    f"源 {name} 提交 {seq} requires {src!r}: "
                    f"位置必须是非负整数，bool 不能冒充（得到 {d!r}）")
            if d < 0:
                raise HistoryError(f"源 {name} 提交 {seq} requires {src!r}: 位置为负")
        for t, ch in c.get("changes", {}).items():
            if t not in tables:
                raise HistoryError(f"源 {name} 提交 {seq}: 改动未知表 {t}")
            sch = tables[t]
            cols = sch["columns"]
            keycols = sch.get("key", ["id"])
            for r in ch.get("upsert", []):
                _check_row(name, t, cols, r)
            for rid in ch.get("delete", []):
                if len(keycols) == 1 and cols[keycols[0]] == "int":
                    if not isinstance(rid, int) or isinstance(rid, bool) \
                            or rid < 0:
                        raise HistoryError(
                            f"源 {name} 提交 {seq} 表 {t}: 删除 id 必须是非负整数")
                else:
                    _check_key(name, t, sch, rid)
    if seqs != list(range(1, head + 1)):
        raise HistoryError(f"源 {name}: 元数据提交号必须为 1..head 的连续序列，得到 {seqs}")


def history_from_config(cfg):
    """从 JSON 配置构建 SourceHistory（供模拟源进程使用）。"""
    commits = []
    for c in cfg.get("commits", []):
        changes = {}
        for t, ch in c.get("changes", {}).items():
            changes[t] = Change(table=t,
                                upserts=[list(r) for r in ch.get("upsert", [])],
                                deletes=list(ch.get("delete", [])))
        commits.append(Commit(seq=int(c["seq"]), changes=changes,
                              requires=dict(c.get("requires", {})),
                              note=c.get("note", "")))
    return SourceHistory(
        name=cfg["name"], instance_id=cfg["instance_id"],
        dialect=cfg["dialect"], schemas=cfg["tables"],
        initial={t: [list(r) for r in rows] for t, rows in cfg["initial"].items()},
        commits=commits, head=cfg.get("head"), earliest=cfg.get("earliest", 0))


def history_from_metadata(meta):
    """协调者侧：把 /metadata 还原成只含开放前缀的 SourceHistory（用于选择与解释）。"""
    commits = []
    for c in meta["commits"]:
        changes = {}
        for t, ch in c["changes"].items():
            changes[t] = Change(table=t, upserts=ch.get("upsert", []),
                                deletes=ch.get("delete", []))
        commits.append(Commit(seq=c["seq"], changes=changes,
                              requires=c.get("requires", {}), note=c.get("note", "")))
    return SourceHistory(
        name=meta["source"], instance_id=meta["instance_id"],
        dialect=meta["dialect"], schemas=meta["tables"],
        initial={t: [] for t in meta["tables"]},   # 协调者不据此重放数据
        commits=commits, head=meta["head"], earliest=meta["earliest"])


# ---------------------------------------------------------------- 依赖闭合选择

def select_vector(histories, H, L):
    """求观察盒 [L, H] 内分量最大的依赖闭合向量 F。

    算法（最小不动点）：F 从 H 出发，反复扫描“已纳入提交”的每条 requires[t]=d，
    若 d > F[t] 则把该源退到触发提交的前一个位置 k-1，直到不再变化。

    正确性要点：
    - 闭合且 ≤H 的向量集合若有最大元，它就是该不动点——任何合法 G 在每次退回后
      仍有 G ≤ F（归纳：若 G[s] ≥ k，合法性要求 d ≤ G[t] ≤ F[t]，矛盾），
      故 G 不可能超过不动点；不动点自身在稳定时满足全部已纳入依赖，即为合法向量。
    - 合法性不向下封闭（缩小分量可能让别的已纳入提交失去前置），所以不能随便
      返回某个“安全但过旧”的向量，必须取这个唯一的分量最大者。
    - 环不构成拒绝理由：互相要求的提交只要彼此都在向量内，依赖即同时满足。
    - 不同源的提交号只是各自本地序号，从不被当作全局时间。

    返回 (F, trace)；trace 记录每次退回，便于解释数据为何尚不可用。
    若不动点跌破某源 L，则抛 NoClosedVector。
    """
    sources = sorted(histories)
    F = dict(H)
    trace = []
    changed = True
    while changed:
        changed = False
        for s in sources:
            hist = histories[s]
            for k in range(1, F[s] + 1):
                commit = hist.commits[k - 1]
                for t, d in commit.requires.items():
                    if F[t] < d:
                        new = k - 1
                        trace.append({
                            "source": s, "commit": k, "requires": {t: d},
                            "from": F[s], "to": new,
                            "reason": f"{s} 的第 {k} 笔要求 {t}:{d}，"
                                      f"但 {t} 至多位于 {F[t]} → {s} 退到 {new}"})
                        F[s] = new
                        changed = True
                        break
                if changed:
                    break
    for s in sources:
        if F[s] < L[s]:
            raise NoClosedVector(
                f"无合法读取向量：{s} 依赖闭合后最多到 {F[s]}，"
                f"但 earliest 下界为 {L[s]}（旧状态已淘汰，前置数据无法同批读取）")
    return F, trace


def retreat_summary(H, F, trace):
    """按源汇总：哪些依赖让该源从观察上界 H 退回到 F。"""
    out = {}
    for s in sorted(H):
        if H[s] == F[s]:
            continue
        causes = [x for x in trace if x["source"] == s]
        out[s] = {"observed_head": H[s], "selected": F[s],
                  "causes": [{"commit": c["commit"], "requires": c["requires"],
                              "reason": c["reason"]} for c in causes]}
    return out
