"""连接器能力声明。

每个数据源公开：
- 表与字段类型、唯一键；
- 文本等值规则 text_eq、文本排序规则 text_order；
- null 比较规则 null_compare；
- 支持的过滤算子、投影、分组聚合、排序/limit 能力；
- 分页参数（公开的单条响应大小上限）与每查询稳定读取令牌。

执行器只依据这些声明做计划；模拟端按声明的规则真实执行。
"""

from __future__ import annotations

from dataclasses import dataclass, field

# 文本等值规则
EQ_UNIFIED = "unified"                 # 码点精确、区分大小写、不去尾空格
EQ_FOLD_AZ_RSTRIP = "fold_az_rstrip"   # 先把 ASCII A-Z 折为 a-z、去掉尾部 ASCII 空格再比较

# 文本排序规则
ORD_UNIFIED = "unified_codepoint"      # 码点序
ORD_UTF8_BYTES = "utf8_bytes"          # UTF-8 字节序（合法 Unicode 下与码点序相容）
ORD_NORMALIZED = "normalized"          # 规整（折叠+去尾空格）后的码点序

# null 比较规则
NULL_UNKNOWN = "unknown"               # 三值逻辑：普通比较遇 null 为 unknown
NULL_FALSE = "false"                   # 二值逻辑：普通比较遇 null 为 false，NOT 在二值结果上执行


@dataclass
class TableSchema:
    columns: dict          # name -> 'int'|'text'|'bool'
    key: list = field(default_factory=lambda: ["id"])

    def to_json(self):
        return {"columns": self.columns, "key": self.key}

    @classmethod
    def from_json(cls, d):
        return cls(dict(d["columns"]), list(d.get("key", ["id"])))


@dataclass
class Caps:
    source: str
    tables: dict                    # table -> TableSchema
    text_eq: str
    text_order: str
    null_compare: str
    filter_ops: frozenset           # {'eq','lt','is_null','and','or','not'} 的子集
    project: bool
    aggregates: frozenset           # {'count','sum_int'} 的子集；空集表示无聚合能力
    sort: bool
    limit: bool
    max_page_bytes: int
    token_per_query: bool = True

    def table(self, name):
        if name not in self.tables:
            raise KeyError(f"源 {self.source} 无表 {name!r}")
        return self.tables[name]

    def to_json(self):
        return {
            "source": self.source,
            "tables": {t: s.to_json() for t, s in self.tables.items()},
            "text_eq": self.text_eq,
            "text_order": self.text_order,
            "null_compare": self.null_compare,
            "filter_ops": sorted(self.filter_ops),
            "project": self.project,
            "aggregates": sorted(self.aggregates),
            "sort": self.sort,
            "limit": self.limit,
            "max_page_bytes": self.max_page_bytes,
            "token_per_query": self.token_per_query,
        }

    @classmethod
    def from_json(cls, d):
        return cls(
            source=d["source"],
            tables={t: TableSchema.from_json(s) for t, s in d["tables"].items()},
            text_eq=d["text_eq"],
            text_order=d["text_order"],
            null_compare=d["null_compare"],
            filter_ops=frozenset(d["filter_ops"]),
            project=bool(d["project"]),
            aggregates=frozenset(d["aggregates"]),
            sort=bool(d["sort"]),
            limit=bool(d["limit"]),
            max_page_bytes=int(d["max_page_bytes"]),
            token_per_query=bool(d.get("token_per_query", True)),
        )
