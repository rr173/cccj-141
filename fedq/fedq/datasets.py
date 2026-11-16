"""两个模拟数据源的公开能力声明与数据。

A：文本先把 ASCII A-Z 折为 a-z、去掉尾部 ASCII 空格再比较；普通比较遇 null 为 unknown。
B：文本等值与统一语义相同；lt 按 UTF-8 字节（合法 Unicode 下与码点序相容）；
   普通比较遇 null 为 false，NOT 在二值结果上执行。

flaky / badtypes / dupids 是用于检验失败边界的表。
"""

from .caps import (Caps, TableSchema, EQ_UNIFIED, EQ_FOLD_AZ_RSTRIP,
                   ORD_UNIFIED, ORD_UTF8_BYTES, ORD_NORMALIZED,
                   NULL_UNKNOWN, NULL_FALSE)

BIG = 2 ** 62          # int64 内的较大加数
BIG_SUM = 2 ** 63      # 两行 BIG 之和：超过 int64，验证 sum 精确值

A_ITEMS = [
    # id, name, amount, flag
    [1, "X", 10, True],
    [2, "x", 20, False],
    [3, "X ", 30, True],     # X 后跟一个 ASCII 空格
    [4, "Y", 20, None],
    [5, None, 50, True],
    [6, "Z", 20, False],
    [7, "Q", 12, True],
]

B_ENTRIES = [
    # id, name, amount, qty
    [1, "X", 3, 100],
    [2, None, 0, 5],         # name=null：NOT(name='X') 在统一语义下不得选中它
    [3, "Y", 9, 7],
    [4, "x", 12, None],      # qty 全 null 的分组来源
    [5, "W", 1, 9],
    [6, "V", 20, 3],
    [7, "U", 20, 4],
    [8, "T", None, 6],       # amount=null：等值连接永不匹配
    [9, "BIG", 10, BIG],     # 两行 BIG 同组：sum = 2**63 超出 int64
    [10, "BIG2", 10, BIG],
]


def source_a(max_page_bytes=32768):
    caps = Caps(
        source="A",
        tables={
            "items": TableSchema({"id": "int", "name": "text",
                                  "amount": "int", "flag": "bool"}),
            "badtypes": TableSchema({"id": "int", "name": "text"}),
        },
        text_eq=EQ_FOLD_AZ_RSTRIP,
        text_order=ORD_NORMALIZED,
        null_compare=NULL_UNKNOWN,
        filter_ops=frozenset({"eq", "lt", "is_null", "and", "or", "not"}),
        project=True,
        aggregates=frozenset({"count", "sum_int"}),
        sort=True,
        limit=True,
        max_page_bytes=max_page_bytes,
    )
    tables = {
        "items": {"columns": caps.tables["items"].columns, "rows": A_ITEMS},
        # 声明为 text 的列里混进了 int：客户端必须拒绝
        "badtypes": {"columns": caps.tables["badtypes"].columns,
                     "rows": [[1, "ok"], [2, 123]]},
    }
    return caps, tables


def source_b(max_page_bytes=32768):
    caps = Caps(
        source="B",
        tables={
            "entries": TableSchema({"id": "int", "name": "text",
                                    "amount": "int", "qty": "int"}),
            "flaky": TableSchema({"id": "int", "v": "int"}),
            "dupids": TableSchema({"id": "int", "v": "text"}),
        },
        text_eq=EQ_UNIFIED,
        text_order=ORD_UTF8_BYTES,
        null_compare=NULL_FALSE,
        filter_ops=frozenset({"eq", "lt", "is_null", "and", "or", "not"}),
        project=True,
        aggregates=frozenset({"count", "sum_int"}),
        sort=True,
        limit=True,
        max_page_bytes=max_page_bytes,
    )
    tables = {
        "entries": {"columns": caps.tables["entries"].columns, "rows": B_ENTRIES},
        # 第 2 页（页码从 0 起）开始注入 HTTP 错误
        "flaky": {"columns": caps.tables["flaky"].columns,
                  "rows": [[i, i * 10] for i in range(1, 41)],
                  "opts": {"fail_on_page": 2}},
        # 跨页重复 id：客户端必须检测并失败
        "dupids": {"columns": caps.tables["dupids"].columns,
                   "rows": [[1, "a"], [2, "b"], [3, "c"]],
                   "opts": {"duplicate_across_pages": True}},
    }
    return caps, tables
