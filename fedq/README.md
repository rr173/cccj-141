# fedq — 联邦查询执行器

把两个供应商 HTTP 数据源（A、B）临时联查。两个源的文本/空值语义不同，
**用户结果统一遵循本项目的统一语义，远端差异不作为数据真相**。
执行器接收有类型的查询树，按每个连接器声明的能力决定远端可以提前做什么，
剩余语义在本地（SQLite 落盘 + Python）完成。

仅依赖 Python 3.11+ 标准库。

## 快速复做

```sh
sh scripts/demo.sh            # 端到端演示：内嵌起两个源，跑全部演示查询+失败边界
sh scripts/manual.sh          # 手动流程：分进程起源，plan/run 一个查询，看 HTTP 日志
python3 -m unittest discover -s tests -v   # 31 个测试
```

手动流程：

```sh
python3 -m fedq serve --source A --port 8011 --log A.http.jsonl &
python3 -m fedq serve --source B --port 8012 --log B.http.jsonl &
python3 -m fedq queries --out-dir queries/          # 导出演示查询 JSON
python3 -m fedq plan --query queries/q1_A文本等值候选超集.json
python3 -m fedq run  --query queries/q1_A文本等值候选超集.json --out r1.jsonl --print
```

## 统一语义（`fedq/semantics.py`）

- 类型：`int64`（精确）、`text`、`bool`、`null`。**bool 不混作 int**（静态类型检查拒绝混比）。
- 文本比较：**Unicode 码点顺序，区分大小写，不去尾空格**。
- 三值逻辑：`true/false/unknown`；过滤**只有 true 通过**；`eq/lt` 遇 null 为 `unknown`。
- 聚合：`sum` 任意精度整数（可超 int64），全 null 为 null；`count(*)` 空集为 0；
  `count(字段)` 跳过 null；left 连接补 null 的行按所选 `count(*)`/`count(字段)` 语义计数。
- `order` 必须由用户给出唯一决胜字段（校验唯一集合），null 顺序显式 `nulls_first`；
  `limit` 只能作用于全序 sort 之上。

## 查询树（第一版不接受任意 SQL 文本、任意函数、相关子查询）

JSON 节点：`source / filter / project / join(inner|left 等值) / aggregate(分组 count、整数 sum)
/ distinct / sort / limit`。表达式：`eq / lt / is_null / and / or / not`，
操作数为字段或带类型字面量（int 字面量按字符串传输，任意精度安全）。
校验：类型解析、连接键类型一致、sort 全序、limit 下必须是 sort、不支持 HAVING/嵌套聚合。

## 连接器能力与两源方言（`fedq/caps.py`、`fedq/datasets.py`）

每个连接器公开：表与字段类型、唯一键、`text_eq`、`text_order`、`null_compare`、
过滤算子集、投影、聚合（`count`/`sum_int`）、排序/limit、
`max_page_bytes`（公开的单条响应大小上限）、每查询稳定读取令牌。

| 规则 | A | B |
|---|---|---|
| 文本等值 | 先折 ASCII A-Z→a-z、去尾部 ASCII 空格再比较 | 与统一语义相同 |
| 文本排序 | 规整后文本序（**非**统一序） | UTF-8 字节序（合法 Unicode 下与码点序相容 → 统一序） |
| null 普通比较 | unknown（三值） | false（二值），NOT 在二值结果上执行 |

模拟端按这些公开规则**真实执行**（`fedq/server.py` 独立实现，不与执行器共享求值代码）。

## HTTP 协议

- `GET /capabilities` → 能力声明。
- `POST /query {"plan": {...}}` → 建立查询会话（快照结果），返回第 0 页与稳定 `token`。
- `POST /query {"token": t, "page": k}` → 取第 k 页；**固定令牌下页次可重试、数据不变**，
  无需另做游标恢复账本。令牌失效/页码非法 → `409`；计划超出声明能力 → `400`。
- 远端子计划只含声明过的操作（scan/filter/project/aggregate/sort/limit），
  **不允许发送任意 Python 谓词**；谓词 JSON 只有 `eq/lt/is_null/and/or/not`。
- 单条响应 ≤ `max_page_bytes`；行值按声明类型传输（int 为 JSON 数字，实现需支持任意精度）。
- 服务端写 HTTP 日志（JSONL）：实际收到的计划、返回行数、传输字段；
  执行器报告各源实际请求数/读取行数/字节/字段。

## 计划规则（`fedq/planner.py`，每条下推都带能力依据与语义前提）

远端谓词两种角色：

- **exact（等价过滤）**：远端通过集 == 统一通过集。整数/bool 的 `eq/lt` 在 A、B 皆等价
  （null 在 unknown/false 下同样不通过）；B 的文本 `eq` 等价；B 的文本 `lt` 按 UTF-8 字节，
  合法 Unicode 下与码点序相容 → 等价（不能仅因编码不同就断言不安全）。
- **superset（候选超集过滤）**：远端通过集 ⊇ 统一通过集，**必须留下残余条件**本地精查。
  A 的文本 `eq`（规整后比较，统一为真的行远端必为真）。

组合规则（不能随便分发只在单原子上安全的近似）：

- `AND`：按合取项分别判定；超集项与不可下推项留作残余。
- `OR`：任一分支不可下推则整体本地；分支超集之并仍是超集 → 整体候选，残余为整个 OR。
- `NOT`：对超集近似取反是**子集**（可能漏行）→ 禁止；B 把 null 折叠为 false 再取反，
  null 行远端为真、统一为 unknown → 远端是超集，只能候选下推并留残余。
- A 的文本 `lt` 与码点序既非超集也非子集 → 不下推。

聚合/limit 安全：

- 有残余（或非等价过滤）时，聚合与 limit **一律不下推**——在残余过滤前执行会改变答案；
  绝不在远端先截前 N 行再本地筛选凑 N。
- sort 不增删行、本地残余过滤保序，故排序键为统一序时 sort 可精确下推（即便有残余）。
- 源内聚合（过滤等价、无连接、连接器声明了 count/sum_int）真实下推，远端只回聚合行。
- 投影列裁剪始终下推（外加行 id：分页去重与行身份需要）。
- left 连接右侧的过滤：连接前（ON 语义，输入过滤）与连接后（WHERE）是不同语义，
  规划器按查询树原样执行，绝不跨连接移动谓词。
- 连接器能力不足（如无聚合能力）时，相应算子留本地，受支持查询仍正确执行。

## 本地执行（`fedq/executor.py`）

- 远端行流入 SQLite 临时库（落盘），连接/distinct/排序/limit 在 SQLite 完成，
  **不把全部联查结果常驻 Python**；SQLite 的三值逻辑与 BINARY 文本序与统一语义一致。
- 分组聚合在 Python 流式完成（sum 任意精度），结果回注 SQLite；
  sum 列以十进制文本存储 + `NUMTEXT` 排序规则，大整数精确且排序正确。
- 输出 JSONL：首行 `{"status":"incomplete"}`，成功结束才追加 `{"status":"ok",...}`；
  中途失败写 `{"status":"error",...}`。**部分流永远没有成功结束标记**，
  用户可以保存它，但它不能被标成完整查询（`read_result()["complete"]` 为 False）。

## 失败边界（明确失败，不自动恢复）

- 令牌失效/页码非法（409）→ `TokenError`
- 计划超能力（400）→ `PlanRejected`
- 坏字段类型（返回值与公开类型不符）→ `TypeViolation`
- 分页重复行 id → `DuplicateId`
- 中途 HTTP/连接错误（同页重试数次后仍失败）→ `SourceError`

## 演示查询（`fedq/demo.py`，全部与独立参照解释器对照）

1. **q1** A 有 `X`、`x`、`X␣` 三行：统一 `name='X'` 只取第一行；A 实际先回 3 候选（统计可见），本地精筛。
2. **q2** B 有 `name=null` 行：`NOT(name='X')` 不能选它（远端超集含它，本地残余排除）。
3. **q3** `amount>5 OR NOT(name='X')` + 全序 + limit 2：远端排序首行 id=2 在本地被过滤，
   limit 仍取正确后继 id=5、id=3（limit 未下推）。
4. **q4a/q4b** left 连接右侧条件分别在连接前（ON）与连接后（WHERE）：手写两种不同预期；
   含重复连接键、全 null 聚合（sum=null）、大整数（2⁶²+2⁶²=2⁶³ 超 int64 精确）、count 语义。
5. **q5** distinct：`X`/`x`/`X␣` 是三个不同值。
6. **q6a** 选择性整数过滤真实下推：HTTP 只传 1 行、2 个字段；**q6b** 聚合下推只回聚合行；
   **q6c** 空集 count=0、sum=null。
7. 失败边界：坏令牌、中途 HTTP 500、坏字段类型、分页重复 id。
8. 能力退化：连接器无聚合能力时结果仍正确。

## 独立参照解释器（`fedq/reference.py`）

纯 Python、袋语义（保留重数）、自己的三值逻辑实现，与执行器不共享求值代码；
只跑小数据，用于对照完整结果（测试中对全部演示查询逐行比对）。

## 已知限制（第一版）

- 不接受任意 SQL 文本、任意函数、相关子查询；连接为单等值键 inner/left。
- 过滤不能位于聚合之上（无 HAVING）；不嵌套聚合；不连续两级 sort。
- 远端单条响应有公开大小上限；客户端按行 id 去重检测需要结果携带 id 列。
- 聚合/limit 不下推跨连接；distinct 总是本地。
