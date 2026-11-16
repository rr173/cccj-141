"""fedq — 联邦查询执行器（参考实现）。

模块划分：
- semantics   统一语义（三值逻辑、文本码点序、bool 不混作 int）。用户结果以此为准。
- expr        过滤表达式 AST：eq / lt / is_null / AND / OR / NOT。
- query       有类型的查询树（投影/过滤/连接/分组聚合/distinct/全序 order/limit）。
- caps        连接器能力声明。
- server      两个可真实请求的本地 HTTP 模拟数据源（A、B），按各自公开规则真实执行。
- client      连接器客户端：分页、稳定令牌、类型校验、重复 id 检测、统计。
- planner     依据能力决定远端提前做什么，剩余语义本地完成；给出每条依据。
- executor    本地执行（SQLite 落盘 + Python），流式输出，部分流不得标记为完整。
- reference   独立的纯 Python 关系解释器（小数据、袋语义），用于对照。
- versions / version_server / viewstore / coordinator
              跨源读取视图：依赖闭合的版本向量、真实冻结、失败清理。
- lineage     结果数据血缘：带血缘执行、可展开证据、按源行反查（SQLite 证据库）。
- lineage_ref 独立小样本血缘参照（自行枚举输入出现并推导，不调用生产血缘代码）。
- subscription 持续查询订阅的增量维护引擎：袋重数差分沿查询树传播，连接键索引、
               distinct 支持计数、聚合分组状态与变更台账全部落 SQLite。
- sub_service 订阅生命周期：登记/推进/读取/分页/状态/停止/重建/崩溃恢复。
- sub_scenarios 订阅小夹具（订单/配送）与运行时规模数据。
- sub_oracle  订阅独立核对器：从完整表重算预期袋与差分，不调用增量维护代码。
"""

__version__ = "0.1.0"
