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
"""

__version__ = "0.1.0"
