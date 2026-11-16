"""过滤表达式 AST：eq / lt / is_null / AND / OR / NOT。

同一棵表达式树有三种用途：
1. eval_unified —— 本地按统一语义求值（三值逻辑，只有 true 通过）；
2. to_json / from_json —— 线上协议格式（协议只承载声明的操作，不接受任意代码）；
3. to_sql —— 本地 SQLite 执行（SQLite 的三值逻辑与 BINARY 文本序与统一语义一致，
   bool 以 0/1 存储且类型安全已在 typecheck 静态保证）。
"""

from __future__ import annotations

from dataclasses import dataclass, field as _dc_field

from . import semantics as S


class ExprError(Exception):
    pass


# ---------------------------------------------------------------- 节点定义

class Expr:
    kind = "?"

    def fields(self):
        raise NotImplementedError

    def typecheck(self, schema):
        raise NotImplementedError

    def eval(self, row):
        """统一语义求值，返回 True / False / UNKNOWN。"""
        raise NotImplementedError

    def to_json(self):
        raise NotImplementedError

    def to_sql(self, params):
        raise NotImplementedError


@dataclass
class Field(Expr):
    name: str
    typ: str = None  # typecheck 时解析
    kind: str = _dc_field(default="field", init=False)

    def fields(self):
        return {self.name}

    def typecheck(self, schema):
        if self.name not in schema:
            raise ExprError(f"未知字段: {self.name}")
        self.typ = schema[self.name]
        return self.typ

    def eval(self, row):
        return row[self.name]

    def to_json(self):
        return {"k": "field", "name": self.name}

    def to_sql(self, params):
        return f'"{self.name}"'


@dataclass
class Lit(Expr):
    typ: str          # 'int' | 'text' | 'bool' | 'null'
    value: object = None
    kind: str = _dc_field(default="lit", init=False)

    def fields(self):
        return set()

    def typecheck(self, schema):
        if self.typ == S.T_INT:
            if not isinstance(self.value, int) or isinstance(self.value, bool):
                raise ExprError("int 字面量类型错误")
            S.check_int64(self.value)
        elif self.typ == S.T_TEXT and not isinstance(self.value, str):
            raise ExprError("text 字面量类型错误")
        elif self.typ == S.T_BOOL and not isinstance(self.value, bool):
            raise ExprError("bool 字面量类型错误")
        elif self.typ == S.T_NULL:
            self.value = None
        elif self.typ not in (S.T_INT, S.T_TEXT, S.T_BOOL, S.T_NULL):
            raise ExprError(f"未知字面量类型: {self.typ}")
        return self.typ

    def eval(self, row):
        return self.value

    def to_json(self):
        v = str(self.value) if self.typ == S.T_INT and self.value is not None else self.value
        return {"k": "lit", "t": self.typ, "v": v}

    def to_sql(self, params):
        if self.typ == S.T_NULL:
            return "NULL"
        if self.typ == S.T_BOOL:
            params.append(1 if self.value else 0)
        else:
            params.append(self.value)
        return "?"


@dataclass
class Eq(Expr):
    a: Expr
    b: Expr
    kind: str = _dc_field(default="eq", init=False)

    def fields(self):
        return self.a.fields() | self.b.fields()

    def typecheck(self, schema):
        S.check_comparable(self.a.typecheck(schema), self.b.typecheck(schema), "eq")
        return S.T_BOOL

    def eval(self, row):
        return S.u_eq(self.a.eval(row), self.b.eval(row))

    def to_json(self):
        return {"k": "eq", "a": self.a.to_json(), "b": self.b.to_json()}

    def to_sql(self, params):
        return f"({self.a.to_sql(params)} = {self.b.to_sql(params)})"


@dataclass
class Lt(Expr):
    a: Expr
    b: Expr
    kind: str = _dc_field(default="lt", init=False)

    def fields(self):
        return self.a.fields() | self.b.fields()

    def typecheck(self, schema):
        S.check_comparable(self.a.typecheck(schema), self.b.typecheck(schema), "lt")
        return S.T_BOOL

    def eval(self, row):
        return S.u_lt(self.a.eval(row), self.b.eval(row))

    def to_json(self):
        return {"k": "lt", "a": self.a.to_json(), "b": self.b.to_json()}

    def to_sql(self, params):
        return f"({self.a.to_sql(params)} < {self.b.to_sql(params)})"


@dataclass
class IsNull(Expr):
    x: Expr
    kind: str = _dc_field(default="is_null", init=False)

    def fields(self):
        return self.x.fields()

    def typecheck(self, schema):
        self.x.typecheck(schema)
        return S.T_BOOL

    def eval(self, row):
        return self.x.eval(row) is None

    def to_json(self):
        return {"k": "is_null", "x": self.x.to_json()}

    def to_sql(self, params):
        return f"({self.x.to_sql(params)} IS NULL)"


@dataclass
class And(Expr):
    args: list
    kind: str = _dc_field(default="and", init=False)

    def fields(self):
        out = set()
        for a in self.args:
            out |= a.fields()
        return out

    def typecheck(self, schema):
        for a in self.args:
            a.typecheck(schema)
        return S.T_BOOL

    def eval(self, row):
        acc = True
        for a in self.args:
            acc = S.tv_and(acc, a.eval(row))
        return acc

    def to_json(self):
        return {"k": "and", "args": [a.to_json() for a in self.args]}

    def to_sql(self, params):
        return "(" + " AND ".join(a.to_sql(params) for a in self.args) + ")"


@dataclass
class Or(Expr):
    args: list
    kind: str = _dc_field(default="or", init=False)

    def fields(self):
        out = set()
        for a in self.args:
            out |= a.fields()
        return out

    def typecheck(self, schema):
        for a in self.args:
            a.typecheck(schema)
        return S.T_BOOL

    def eval(self, row):
        acc = False
        for a in self.args:
            acc = S.tv_or(acc, a.eval(row))
        return acc

    def to_json(self):
        return {"k": "or", "args": [a.to_json() for a in self.args]}

    def to_sql(self, params):
        return "(" + " OR ".join(a.to_sql(params) for a in self.args) + ")"


@dataclass
class Not(Expr):
    x: Expr
    kind: str = _dc_field(default="not", init=False)

    def fields(self):
        return self.x.fields()

    def typecheck(self, schema):
        self.x.typecheck(schema)
        return S.T_BOOL

    def eval(self, row):
        return S.tv_not(self.x.eval(row))

    def to_json(self):
        return {"k": "not", "x": self.x.to_json()}

    def to_sql(self, params):
        return f"(NOT {self.x.to_sql(params)})"


# ---------------------------------------------------------------- JSON 反序列化

def from_json(d):
    k = d["k"]
    if k == "field":
        return Field(d["name"])
    if k == "lit":
        t = d["t"]
        v = int(d["v"]) if t == S.T_INT and d["v"] is not None else d["v"]
        return Lit(t, v)
    if k == "eq":
        return Eq(from_json(d["a"]), from_json(d["b"]))
    if k == "lt":
        return Lt(from_json(d["a"]), from_json(d["b"]))
    if k == "is_null":
        return IsNull(from_json(d["x"]))
    if k == "and":
        return And([from_json(x) for x in d["args"]])
    if k == "or":
        return Or([from_json(x) for x in d["args"]])
    if k == "not":
        return Not(from_json(d["x"]))
    raise ExprError(f"未知表达式 kind: {k!r}（协议只接受声明的操作）")


def to_sql(expr):
    """返回 (sql, params)。"""
    params = []
    sql = expr.to_sql(params)
    return sql, params


def render(expr):
    """人类可读形式，用于计划展示。"""
    if isinstance(expr, Field):
        return expr.name
    if isinstance(expr, Lit):
        return "NULL" if expr.typ == S.T_NULL else repr(expr.value)
    if isinstance(expr, Eq):
        return f"eq({render(expr.a)}, {render(expr.b)})"
    if isinstance(expr, Lt):
        return f"lt({render(expr.a)}, {render(expr.b)})"
    if isinstance(expr, IsNull):
        return f"is_null({render(expr.x)})"
    if isinstance(expr, And):
        return "AND(" + ", ".join(render(a) for a in expr.args) + ")"
    if isinstance(expr, Or):
        return "OR(" + ", ".join(render(a) for a in expr.args) + ")"
    if isinstance(expr, Not):
        return f"NOT({render(expr.x)})"
    return repr(expr)
