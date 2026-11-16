"""统一语义：用户可见结果的唯一裁判。

规则（与远端方言无关）：
- 类型：int64（精确）、text、bool、null。bool 不混作 int。
- 文本比较：Unicode 码点顺序，区分大小写，不去尾空格（Python str 比较即码点序）。
- 三值逻辑：true / false / unknown；过滤只有 true 通过。
- eq / lt 任一操作数为 null → unknown。
"""

INT64_MIN = -(2 ** 63)
INT64_MAX = 2 ** 63 - 1

T_INT = "int"
T_TEXT = "text"
T_BOOL = "bool"
T_NULL = "null"


class _Unknown:
    _inst = None

    def __new__(cls):
        if cls._inst is None:
            cls._inst = super().__new__(cls)
        return cls._inst

    def __repr__(self):
        return "UNKNOWN"

    def __bool__(self):
        raise TypeError("UNKNOWN 没有布尔真值；三值逻辑下只有 true 通过过滤")


UNKNOWN = _Unknown()


class SemanticsError(TypeError):
    """类型层面的语义错误（如 bool 与 int 混比、lt 作用于 bool）。"""


def type_of(v):
    if v is None:
        return T_NULL
    if isinstance(v, bool):  # 必须先于 int 判断：bool 不混作 int
        return T_BOOL
    if isinstance(v, int):
        return T_INT
    if isinstance(v, str):
        return T_TEXT
    raise SemanticsError(f"不支持的值类型: {type(v).__name__}")


def check_int64(v):
    if not (INT64_MIN <= v <= INT64_MAX):
        raise SemanticsError(f"整数字面量超出 int64 范围: {v}")


def check_comparable(ta, tb, op):
    """静态类型检查；null 字面量可与任何类型比较（结果必为 unknown）。"""
    if ta == T_NULL or tb == T_NULL:
        return
    if ta != tb:
        raise SemanticsError(f"{op}: 操作数类型不一致 ({ta} vs {tb})；bool 不混作 int")
    if op == "lt" and ta == T_BOOL:
        raise SemanticsError("lt 不支持 bool 操作数")


def u_eq(a, b):
    """统一等值：任一 null → unknown；文本按码点、区分大小写、不去尾空格。"""
    if a is None or b is None:
        return UNKNOWN
    check_comparable(type_of(a), type_of(b), "eq")
    return a == b


def u_lt(a, b):
    """统一小于：任一 null → unknown；文本为码点序（str.__lt__ 即码点序）。"""
    if a is None or b is None:
        return UNKNOWN
    check_comparable(type_of(a), type_of(b), "lt")
    return a < b


def tv_not(v):
    if v is UNKNOWN:
        return UNKNOWN
    return not v


def tv_and(a, b):
    if a is False or b is False:
        return False
    if a is UNKNOWN or b is UNKNOWN:
        return UNKNOWN
    return True


def tv_or(a, b):
    if a is True or b is True:
        return True
    if a is UNKNOWN or b is UNKNOWN:
        return UNKNOWN
    return False


def passes(v):
    """过滤语义：只有 true 通过。"""
    return v is True
