"""命令行：serve / plan / run / demo。

  python -m fedq serve --source A --port 8011 --log A.http.jsonl
  python -m fedq serve --source B --port 8012 --log B.http.jsonl
  python -m fedq plan --query q.json
  python -m fedq run  --query q.json --out r.jsonl --print
  python -m fedq demo --workdir /tmp/fedq-demo
"""

from __future__ import annotations

import argparse
import json
import sys

from . import datasets, query as Q
from .client import Client
from .executor import Executor, QueryFailed, read_result
from .planner import Planner, render_plan

DEFAULT_PORTS = {"A": 8011, "B": 8012}


def _clients(args):
    urls = {"A": args.a_url, "B": args.b_url}
    clients = {}
    caps = {}
    for name, url in urls.items():
        c = Client(name, url)
        caps[name] = c.capabilities()
        clients[name] = c
    return clients, caps


def cmd_serve(args):
    from .server import serve
    if args.source == "A":
        caps, tables = datasets.source_a(args.max_page_bytes)
    else:
        caps, tables = datasets.source_b(args.max_page_bytes)
    serve(caps, tables, args.log, "127.0.0.1", args.port)


def cmd_plan(args):
    _, caps = _clients(args)
    with open(args.query, encoding="utf-8") as f:
        q = Q.from_json(json.load(f))
    plan = Planner(caps).plan(q)
    print(render_plan(plan))


def cmd_run(args):
    clients, caps = _clients(args)
    with open(args.query, encoding="utf-8") as f:
        q = Q.from_json(json.load(f))
    plan = Planner(caps).plan(q)
    if args.show_plan:
        print(render_plan(plan))
    exe = Executor(clients, args.workdir)
    try:
        stats, rows = exe.execute(plan, args.out)
    except QueryFailed as e:
        print(f"查询失败: {e}", file=sys.stderr)
        print(f"部分结果（若有）在 {args.out}，无成功结束标记，不得视为完整",
              file=sys.stderr)
        sys.exit(1)
    r = read_result(args.out)
    assert r["complete"]
    print(f"完成: {r['rows'] and len(r['rows']) or 0} 行 → {args.out}")
    print(f"统计: {json.dumps(stats['sources'], ensure_ascii=False)}")
    if args.print_rows:
        print(f"列: {r['columns']}")
        for row in r["rows"]:
            print(f"  {row}")


def cmd_demo(args):
    from .demo import run_demo
    run_demo(args.workdir)


def cmd_queries(args):
    """把演示查询写成 JSON 文件，便于用 plan/run 复做。"""
    import os
    from .demo import demo_queries
    os.makedirs(args.out_dir, exist_ok=True)
    for name, qjson, note in demo_queries():
        path = os.path.join(args.out_dir, f"{name}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(qjson, f, ensure_ascii=False, indent=1)
        print(f"{path}\n    # {note}")


def main(argv=None):
    p = argparse.ArgumentParser(prog="fedq", description="联邦查询执行器")
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("serve", help="启动模拟数据源")
    s.add_argument("--source", choices=["A", "B"], required=True)
    s.add_argument("--port", type=int, required=True)
    s.add_argument("--log", required=True, help="HTTP 日志 JSONL 路径")
    s.add_argument("--max-page-bytes", type=int, default=32768)
    s.set_defaults(fn=cmd_serve)

    for name in ("plan", "run"):
        s = sub.add_parser(name)
        s.add_argument("--query", required=True, help="查询树 JSON 文件")
        s.add_argument("--a-url", default=f"http://127.0.0.1:{DEFAULT_PORTS['A']}")
        s.add_argument("--b-url", default=f"http://127.0.0.1:{DEFAULT_PORTS['B']}")
        if name == "plan":
            s.set_defaults(fn=cmd_plan)
        else:
            s.add_argument("--out", required=True)
            s.add_argument("--workdir", default="/tmp/fedq-work")
            s.add_argument("--print", dest="print_rows", action="store_true")
            s.add_argument("--show-plan", action="store_true")
            s.set_defaults(fn=cmd_run)

    s = sub.add_parser("demo", help="端到端演示（内嵌启动两个模拟源）")
    s.add_argument("--workdir", default="/tmp/fedq-demo")
    s.set_defaults(fn=cmd_demo)

    s = sub.add_parser("queries", help="导出演示查询 JSON 文件")
    s.add_argument("--out-dir", required=True)
    s.set_defaults(fn=cmd_queries)

    args = p.parse_args(argv)
    args.fn(args)


if __name__ == "__main__":
    main()
