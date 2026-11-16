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
    if getattr(args, "view_id", None):
        # 视图入口：查询绑定视图的固定 F，从本地冻结库读取，不访问实时源
        from .coordinator import Coordinator
        from .executor import read_result
        coord = Coordinator(args.workdir)
        with open(args.query, encoding="utf-8") as f:
            qjson = json.load(f)
        plan, result, stats = coord.run(
            args.view_id, qjson, args.out, name=args.name or "cli-run")
        assert read_result(args.out)["complete"]
        print(f"视图 {args.view_id} 完成: {len(result['rows'])} 行 → {args.out}")
        if args.print_rows:
            print(f"列: {result['columns']}")
            for row in result["rows"]:
                print(f"  {row}")
        return
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


# ------------------------------------------------------------ 跨源读取视图

def _load_json(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def cmd_serve_history(args):
    """启动一个版本化模拟源（真实提交历史、读取保护、管理接口）。"""
    from .version_server import serve_version
    from .versions import history_from_config
    hist = history_from_config(_load_json(args.history))
    serve_version(hist, args.log, "127.0.0.1", args.port, args.max_page_bytes)


def cmd_view_demo(args):
    from .demo_views import run
    run(args.workdir)


def cmd_view_open(args):
    from .coordinator import Coordinator
    coord = Coordinator(args.workdir)
    config = _load_json(args.config)
    res = coord.open(config, note=args.note or "")
    print(json.dumps(res, ensure_ascii=False, indent=1))


def cmd_view_list(args):
    from .coordinator import Coordinator
    for v in Coordinator(args.workdir).list_views():
        print(json.dumps(v, ensure_ascii=False))


def cmd_view_show(args):
    from .coordinator import Coordinator
    d = Coordinator(args.workdir).describe(args.view_id)
    print(json.dumps(d, ensure_ascii=False, indent=1))


def cmd_view_run(args):
    from .coordinator import Coordinator
    from .executor import read_result
    coord = Coordinator(args.workdir)
    qjson = _load_json(args.query)
    plan, result, stats = coord.run(args.view_id, qjson, args.out,
                                    name=args.name or "cli-query")
    print(f"完成: {len(result['rows'])} 行 → {args.out}")
    if args.print_rows:
        print(f"列: {result['columns']}")
        for row in result["rows"]:
            print(f"  {row}")
    print("源读取统计: " + json.dumps(stats["sources"], ensure_ascii=False))


def cmd_view_close(args):
    from .coordinator import Coordinator
    print(json.dumps(Coordinator(args.workdir).close(args.view_id),
                     ensure_ascii=False))


def cmd_view_cleanup(args):
    from .coordinator import Coordinator
    info = Coordinator(args.workdir).cleanup_failed(args.view_id)
    print(json.dumps(info, ensure_ascii=False))


def cmd_admin(args):
    """对版本源发管理动作（发布/淘汰/替换/注入失败/查状态）。"""
    from .coordinator import VersionClient
    c = VersionClient(args.source, args.url)
    payload = None
    if args.head is not None:
        path, payload = "/admin/publish", {"head": args.head}
    elif args.earliest is not None:
        path, payload = "/admin/earliest", {"earliest": args.earliest}
    elif args.new_instance:
        path, payload = "/admin/replace-instance", {"instance_id": args.new_instance}
    elif args.fail_route:
        path, payload = "/admin/fail", {"route": args.fail_route}
    else:
        path = "/admin/state"
    print(json.dumps(c.admin(path, payload), ensure_ascii=False, indent=1))


def cmd_enumcheck(args):
    from .enumcheck import main as enum_main
    raise SystemExit(enum_main(["--config", args.config] if args.config else
                               ["--metadata-dir", args.metadata_dir]))


def cmd_gen_scenarios(args):
    """导出三源历史与协调者配置文件（供分进程 serve-history 与 enumcheck 复做）。"""
    from . import scenarios as SC
    histories = SC.ring_histories()
    urls = {n: f"http://127.0.0.1:{p}" for n, p in
            zip(("A", "B", "C"), (args.port_a, args.port_b, args.port_c))}
    path = SC.write_configs(histories, urls, args.out_dir)
    print("已写出历史配置:")
    for n in ("A", "B", "C"):
        print(f"  {args.out_dir}/{n}.json   # {urls[n]}")
    print(f"协调者配置: {path}")


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


# ------------------------------------------------------------ 结果数据血缘

def _lineage_service(args):
    from .lineage import LineageService
    return LineageService(args.workdir)


def cmd_lineage_run(args):
    """开始一次带血缘执行（需要仍打开的视图）。"""
    from .coordinator import Coordinator
    from .lineage import LineageError
    coord = Coordinator(args.workdir)
    qjson = _load_json(args.query)
    try:
        info = coord.lineage_run(args.view_id, qjson,
                                 name=args.name or "cli-lineage")
    except LineageError as e:
        print(f"带血缘执行失败: {e}", file=sys.stderr)
        sys.exit(1)
    print(json.dumps(info, ensure_ascii=False, indent=1))
    first = _lineage_service(args).rows(info["run_id"], 0, args.page_size)
    print(f"结果第 0 页（共 {first['total']} 行，列 {first['columns']}）:")
    for it in first["items"]:
        print(f"  {it['row_ref']}  {json.dumps(it['values'], ensure_ascii=False)}")


def cmd_lineage_runs(args):
    for r in _lineage_service(args).list_runs():
        print(json.dumps(r, ensure_ascii=False))


def cmd_lineage_show(args):
    print(json.dumps(_lineage_service(args).show(args.run_id),
                     ensure_ascii=False, indent=1))


def cmd_lineage_rows(args):
    r = _lineage_service(args).rows(args.run_id, args.page, args.page_size)
    print(json.dumps(r, ensure_ascii=False, indent=1))


def cmd_lineage_expand(args):
    r = _lineage_service(args).expand(args.row_ref, args.page, args.page_size)
    print(json.dumps(r, ensure_ascii=False, indent=1))


def cmd_lineage_lookup(args):
    try:
        pk = json.loads(args.pk)
    except json.JSONDecodeError:
        pk = args.pk
    if not isinstance(pk, list):
        pk = [pk]
    r = _lineage_service(args).lookup(
        args.run_id, args.source, args.table, pk, path=args.path,
        instance_id=args.instance_id, at=args.at,
        page=args.page, page_size=args.page_size)
    print(json.dumps(r, ensure_ascii=False, indent=1))


def cmd_lineage_demo(args):
    from .demo_lineage import run
    run(args.workdir)


def cmd_lineage_fixtures(args):
    """导出血缘演示夹具（两源历史 + 查询 + 协调者配置），供分进程复做。"""
    import os
    from . import scenarios as SC
    from .demo_lineage import fixture_histories, lineage_queries
    os.makedirs(args.out_dir, exist_ok=True)
    hists = fixture_histories(args.scale_rows)
    urls = {"A": f"http://127.0.0.1:{args.port_a}",
            "B": f"http://127.0.0.1:{args.port_b}"}
    SC.write_configs(hists, urls, args.out_dir)
    for name, qjson, note in lineage_queries():
        with open(os.path.join(args.out_dir, f"q_{name}.json"), "w",
                  encoding="utf-8") as f:
            json.dump(qjson, f, ensure_ascii=False, indent=1)
    print(f"已写出夹具: {args.out_dir}/A.json B.json coordinator.json "
          f"q_*.json（scale 行数 {args.scale_rows}）")


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
            # 视图入口：给定 view_id 后查询绑定视图固定 F，从冻结库读取
            s.add_argument("--view-id", default=None,
                           help="在已打开视图内执行（绑定该视图的 F，本地冻结读取）")
            s.add_argument("--name", default=None, help="本次查询在视图账本中的名字")
            s.set_defaults(fn=cmd_run)

    s = sub.add_parser("demo", help="端到端演示（内嵌启动两个模拟源）")
    s.add_argument("--workdir", default="/tmp/fedq-demo")
    s.set_defaults(fn=cmd_demo)

    s = sub.add_parser("queries", help="导出演示查询 JSON 文件")
    s.add_argument("--out-dir", required=True)
    s.set_defaults(fn=cmd_queries)

    # ---------------- 跨源读取视图（版本化） ----------------
    s = sub.add_parser("serve-history", help="启动一个版本化模拟数据源")
    s.add_argument("--history", required=True, help="单源历史 JSON（gen-scenarios 产出）")
    s.add_argument("--port", type=int, required=True)
    s.add_argument("--log", required=True)
    s.add_argument("--max-page-bytes", type=int, default=8192)
    s.set_defaults(fn=cmd_serve_history)

    s = sub.add_parser("gen-scenarios", help="导出三源历史与协调者配置")
    s.add_argument("--out-dir", required=True)
    s.add_argument("--port-a", type=int, default=8021)
    s.add_argument("--port-b", type=int, default=8022)
    s.add_argument("--port-c", type=int, default=8023)
    s.set_defaults(fn=cmd_gen_scenarios)

    s = sub.add_parser("view-demo", help="跨源读取视图全部验收场景（内嵌三源）")
    s.add_argument("--workdir", default="/tmp/fedq-views-demo")
    s.set_defaults(fn=cmd_view_demo)

    s = sub.add_parser("view-open", help="打开依赖闭合的读取视图，返回 view_id")
    s.add_argument("--config", required=True, help="逻辑源名→HTTP 地址配置 JSON")
    s.add_argument("--workdir", default="/tmp/fedq-views")
    s.add_argument("--note", default=None)
    s.set_defaults(fn=cmd_view_open)

    s = sub.add_parser("view-list", help="列出视图及其状态")
    s.add_argument("--workdir", default="/tmp/fedq-views")
    s.set_defaults(fn=cmd_view_list)

    s = sub.add_parser("view-show", help="查看视图：F、退回解释、源身份与 HTTP 读取依据")
    s.add_argument("--view-id", required=True)
    s.add_argument("--workdir", default="/tmp/fedq-views")
    s.set_defaults(fn=cmd_view_show)

    s = sub.add_parser("view-run", help="在视图内执行查询（绑定同一 F）")
    s.add_argument("--view-id", required=True)
    s.add_argument("--query", required=True)
    s.add_argument("--out", required=True)
    s.add_argument("--name", default=None)
    s.add_argument("--workdir", default="/tmp/fedq-views")
    s.add_argument("--print", dest="print_rows", action="store_true")
    s.set_defaults(fn=cmd_view_run)

    s = sub.add_parser("view-close", help="关闭视图（拒绝新查询，回收冻结副本）")
    s.add_argument("--view-id", required=True)
    s.add_argument("--workdir", default="/tmp/fedq-views")
    s.set_defaults(fn=cmd_view_close)

    s = sub.add_parser("view-cleanup", help="清理失败建立遗留的临时保护与副本")
    s.add_argument("--view-id", required=True)
    s.add_argument("--workdir", default="/tmp/fedq-views")
    s.set_defaults(fn=cmd_view_cleanup)

    s = sub.add_parser("admin", help="版本源管理动作（发布/淘汰/替换/注入/状态）")
    s.add_argument("--source", required=True)
    s.add_argument("--url", required=True)
    s.add_argument("--head", type=int, default=None)
    s.add_argument("--earliest", type=int, default=None)
    s.add_argument("--new-instance", default=None)
    s.add_argument("--fail-route", default=None)
    s.set_defaults(fn=cmd_admin)

    s = sub.add_parser("enumcheck", help="独立穷举所有候选向量核对最大闭合向量")
    s.add_argument("--config", default=None)
    s.add_argument("--metadata-dir", default=None)
    s.set_defaults(fn=cmd_enumcheck)

    # ---------------- 结果数据血缘（第三部分） ----------------
    s = sub.add_parser("lineage-run",
                       help="开始一次带血缘执行（视图内），返回 run_id 与 row_ref")
    s.add_argument("--view-id", required=True)
    s.add_argument("--query", required=True)
    s.add_argument("--name", default=None)
    s.add_argument("--workdir", default="/tmp/fedq-views")
    s.add_argument("--page-size", type=int, default=20)
    s.set_defaults(fn=cmd_lineage_run)

    s = sub.add_parser("lineage-runs", help="列出全部血缘 run 及其状态")
    s.add_argument("--workdir", default="/tmp/fedq-views")
    s.set_defaults(fn=cmd_lineage_runs)

    s = sub.add_parser("lineage-show",
                       help="查看 run 元数据（查询/源坐标/状态）并核对结果校验和")
    s.add_argument("--run-id", required=True)
    s.add_argument("--workdir", default="/tmp/fedq-views")
    s.set_defaults(fn=cmd_lineage_show)

    s = sub.add_parser("lineage-rows", help="分页读取 run 结果（每项带 row_ref）")
    s.add_argument("--run-id", required=True)
    s.add_argument("--workdir", default="/tmp/fedq-views")
    s.add_argument("--page", type=int, default=0)
    s.add_argument("--page-size", type=int, default=20)
    s.set_defaults(fn=cmd_lineage_rows)

    s = sub.add_parser("lineage-expand", help="按 row_ref 逐层展开推导（备选/成员分页）")
    s.add_argument("--row-ref", required=True)
    s.add_argument("--workdir", default="/tmp/fedq-views")
    s.add_argument("--page", type=int, default=0)
    s.add_argument("--page-size", type=int, default=20)
    s.set_defaults(fn=cmd_lineage_expand)

    s = sub.add_parser("lineage-lookup",
                       help="按源行身份反查本次哪些最终结果用到了它")
    s.add_argument("--run-id", required=True)
    s.add_argument("--source", required=True)
    s.add_argument("--table", required=True)
    s.add_argument("--pk", required=True,
                   help="主键值（单个值或 JSON 数组，如 3 或 '[3]')")
    s.add_argument("--path", default=None,
                   help="可选：限定查询树输入位置（同表两次引用时区分）")
    s.add_argument("--instance-id", default=None)
    s.add_argument("--at", type=int, default=None)
    s.add_argument("--workdir", default="/tmp/fedq-views")
    s.add_argument("--page", type=int, default=0)
    s.add_argument("--page-size", type=int, default=20)
    s.set_defaults(fn=cmd_lineage_lookup)

    s = sub.add_parser("lineage-demo", help="结果数据血缘全部验收场景（内嵌两源）")
    s.add_argument("--workdir", default="/tmp/fedq-lineage-demo")
    s.set_defaults(fn=cmd_lineage_demo)

    s = sub.add_parser("lineage-fixtures", help="导出血缘演示夹具（供分进程复做）")
    s.add_argument("--out-dir", required=True)
    s.add_argument("--scale-rows", type=int, default=2500)
    s.add_argument("--port-a", type=int, default=8031)
    s.add_argument("--port-b", type=int, default=8032)
    s.set_defaults(fn=cmd_lineage_fixtures)

    args = p.parse_args(argv)
    args.fn(args)


if __name__ == "__main__":
    main()
