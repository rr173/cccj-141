"""跨源读取视图协调器：打开依赖闭合的读取批并在其上执行多份查询。

生命周期：
- open()   对每个源取 /metadata（记住实例身份与观察盒 H/L），求分量最大闭合向量 F，
           逐源把恰好截至 F[s] 的真实表状态**完整冻结**到本视图的 SQLite 库，
           任一源失败或实例身份不符则整体不登记、回滚并清理临时保护；
- describe() 查看 F、H/L、退回解释、源身份、字段声明、每一页 HTTP 读取依据；
- run()    在冻结库上执行已有类型查询树（统一值语义不变），所有查询绑定同一 F；
- close()  关闭后拒绝新查询，进行中的查询允许跑完；
- cleanup_failed()  清理失败建立遗留的源保护与半成品副本，不碰其他视图。

冻结策略意味着打开成功后查询不再访问源：源继续发布、删除行或淘汰旧状态都不改变
已打开视图的结果；协调进程重启后凭目录库与冻结库继续服务原 view_id。
"""

from __future__ import annotations

import json
import os
import sqlite3
import urllib.error
import urllib.request

from . import caps as C
from . import query as Q
from .client import Stats
from .executor import Executor, QueryFailed, read_result
from .planner import Planner
from .versions import (NoClosedVector, history_from_metadata,
                       select_vector, retreat_summary, validate_metadata)
from .viewstore import ViewError, ViewStore


class CoordinatorError(Exception):
    pass


# ------------------------------------------------------------ 版本源 HTTP 客户端

class VersionClient:
    def __init__(self, name, url, timeout=10, max_retries=3):
        self.name = name
        self.url = url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.stats = Stats()

    def _post(self, path, payload, retry=True):
        data = json.dumps(payload).encode("utf-8")
        attempts = self.max_retries if retry else 1
        last = None
        for i in range(attempts):
            self.stats.requests += 1
            req = urllib.request.Request(
                self.url + path, data=data, method="POST",
                headers={"Content-Type": "application/json"})
            try:
                with urllib.request.urlopen(req, timeout=self.timeout) as r:
                    raw = r.read()
                    self.stats.bytes += len(raw)
                    return json.loads(raw)
            except urllib.error.HTTPError as e:
                raw = e.read()
                self.stats.bytes += len(raw)
                try:
                    msg = json.loads(raw).get("error", str(e))
                except Exception:
                    msg = str(e)
                last = CoordinatorError(f"源 {self.name} HTTP {e.code}: {msg}")
                # 400/409 是语义错误、500 是源端明确失败：重试只会把一次性故障“试过去”，
                # 违背“一次建立里某源读取失败就不得登记”的要求，因此都不重试。
                if e.code in (400, 409, 500):
                    break
            except (urllib.error.URLError, ConnectionError, TimeoutError) as e:
                last = CoordinatorError(f"源 {self.name} 连接错误: {e}")
            if i + 1 < attempts:
                import time as _t
                _t.sleep(0.05 * (i + 1))
        raise last

    def _get(self, path):
        self.stats.requests += 1
        try:
            with urllib.request.urlopen(self.url + path, timeout=self.timeout) as r:
                raw = r.read()
                self.stats.bytes += len(raw)
                return json.loads(raw)
        except urllib.error.HTTPError as e:
            raise CoordinatorError(f"源 {self.name} HTTP {e.code}: {e.read()!r}")
        except (urllib.error.URLError, ConnectionError, TimeoutError) as e:
            raise CoordinatorError(f"源 {self.name} 连接错误: {e}")

    def metadata(self):
        return self._get("/metadata")

    def open_snapshot(self, at, view_id):
        return self._post("/snapshot/open", {"at": at, "view_id": view_id})

    def page(self, snapshot_id, table, page):
        return self._post("/snapshot/page",
                          {"snapshot_id": snapshot_id, "table": table, "page": page})

    def release(self, snapshot_id):
        return self._post("/snapshot/release", {"snapshot_id": snapshot_id},
                          retry=False)

    # ---- 持续查询订阅的按位置变更入口 ----
    def changes_open(self, after, subscription_id):
        return self._post("/changes/open",
                          {"after": after, "subscription_id": subscription_id})

    def changes_page(self, change_id, page):
        return self._post("/changes/page",
                          {"change_id": change_id, "page": page})

    def changes_release(self, change_id):
        return self._post("/changes/release", {"change_id": change_id},
                          retry=False)

    # 管理接口（演示用）
    def admin(self, path, payload=None):
        if path.startswith("/admin/state"):
            return self._get(path)
        return self._post(path, payload or {}, retry=False)


# ------------------------------------------------------------ 冻结客户端

def frozen_caps(dialect, source, schema_json):
    """按方言构造只保留 scan/project 的能力：其余算子全部在协调者本地执行。

    方言的文本/null 语义差异只影响“能否下推”的判断；这里把过滤/聚合/排序/limit
    全部留在本地（统一语义），冻结端只负责按列吐出已冻结的行，因此查询答案与
    源方言无关，统一语义仍是唯一裁判。
    """
    if dialect == "A":
        text_eq, text_order, null_cmp = (
            C.EQ_FOLD_AZ_RSTRIP, C.ORD_NORMALIZED, C.NULL_UNKNOWN)
    else:
        text_eq, text_order, null_cmp = (
            C.EQ_UNIFIED, C.ORD_UTF8_BYTES, C.NULL_FALSE)
    tables = {t: C.TableSchema(d["columns"], d.get("key", ["id"]))
              for t, d in schema_json.items()}
    return C.Caps(source=source, tables=tables, text_eq=text_eq,
                  text_order=text_order, null_compare=null_cmp,
                  filter_ops=frozenset(), project=True,
                  aggregates=frozenset(), sort=False, limit=False,
                  max_page_bytes=10 ** 9)


class FrozenClient:
    """与 client.Client 同构的读取端，但数据来自视图冻结库，永不发起 HTTP。"""

    def __init__(self, name, frozen_db_path, schema_json, batch=256,
                 on_batch=None):
        self.name = name
        self.frozen_db_path = frozen_db_path
        self.schema_json = schema_json
        self.batch = batch
        self.on_batch = on_batch          # 每读完一批回调（演示翻页期推进源版本）
        self.stats = Stats()
        self._con = None

    def _connect(self):
        if self._con is None:
            self._con = sqlite3.connect(self.frozen_db_path)
        return self._con

    def capabilities(self):
        return frozen_caps(self._dialect, self.name, self.schema_json)

    def bind_dialect(self, dialect):
        self._dialect = dialect
        return self

    def stream(self, plan):
        """只接受 project(scan) 形态的下推计划，逐批读冻结库，逐行产出 dict。"""
        if plan["op"] != "project":
            raise CoordinatorError("冻结端只支持 project(scan) 下推")
        cols = list(plan["cols"])
        node = plan["input"]
        if node["op"] != "scan":
            raise CoordinatorError("冻结端下推计划非法")
        table = node["table"]
        all_cols = list(self.schema_json[table]["columns"].keys())
        con = self._connect()
        cur = con.execute(
            "SELECT jrow FROM frozen_rows WHERE tbl=? ORDER BY page,rowidx",
            (table,))
        self.stats.fields.update(cols)
        while True:
            chunk = cur.fetchmany(self.batch)
            if not chunk:
                break
            for (jrow,) in chunk:
                full_list = json.loads(jrow)          # 行值按表列顺序存储
                full = dict(zip(all_cols, full_list))
                row = {c: full[c] for c in cols}
                self.stats.rows += 1
                yield row
            if self.on_batch is not None:
                self.on_batch(self.name, table, len(chunk))

    def close(self):
        if self._con is not None:
            self._con.close()
            self._con = None


# ------------------------------------------------------------ 协调器

class Coordinator:
    def __init__(self, workdir):
        self.workdir = workdir
        os.makedirs(workdir, exist_ok=True)
        self.store = ViewStore(workdir)
        self._clients = {}          # 运行期复用的 VersionClient（按 url）

    def _client(self, name, url):
        key = (name, url)
        if key not in self._clients:
            self._clients[key] = VersionClient(name, url)
        return self._clients[key]

    # ------------------------------------------------ 打开
    def open(self, config, note=""):
        sources = config["sources"]
        names = sorted(sources)
        clients = {n: self._client(n, sources[n]["url"]) for n in names}

        # 1) 取元数据、记住实例身份与观察盒（后续发布不并入本次打开）
        #    若配置给出 expect_instance，则该地址必须仍是同一实例（同地址换实例即拒）。
        metas, H, L, instances = {}, {}, {}, {}
        for n in names:
            meta = clients[n].metadata()
            validate_metadata(meta, set(names))
            expected = sources[n].get("expect_instance")
            if expected is not None and meta["instance_id"] != expected:
                raise CoordinatorError(
                    f"源 {n} 地址 {sources[n]['url']} 的实例身份已更换："
                    f"期望 {expected}，实际 {meta['instance_id']}，拒绝打开")
            metas[n] = meta
            H[n], L[n] = meta["head"], meta["earliest"]
            instances[n] = meta["instance_id"]

        # 2) 求分量最大依赖闭合向量
        histories = {n: history_from_metadata(metas[n]) for n in names}
        try:
            F, trace = select_vector(histories, H, L)
        except NoClosedVector:
            raise
        retreat = retreat_summary(H, F, trace)

        # 3) 建立期：逐源冻结，任一失败整体回滚
        view_id = self.store.begin_build(config, note)
        frozen_path, fcon = self.store.new_frozen_db(view_id)
        held = {}                  # source -> snapshot_id
        source_records = []
        try:
            for n in names:
                resp = clients[n].open_snapshot(F[n], view_id)
                # 实例身份核验：同地址换成另一个 instance_id 必须失败
                if resp["instance_id"] != instances[n]:
                    raise CoordinatorError(
                        f"源 {n} 实例身份在建立期间变化：{instances[n]} → "
                        f"{resp['instance_id']}，拒绝登记部分输入")
                snap_id = resp["snapshot_id"]
                held[n] = snap_id
                self.store.stage_protection(view_id, n, sources[n]["url"],
                                            snap_id, frozen_path)
                rows_total = 0
                for table, tmeta in resp["tables"].items():
                    page = 0
                    while True:
                        pr = clients[n].page(snap_id, table, page)
                        if pr["instance_id"] != instances[n]:
                            raise CoordinatorError(
                                f"源 {n} 翻页期间实例身份变化，建立失败")
                        self.store.insert_frozen_rows(fcon, table, page, pr["rows"])
                        nbytes = len(json.dumps(pr["rows"], ensure_ascii=False))
                        self.store.record_http_read(
                            view_id, n, instances[n], F[n], snap_id,
                            table, page, len(pr["rows"]), nbytes)
                        rows_total += len(pr["rows"])
                        if pr["next_page"] is None:
                            break
                        page = pr["next_page"]
                fcon.commit()
                source_records.append({
                    "source": n, "instance_id": instances[n], "at": F[n],
                    "head": H[n], "earliest": L[n], "url": sources[n]["url"],
                    "dialect": metas[n]["dialect"],
                    "schema_json": metas[n]["tables"],
                    "snapshot_id": snap_id, "frozen_db": frozen_path,
                    "rows_total": rows_total})
        except Exception as e:
            # 不自动释放：已取得的源端保护保留在 build_staging 中，
            # 由显式 cleanup_failed 入口统一回收（含半成品冻结库）。
            fcon.close()
            self.store.fail_build(view_id, str(e))
            err = CoordinatorError(f"打开视图失败（未登记任何可用视图，"
                                   f"失败 view_id={view_id} 待清理）: {e}")
            err.failed_view_id = view_id
            raise err

        fcon.close()
        # 4) 全部冻结成功 → 原子登记为 open，随后释放源端保护（本地冻结已接管保留责任）
        self.store.commit_view(view_id, F, H, L, trace, retreat, source_records)
        for n, snap_id in held.items():
            try:
                clients[n].release(snap_id)
            except Exception:
                pass
        return {"view_id": view_id, "F": F, "H": H, "L": L,
                "trace": trace, "retreat": retreat,
                "sources": [{"source": r["source"], "instance_id": r["instance_id"],
                             "at": r["at"], "rows_total": r["rows_total"]}
                            for r in source_records]}

    # ------------------------------------------------ 查看
    def describe(self, view_id):
        return self.store.describe(view_id)

    def list_views(self):
        return self.store.list_views()

    # ------------------------------------------------ 运行查询
    def run(self, view_id, qjson, out_path, name="query", on_batch=None,
            print_rows=False):
        # 关闭后拒绝新查询；begin_query 在存储层做状态校验
        self.store.begin_query(view_id, name)
        frozen_clients = []
        finished = False
        try:
            self.store.get_open_view(view_id)
            srcs = self.store.view_sources(view_id)
            frozen_path = self.store.frozen_db_path(view_id)
            if not os.path.exists(frozen_path):
                raise ViewError(f"视图 {view_id} 的冻结库缺失（可能已关闭）")
            caps, clients = {}, {}
            for r in srcs:
                schema_json = json.loads(r["schema_json"])
                fc = FrozenClient(r["source"], frozen_path, schema_json,
                                  on_batch=on_batch).bind_dialect(r["dialect"])
                frozen_clients.append(fc)
                clients[r["source"]] = fc
                caps[r["source"]] = fc.capabilities()

            q = Q.from_json(qjson)
            plan = Planner(caps).plan(q)     # 仅 scan/project 下推，其余本地
            run_dir = os.path.join(self.workdir, "runs", view_id,
                                   name.replace("/", "_"))
            os.makedirs(run_dir, exist_ok=True)
            exe = Executor(clients, run_dir)
            try:
                stats, rows = exe.execute(plan, out_path)
            except QueryFailed as e:
                self.store.end_query(view_id, name, "error", None, 0, str(e))
                finished = True
                raise
            result = read_result(out_path)
            self.store.end_query(view_id, name, "ok", result["columns"],
                                 len(result["rows"]),
                                 json.dumps(stats, ensure_ascii=False))
            finished = True
            return plan, result, stats
        except Exception as e:
            if not finished:
                try:
                    self.store.end_query(view_id, name, "error", None, 0, str(e))
                except Exception:
                    pass
            raise
        finally:
            for fc in frozen_clients:
                fc.close()

    # ------------------------------------------------ 带血缘执行
    def lineage_run(self, view_id, qjson, name="lineage", fail_hook=None):
        """在视图冻结数据上做一次**带血缘执行**（结果数据血缘，非下推计划解释）。

        返回 {"run_id", "state", "columns", "rows_out", ...}；失败抛 LineageError
        （携带 run_id，目录已标记 error）。之后的读取结果/展开/反查走
        LineageService（同一 workdir），不依赖视图是否仍打开。
        """
        from .lineage import LineageService
        ledger = f"lineage:{name}"
        self.store.begin_query(view_id, ledger)
        finished = False
        try:
            self.store.get_open_view(view_id)
            srcs = self.store.view_sources(view_id)
            frozen_path = self.store.frozen_db_path(view_id)
            if not os.path.exists(frozen_path):
                raise ViewError(f"视图 {view_id} 的冻结库缺失（可能已关闭）")
            service = LineageService(self.workdir)
            try:
                info = service.start_run(view_id, qjson, srcs, frozen_path,
                                         name=name, fail_hook=fail_hook)
            except Exception as e:
                self.store.end_query(view_id, ledger, "error", None, 0, str(e))
                finished = True
                raise
            self.store.end_query(view_id, ledger, "ok", info["columns"],
                                 info["rows_out"],
                                 json.dumps({"run_id": info["run_id"],
                                             "results_sha256":
                                             info["results_sha256"]},
                                            ensure_ascii=False))
            finished = True
            return info
        finally:
            if not finished:
                try:
                    self.store.end_query(view_id, ledger, "error", None, 0,
                                         "interrupted")
                except Exception:
                    pass

    # ------------------------------------------------ 关闭 / 清理
    def close(self, view_id):
        return self.store.close(view_id)

    def cleanup_failed(self, view_id):
        def release_cb(source, snapshot_id):
            # 从建立暂存记录找 url，构造客户端释放；失败不阻断本地清理
            url = next((st["url"] for st in self.store.staging(view_id)
                        if st["source"] == source and
                        st["snapshot_id"] == snapshot_id), None)
            if url:
                try:
                    self._client(source, url).release(snapshot_id)
                except Exception:
                    pass
        return self.store.cleanup_failed(view_id, release_cb)

    def client_for(self, name, url):
        return self._client(name, url)
