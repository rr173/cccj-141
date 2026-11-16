"""独立枚举核对程序：小规模穷举所有候选向量，找分量最大的合法向量。

与 versions.select_vector **不共享任何选择逻辑**：这里直接笛卡尔积枚举
[L,H] 盒内的全部向量，逐个独立判定依赖闭合，再在合法集合里做分量比较求最大元。
只用于小数据核对，故意写得直白、慢也没关系。

用法：
  python -m fedq.enumcheck --config demo/three_sources.json
  python -m fedq.enumcheck --metadata-dir <存有 A.json B.json C.json 的目录>
"""

from __future__ import annotations

import argparse
import itertools
import json
import os

from .versions import history_from_config, history_from_metadata, validate_metadata


def _feasible(vec, req_index):
    """vec: {source: 位置}。req_index[s][k] = 第 k 笔提交的 requires 字典。

    闭合条件：对每个源 s、每个被纳入的提交 k<=vec[s]，
    其每条 requires[t]=d 都必须满足 vec[t] >= d。
    """
    for s, pos in vec.items():
        for k in range(1, pos + 1):
            for t, d in req_index[s][k].items():
                if vec[t] < d:
                    return False
    return True


def enumerate_greatest(metas_or_config, sources, H, L):
    """纯枚举实现。返回 (greatest 或 None, 合法向量列表, 候选总数)。"""
    req_index = {}
    for s in sources:
        hist = metas_or_config[s]
        req_index[s] = {c.seq: dict(c.requires) for c in hist.commits}

    axes = [range(L[s], H[s] + 1) for s in sources]
    feasible = []
    total = 1
    for s in sources:
        total *= (H[s] - L[s] + 1)
    for combo in itertools.product(*axes):
        vec = dict(zip(sources, combo))
        if _feasible(vec, req_index):
            feasible.append(vec)

    greatest = None
    for vec in feasible:                       # 最大元：逐分量不小于任何合法向量
        if all(all(vec[t] >= g[t] for t in sources) for g in feasible):
            greatest = vec
            break
    return greatest, feasible, total


def load_from_config(path):
    cfg = json.load(open(path, encoding="utf-8"))
    sources = sorted(cfg["sources"])
    hists, H, L = {}, {}, {}
    for s in sources:
        entry = cfg["sources"][s]
        if "history" not in entry:
            raise SystemExit(
                f"enumcheck 需要含历史的配置（gen-scenarios 产出），源 {s} 只有 URL；"
                f"对运行中的源请改用 --metadata-dir")
        h = history_from_config(entry["history"])
        hists[s] = h
        H[s], L[s] = h.head, h.earliest
    return hists, sources, H, L


def load_from_metadata_dir(d):
    metas = {}
    for fn in os.listdir(d):
        if fn.endswith(".json"):
            metas[fn[:-5]] = json.load(open(os.path.join(d, fn), encoding="utf-8"))
    sources = sorted(metas)
    for m in metas.values():
        validate_metadata(m, set(sources))
    hists = {s: history_from_metadata(metas[s]) for s in sources}
    H = {s: hists[s].head for s in sources}
    L = {s: hists[s].earliest for s in sources}
    return hists, sources, H, L


def main(argv=None):
    p = argparse.ArgumentParser(prog="fedq.enumcheck",
                                description="独立穷举核对最大闭合向量")
    p.add_argument("--config")
    p.add_argument("--metadata-dir")
    args = p.parse_args(argv)
    if args.config:
        hists, sources, H, L = load_from_config(args.config)
    elif args.metadata_dir:
        hists, sources, H, L = load_from_metadata_dir(args.metadata_dir)
    else:
        p.error("需要 --config 或 --metadata-dir")

    greatest, feasible, total = enumerate_greatest(hists, sources, H, L)
    print(f"源顺序: {sources}")
    print(f"观察上界 H = {[H[s] for s in sources]}")
    print(f"读取下界 L = {[L[s] for s in sources]}")
    print(f"候选向量总数: {total}，其中合法: {len(feasible)}")
    if greatest is None:
        print("结论: 盒内无合法向量（应当拒绝打开）")
        return 1
    print("全部合法向量:")
    for v in sorted((tuple(v[s] for s in sources) for v in feasible)):
        print("  ", v)
    print(f"枚举得到的分量最大向量 F = {[greatest[s] for s in sources]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
