"""Merkle 纯函数性质测试：根、纳入证明、追加证明、篡改拒绝。"""
from __future__ import annotations

import hashlib
import itertools
import os

from app import merkle


def _leaves(n: int) -> list[bytes]:
    return [
        merkle.leaf_hash(f"S{i:03d}", "2026-11-16", f"ORD-{i}", os.urandom(16).hex(), 10 + i)
        for i in range(n)
    ]


def test_empty_root_is_stable():
    assert merkle.empty_root(3) == merkle.root_from_leaves([], 3)
    # 与深度 7（默认 128 槽）的空根不同，说明深度参与计算。
    assert merkle.empty_root(3) != merkle.empty_root(7)


def test_prefix_roots_match_root_from_leaves_for_every_prefix():
    """prefix_roots[k] 必须等于 leaves[:k] 独立算根（启动自检逐点复算的依据）。"""
    for depth in (1, 3, 5):
        capacity = 1 << depth
        leaves = _leaves(capacity)  # 覆盖满槽极端情形
        roots = merkle.prefix_roots(leaves, depth)
        assert len(roots) == capacity + 1
        for k in range(capacity + 1):
            assert roots[k] == merkle.root_from_leaves(leaves[:k], depth)
    # 空叶子集合：只有空前缀一个根。
    assert merkle.prefix_roots([], 4) == [merkle.empty_root(4)]


def test_root_append_only_monotonic_and_inclusion():
    depth = 3
    leaves: list[bytes] = []
    roots = [merkle.root_from_leaves(leaves, depth)]
    for n in range(1, 9):
        leaves = _leaves(n)
        roots.append(merkle.root_from_leaves(leaves, depth))
        # 每个旧叶子在新根下都有有效纳入证明。
        for index in range(n):
            path = merkle.inclusion_path(index, leaves, depth)
            assert len(path) == depth  # 路径长度恒为深度 O(log n)
            assert merkle.verify_inclusion(
                index, leaves[index], [s.to_dict() for s in path], roots[n], depth
            )
        # 根随追加而变化。
        assert roots[n] != roots[n - 1]


def test_inclusion_rejects_wrong_index_and_tampering():
    depth = 3
    leaves = _leaves(5)
    root = merkle.root_from_leaves(leaves, depth)
    path = [s.to_dict() for s in merkle.inclusion_path(2, leaves, depth)]
    assert not merkle.verify_inclusion(3, leaves[2], path, root, depth)
    bad = hashlib.sha256(b"tampered").digest()
    assert not merkle.verify_inclusion(2, bad, path, root, depth)
    tampered = [dict(path[0]), *path[1:]]
    tampered[0]["hash"] = hashlib.sha256(b"x").hexdigest()
    assert not merkle.verify_inclusion(2, leaves[2], tampered, root, depth)
    # 路径长度必须等于深度。
    assert not merkle.verify_inclusion(2, leaves[2], path[:-1], root, depth)


def test_append_proof_all_prefixes_and_sizes():
    depth = 4
    leaves = _leaves(13)
    roots = [merkle.root_from_leaves(leaves[:n], depth) for n in range(14)]
    for old in range(14):
        proof = merkle.append_proof(leaves, old, depth)
        # 条目数受 2*depth 上界约束（O(log n)）。
        assert len(proof) <= 2 * depth
        assert merkle.verify_append(
            roots[old], roots[13], old, 13, proof, depth
        ), f"old={old} 的追加证明验证失败"
        # 篡改旧根后必须拒绝（旧检查点留存件被改动的情形）。
        bogus_old = hashlib.sha256(b"bogus-old-root").digest()
        assert not merkle.verify_append(
            bogus_old, roots[13], old, 13, proof, depth
        )


def test_append_proof_detects_replaced_history():
    depth = 3
    leaves_a = _leaves(5)
    leaves_b = list(leaves_a)
    # 替换第 2 条（模拟历史被删改），条数保持不变。
    leaves_b[2] = hashlib.sha256(b"replaced").digest()
    root_old = merkle.root_from_leaves(leaves_a[:5], depth)
    root_new_b = merkle.root_from_leaves(leaves_b, depth)
    proof_b = merkle.append_proof(leaves_b, 5, depth)
    assert not merkle.verify_append(
        root_old, root_new_b, 5, 5, proof_b, depth
    ) or root_old == root_new_b
    # 根必然不同，因此上式必然拒绝；显式断言根不同。
    assert root_old != root_new_b


def test_append_proof_zero_old_and_equal_counts():
    depth = 3
    leaves = _leaves(6)
    root0 = merkle.empty_root(depth)
    root6 = merkle.root_from_leaves(leaves, depth)
    proof = merkle.append_proof(leaves, 0, depth)
    assert merkle.verify_append(root0, root6, 0, 6, proof, depth)
    same_root = root6
    assert merkle.verify_append(same_root, root6, 6, 6, [], depth)


def test_append_proof_rejects_overlap_and_gap():
    depth = 3
    leaves = _leaves(4)
    root = merkle.root_from_leaves(leaves, depth)
    good = merkle.append_proof(leaves, 2, depth)
    # 伪造重复起点。
    dup = [good[0], good[0]]
    assert not merkle.verify_append(
        merkle.root_from_leaves(leaves[:2], depth), root, 2, 4, dup, depth
    )
    # 伪造未对齐起点。
    bad = [dict(e, start=e["start"] + 1) for e in good]
    assert not merkle.verify_append(
        merkle.root_from_leaves(leaves[:2], depth), root, 2, 4, bad, depth
    )


def test_tiling_covers_every_interval():
    for depth in (1, 3, 7):
        cap = 1 << depth
        for lo, hi in itertools.combinations(range(cap + 1), 2):
            blocks = merkle._tile(lo, hi, depth)
            covered = []
            for start, height in blocks:
                size = 1 << height
                assert start % size == 0
                covered.extend(range(start, start + size))
            assert covered == list(range(lo, hi))
