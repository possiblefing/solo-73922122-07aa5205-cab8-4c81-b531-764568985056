"""SHA-256 固定深度 Merkle 树：根摘要、纳入证明、追加证明及其离线验证。

设计要点
--------
* 树为固定深度 ``depth`` 的完整二叉树，叶子槽位数 ``capacity = 2**depth``
  （默认 128）。未使用的槽位以公开常量 ``EMPTY_LEAF`` 占位，因此：
    - 每次追加只改变从新叶子到根的路径，旧证明不会因树“长高”而失效；
    - 任意两个检查点之间的追加证明大小仅取决于条数（O(log n) 个哈希）。
* 域分隔：叶子用 ``0x00`` 前缀，内部节点用 ``0x01`` 前缀，避免二阶碰撞。
* 叶子载荷使用带版本号的规范化 JSON（键排序、无空白），离线端可独立复算。
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Iterable, Sequence

LEAF_VERSION = 1
EMPTY_LEAF = hashlib.sha256(b"\x00empty-leaf-v1").digest()


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def leaf_hash(
    station_id: str,
    sample_date: str,
    order_no: str,
    blob_sha256: str,
    size: int,
) -> bytes:
    """计算一条发布记录的叶子哈希（含域分隔前缀）。"""
    payload = canonical_leaf(station_id, sample_date, order_no, blob_sha256, size)
    return hashlib.sha256(b"\x00" + payload).digest()


def canonical_leaf(
    station_id: str,
    sample_date: str,
    order_no: str,
    blob_sha256: str,
    size: int,
) -> bytes:
    """规范化叶子载荷。离线验证端必须使用完全相同的字节序列。"""
    document = {
        "v": LEAF_VERSION,
        "station_id": station_id,
        "sample_date": sample_date,
        "order_no": order_no,
        "blob_sha256": blob_sha256,
        "size": size,
    }
    return json.dumps(
        document, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


def pair_hash(left: bytes, right: bytes) -> bytes:
    """内部节点哈希。"""
    return hashlib.sha256(b"\x01" + left + right).digest()


def root_from_leaves(leaf_hashes: Sequence[bytes], depth: int) -> bytes:
    """由前若干个叶子哈希计算固定深度树的根，其余槽位以 EMPTY_LEAF 补齐。"""
    capacity = 1 << depth
    if len(leaf_hashes) > capacity:
        raise ValueError("叶子数量超过树容量")
    level = list(leaf_hashes) + [EMPTY_LEAF] * (capacity - len(leaf_hashes))
    for _ in range(depth):
        level = [pair_hash(level[i], level[i + 1]) for i in range(0, len(level), 2)]
    return level[0]


def empty_root(depth: int) -> bytes:
    return root_from_leaves([], depth)


# --------------------------------------------------------------------------- #
# 纳入证明（inclusion proof）
# --------------------------------------------------------------------------- #

@dataclass(frozen=True)
class PathStep:
    # sibling 位于当前节点的哪一侧
    side: str  # "L" 或 "R"
    # 当前节点在其父节点中的位置：0=左，1=右。与 index 的二进制位绑定，
    # 防止同一路径被重放到其他下标。
    bit: int
    hash: bytes

    def to_dict(self) -> dict:
        return {"side": self.side, "bit": self.bit, "hash": self.hash.hex()}


def inclusion_path(
    index: int, leaf_hashes: Sequence[bytes], depth: int
) -> list[PathStep]:
    """生成 ``index`` 号叶子的纳入路径，路径长度恰为 ``depth``（O(log n)）。"""
    capacity = 1 << depth
    if not 0 <= index < len(leaf_hashes):
        raise ValueError("纳入证明索引越界")
    level = list(leaf_hashes) + [EMPTY_LEAF] * (capacity - len(leaf_hashes))
    pos = index
    steps: list[PathStep] = []
    for _ in range(depth):
        bit = pos % 2
        if bit == 0:
            steps.append(PathStep(side="R", bit=bit, hash=level[pos + 1]))
        else:
            steps.append(PathStep(side="L", bit=bit, hash=level[pos - 1]))
        pos //= 2
        level = [pair_hash(level[i], level[i + 1]) for i in range(0, len(level), 2)]
    return steps


def verify_inclusion(
    index: int,
    leaf: bytes,
    path: Iterable[dict],
    root: bytes,
    depth: int,
) -> bool:
    """离线核验单件纳入证明。纯函数，不依赖服务端状态。"""
    try:
        if not 0 <= index < (1 << depth):
            return False
        steps = list(path)
        if len(steps) != depth:
            return False
        current = leaf
        pos = index
        for step in steps:
            sibling = bytes.fromhex(step["hash"])
            if len(sibling) != 32:
                return False
            bit = step.get("bit")
            # 证明必须逐位绑定到所声称的下标。
            if bit not in (0, 1) or bit != pos % 2:
                return False
            if step["side"] == "L":
                if bit != 1:
                    return False
                current = pair_hash(sibling, current)
            elif step["side"] == "R":
                if bit != 0:
                    return False
                current = pair_hash(current, sibling)
            else:
                return False
            pos //= 2
        return current == root
    except (KeyError, TypeError, ValueError):
        return False


# --------------------------------------------------------------------------- #
# 追加证明（append / consistency proof）
# --------------------------------------------------------------------------- #

def _tile(lo: int, hi: int, depth: int) -> list[tuple[int, int]]:
    """把区间 [lo, hi) 切分为起点对齐的 2 的幂大小块，返回 (起点, 高度) 列表。

    块数为区间长度二进制中 1 的个数量级，因此为 O(log n)。
    """
    blocks: list[tuple[int, int]] = []
    pos = lo
    while pos < hi:
        if pos == 0:
            align_h = depth
        else:
            align_h = (pos & -pos).bit_length() - 1
        remaining = hi - pos
        remain_h = remaining.bit_length() - 1
        height = min(align_h, remain_h, depth)
        blocks.append((pos, height))
        pos += 1 << height
    return blocks


def _subtree_root(
    leaf_hashes: Sequence[bytes], start: int, height: int, depth: int
) -> bytes:
    """计算以 ``start`` 为起点、覆盖 2**height 个槽位的完美子树根。"""
    size = 1 << height
    if start % size != 0 or start + size > (1 << depth):
        raise ValueError("子树起点未对齐或超出树容量")
    level = [
        leaf_hashes[i] if i < len(leaf_hashes) else EMPTY_LEAF
        for i in range(start, start + size)
    ]
    for _ in range(height):
        level = [pair_hash(level[i], level[i + 1]) for i in range(0, len(level), 2)]
    return level[0]


def append_proof(
    leaf_hashes: Sequence[bytes], old_count: int, depth: int
) -> list[dict]:
    """生成从 ``old_count`` 号旧检查点到当前检查点的追加证明。

    返回的每个条目描述一个完美子树：起点、高度、子树根哈希。
    旧前缀 [0, old_count) 与新增区间 [old_count, new_count) 分别平铺，
    条目总数不超过 ``2 * depth``。
    """
    new_count = len(leaf_hashes)
    if not 0 <= old_count <= new_count:
        raise ValueError("旧条数必须介于 0 与当前条数之间")
    if old_count == new_count:
        # 同一检查点之间的一致性证明为空。
        return []
    entries: list[dict] = []
    for start, height in _tile(0, old_count, depth) + _tile(
        old_count, new_count, depth
    ):
        entries.append(
            {
                "start": start,
                "height": height,
                "hash": _subtree_root(leaf_hashes, start, height, depth).hex(),
            }
        )
    return entries


def _rebuild_root(
    entries: Sequence[dict], covered: int, depth: int
) -> bytes | None:
    """依据子树条目重建固定深度树的根。

    仅承认覆盖 [0, covered) 的条目；其余槽位按 EMPTY_LEAF 处理。
    条目若存在重叠、缺口、错位则返回 None。
    """
    capacity = 1 << depth
    by_start: dict[int, dict] = {}
    for entry in entries:
        try:
            start = int(entry["start"])
            height = int(entry["height"])
            digest = bytes.fromhex(entry["hash"])
        except (KeyError, TypeError, ValueError):
            return None
        if not (0 <= start < capacity and 0 <= height <= depth):
            return None
        if len(digest) != 32 or start in by_start:
            return None
        if start % (1 << height) != 0:
            return None
        by_start[start] = {"height": height, "hash": digest}

    pointer = 0

    def node(lo: int, hi: int, height: int) -> bytes | None:
        nonlocal pointer
        block = by_start.get(lo)
        if block is not None and block["height"] == height and lo < covered:
            if lo != pointer:
                return None
            pointer = lo + (1 << height)
            return block["hash"]
        if height == 0:
            return EMPTY_LEAF
        mid = (lo + hi) // 2
        left = node(lo, mid, height - 1)
        if left is None:
            return None
        right = node(mid, hi, height - 1)
        if right is None:
            return None
        return pair_hash(left, right)

    root = node(0, capacity, depth)
    if root is None or pointer != covered:
        return None
    return root


def verify_append(
    old_root: bytes,
    new_root: bytes,
    old_count: int,
    new_count: int,
    entries: Sequence[dict],
    depth: int,
) -> bool:
    """离线核验追加证明：旧检查点是新检查点的仅追加前缀。

    纯函数，只需两个留存检查点的根、条数与证明本身，不接触服务端。
    """
    capacity = 1 << depth
    try:
        old_count = int(old_count)
        new_count = int(new_count)
    except (TypeError, ValueError):
        return False
    if not (0 <= old_count <= new_count <= capacity):
        return False
    if old_count == new_count:
        # 同一检查点：证明应为空，且两端留存的根必须一致。
        return not entries and old_root == new_root

    rebuilt_new = _rebuild_root(entries, new_count, depth)
    if rebuilt_new is None or rebuilt_new != new_root:
        return False
    rebuilt_old = _rebuild_root(entries, old_count, depth)
    if rebuilt_old is None or rebuilt_old != old_root:
        return False
    return True
