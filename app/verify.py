"""离线验证：仅凭留存的检查点、证明与待验原件即可核验。

科研组可在无网络、无服务端的环境下使用：

    python -m app.verify inclusion --proof proof.json --file 原件.csv
    python -m app.verify append --proof append.json \\
        --old-checkpoint old.json --new-checkpoint new.json

也可直接导入 :func:`verify_release_bytes` 等纯函数嵌入自己的流程。
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any

from . import merkle


def _load_json(path: str | Path) -> Any:
    with open(path, "rb") as handle:
        return json.loads(handle.read().decode("utf-8"))


def verify_release_bytes(proof: dict, raw_bytes: bytes) -> tuple[bool, str]:
    """核验单件原件的站日归属与检查点纳入情况。

    ``proof`` 为 GET /releases/.../inclusion-proof 的返回内容。
    返回 (是否通过, 说明)。
    """
    try:
        record = proof["leaf_record"]
        index = int(proof["index"])
        depth = int(proof["tree_depth"])
        root = bytes.fromhex(proof["checkpoint_root"])
    except (KeyError, TypeError, ValueError):
        return False, "证明结构不完整"

    actual_digest = hashlib.sha256(raw_bytes).hexdigest()
    if actual_digest != record["blob_sha256"]:
        return False, "原件 SHA-256 与证明记录不一致，原件可能被篡改或替换"
    if len(raw_bytes) != record["size"]:
        return False, "原件字节数与证明记录不一致"

    leaf = merkle.leaf_hash(
        record["station_id"],
        record["sample_date"],
        record["order_no"],
        record["blob_sha256"],
        record["size"],
    )
    if leaf.hex() != proof["leaf_hash"]:
        return False, "叶子哈希与规范化记录不一致（证明本身被篡改）"

    if not merkle.verify_inclusion(index, leaf, proof["path"], root, depth):
        return False, "纳入路径无法重建检查点根，证明或检查点不匹配"

    if proof["checkpoint_seq"] <= index:
        return False, "检查点条数与叶子下标矛盾"
    return True, (
        f"通过：{record['station_id']} / {record['sample_date']} / "
        f"{record['order_no']} 的原件已纳入 {proof['checkpoint_seq']} 号检查点"
    )


def verify_append_proof(
    old_checkpoint: dict, new_checkpoint: dict, proof: dict
) -> tuple[bool, str]:
    """核验旧检查点到新检查点为仅追加前缀关系（历史未被删除或替换）。"""
    try:
        depth = int(proof["tree_depth"])
        ok = merkle.verify_append(
            old_root=bytes.fromhex(old_checkpoint["root_hex"]),
            new_root=bytes.fromhex(new_checkpoint["root_hex"]),
            old_count=int(old_checkpoint["seq"]),
            new_count=int(new_checkpoint["seq"]),
            entries=proof["entries"],
            depth=depth,
        )
    except (KeyError, TypeError, ValueError):
        return False, "追加证明或检查点结构不完整"
    if not ok:
        return False, "追加证明不成立：新检查点不包含完整旧前缀，历史可能被改动"
    return True, (
        f"通过：{old_checkpoint['seq']} 号检查点是 {new_checkpoint['seq']} 号"
        "检查点的完整仅追加前缀"
    )


def _main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="河流水质发布数据离线验证工具")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_inc = sub.add_parser("inclusion", help="核验单件原件纳入证明")
    p_inc.add_argument("--proof", required=True)
    p_inc.add_argument("--file", required=True, help="待验原件字节路径")

    p_app = sub.add_parser("append", help="核验检查点追加证明")
    p_app.add_argument("--proof", required=True)
    p_app.add_argument("--old-checkpoint", required=True)
    p_app.add_argument("--new-checkpoint", required=True)

    args = parser.parse_args(argv)
    if args.cmd == "inclusion":
        proof = _load_json(args.proof)
        raw = Path(args.file).read_bytes()
        ok, message = verify_release_bytes(proof, raw)
    else:
        ok, message = verify_append_proof(
            _load_json(args.old_checkpoint),
            _load_json(args.new_checkpoint),
            _load_json(args.proof),
        )
    print(message)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(_main())
