"""并发交错回归测试。

场景：另一实例在“读取叶子”与“读取检查点”之间成功追加时——
* 纳入证明仍须 200 且离线可核验（不得 200 却验不过）；
* 追加证明仍须 200 且离线可核验（不得 500）；
* 启动自检不得把正常追加误判为损坏；
* 自检/取证停顿不得阻塞另一实例发布；
* 真实篡改（历史检查点被改）仍须被自检检出。

注入方式：在指定钩子首次被调用时，用**另一条数据库连接**（等价于另一
实例）完成一次真实发布（原件落盘 + 同事务提交记录与检查点）。
"""
from __future__ import annotations

import io
import threading
import time

from app import main as main_mod
from app import merkle
from app.db import Database
from app.verify import verify_append_proof, verify_release_bytes

DAY = "2026-11-16"


def _publish(client, station, order, data):
    return client.post(
        "/releases",
        data={"station_id": station, "sample_date": DAY, "order_no": order},
        files={"file": ("day.csv", io.BytesIO(data), "text/csv")},
    )


def _publish_via_other_instance(station: str, order: str, data: bytes):
    """模拟另一实例的完整发布路径：先落盘，再同事务提交记录与检查点。"""
    digest, _ = main_mod.blob_store.store(data)
    return main_mod.database.publish(station, DAY, order, digest, len(data))


def _arm_append_on_leaf_read(monkeypatch, station, order, data):
    """在 ``Database._leaf_hashes_locked`` 首次返回前注入一次并发追加。

    该钩子位于读快照事务内部、叶子读取之后，等价于“另一实例在读取
    叶子与读取检查点之间成功追加”的最强交错。
    """
    original = Database._leaf_hashes_locked
    state = {"done": False}

    def wrapper(conn):
        result = original(conn)
        if not state["done"]:
            state["done"] = True
            _publish_via_other_instance(station, order, data)
        return result

    monkeypatch.setattr(Database, "_leaf_hashes_locked", staticmethod(wrapper))
    return state


def _any_release(client) -> dict:
    rows = client.get("/releases").json()
    if rows:
        return rows[0]
    _publish(client, "SEED", "ORD-SEED", b"seed\n")
    return client.get("/releases").json()[0]


# --------------------------------------------------------------------------- #
# 纳入证明
# --------------------------------------------------------------------------- #
def test_inclusion_snapshot_isolated_from_concurrent_append(app_client, monkeypatch):
    """读快照事务进行中另一实例追加：快照内条数/根/叶子仍属同一前缀。"""
    client, _, settings = app_client
    record = _any_release(client)
    before = main_mod.database.leaf_hashes()

    state = _arm_append_on_leaf_read(
        monkeypatch, "RACE-INC", "ORD-RACE-INC", b"race-inc\n"
    )
    snapshot = main_mod.database.inclusion_snapshot(
        record["station_id"], record["sample_date"], record["order_no"]
    )
    assert state["done"], "并发追加必须真的落在快照读取期间"
    _, leaves, checkpoint = snapshot
    # 快照不含新追加（WAL 快照隔离），且内部自洽。
    assert len(leaves) == len(before)
    assert checkpoint["seq"] == len(leaves)
    assert (
        checkpoint["root_hex"]
        == merkle.root_from_leaves(leaves, settings.tree_depth).hex()
    )
    # 快照之外，追加已提交可见。
    assert len(main_mod.database.leaf_hashes()) == len(before) + 1


def test_inclusion_proof_valid_when_append_lands_during_assembly(
    app_client, monkeypatch
):
    """HTTP 层：快照取完后、证明拼装中另一实例追加，响应仍 200 且离线可验。"""
    client, _, _ = app_client
    data = b"station,ph\nRACE-HTTP,7.0\n"
    r = _publish(client, "RACE-HTTP", "ORD-RACE-HTTP", data)
    assert r.status_code == 201

    real_path = merkle.inclusion_path
    state = {"done": False}

    def path_after_concurrent_append(index, leaves, depth):
        if not state["done"]:
            state["done"] = True
            _publish_via_other_instance(
                "RACE-HTTP2", "ORD-RACE-HTTP2", b"race-http2\n"
            )
        return real_path(index, leaves, depth)

    monkeypatch.setattr(main_mod.merkle, "inclusion_path", path_after_concurrent_append)
    r = client.get("/releases/RACE-HTTP/2026-11-16/ORD-RACE-HTTP/inclusion-proof")
    assert r.status_code == 200
    assert state["done"], "并发追加必须真的落在证明拼装期间"
    ok, msg = verify_release_bytes(r.json(), data)
    assert ok, msg


# --------------------------------------------------------------------------- #
# 追加证明
# --------------------------------------------------------------------------- #
def test_append_proof_snapshot_isolated_from_concurrent_append(
    app_client, monkeypatch
):
    """旧/新检查点与叶子同一快照：追加证明条目与两端检查点严格对应。"""
    client, _, settings = app_client
    _any_release(client)
    before = main_mod.database.leaf_hashes()

    state = _arm_append_on_leaf_read(
        monkeypatch, "RACE-APS", "ORD-RACE-APS", b"race-aps\n"
    )
    old_cp, new_cp, leaves = main_mod.database.append_proof_snapshot(0)
    assert state["done"]
    assert new_cp["seq"] == len(leaves) == len(before)
    assert (
        new_cp["root_hex"]
        == merkle.root_from_leaves(leaves, settings.tree_depth).hex()
    )
    # 用快照数据出具的追加证明可离线核验。
    entries = merkle.append_proof(leaves, old_cp["seq"], settings.tree_depth)
    ok, msg = verify_append_proof(
        old_cp, new_cp, {"tree_depth": settings.tree_depth, "entries": entries}
    )
    assert ok, msg
    assert len(main_mod.database.leaf_hashes()) == len(before) + 1


def test_append_proof_valid_when_append_lands_during_assembly(
    app_client, monkeypatch
):
    """HTTP 层：快照取完后、证明拼装中另一实例追加，不再 500，且离线可验。"""
    client, _, _ = app_client
    r = _publish(client, "RACE-APSEED", "ORD-RACE-APSEED", b"race-apseed\n")
    assert r.status_code == 201
    old_cp = client.get("/checkpoints/1").json()

    real_append = merkle.append_proof
    state = {"done": False}

    def append_after_concurrent_append(leaves, old_count, depth):
        if not state["done"]:
            state["done"] = True
            _publish_via_other_instance("RACE-AP", "ORD-RACE-AP", b"race-ap\n")
        return real_append(leaves, old_count, depth)

    monkeypatch.setattr(
        main_mod.merkle, "append_proof", append_after_concurrent_append
    )
    r = client.get("/checkpoints/1/append-proof")
    assert r.status_code == 200, r.text
    assert state["done"], "并发追加必须真的落在证明拼装期间"
    proof = r.json()
    new_cp = {"seq": proof["new_seq"], "root_hex": proof["new_root"]}
    ok, msg = verify_append_proof(old_cp, new_cp, proof)
    assert ok, msg


# --------------------------------------------------------------------------- #
# 启动自检与并发追加
# --------------------------------------------------------------------------- #
def test_startup_check_tolerates_concurrent_append(app_client, monkeypatch):
    """自检两次读取之间另一实例追加：不得误报损坏。"""
    client, _, _ = app_client
    _any_release(client)
    before = len(main_mod.database.leaf_hashes())

    state = _arm_append_on_leaf_read(
        monkeypatch, "RACE-BOOT", "ORD-RACE-BOOT", b"race-boot\n"
    )
    problems = main_mod.database.integrity_problems(
        main_mod.blob_store.verify_present
    )
    assert state["done"], "并发追加必须真的落在自检读取期间"
    assert problems == []
    assert len(main_mod.database.leaf_hashes()) == before + 1


def test_startup_check_with_slow_blob_verify_still_consistent(app_client, monkeypatch):
    """逐件校验卷上原件期间另一实例追加：自检结论仍一致、不误报。"""
    original_verify = main_mod.blob_store.verify_present
    state = {"done": False}

    def verify_with_concurrent_append(digest):
        if not state["done"]:
            state["done"] = True
            _publish_via_other_instance(
                "RACE-SLOW", "ORD-RACE-SLOW", b"race-slow\n"
            )
        return original_verify(digest)

    problems = main_mod.database.integrity_problems(verify_with_concurrent_append)
    assert state["done"]
    assert problems == []


def test_paused_read_snapshot_does_not_block_publishing(app_client):
    """自检/取证持有的读快照不阻塞另一实例发布（WAL 读者不挡写者）。"""
    client, _, _ = app_client
    with main_mod.database.read_snapshot() as conn:
        conn.execute("SELECT COUNT(*) FROM releases").fetchone()  # 建立读快照
        # 快照持有期间（等价于自检/取证停顿），另一实例发布须立即成功。
        record, reused, checkpoint = _publish_via_other_instance(
            "RACE-PAUSE", "ORD-RACE-PAUSE", b"race-pause\n"
        )
    assert reused is False
    assert checkpoint["seq"] == record["id"]
    rows = client.get("/releases").json()
    assert any(r["order_no"] == "ORD-RACE-PAUSE" for r in rows)


# --------------------------------------------------------------------------- #
# 真实篡改仍须被检出
# --------------------------------------------------------------------------- #
def test_historical_checkpoint_tamper_is_detected(app_client):
    """历史检查点根被改：自检逐点复算必须检出；恢复后自检通过。"""
    client, _, _ = app_client
    _any_release(client)
    with main_mod.database.connect() as conn:
        row = conn.execute(
            "SELECT seq, root_hex FROM checkpoints WHERE seq = 1"
        ).fetchone()
        original_root = row["root_hex"]
        conn.execute(
            "UPDATE checkpoints SET root_hex = ? WHERE seq = 1", ("00" * 32,)
        )
    try:
        problems = main_mod.database.integrity_problems(
            main_mod.blob_store.verify_present
        )
        assert any("seq=1" in p and "不一致" in p for p in problems), problems
    finally:
        with main_mod.database.connect() as conn:
            conn.execute(
                "UPDATE checkpoints SET root_hex = ? WHERE seq = 1",
                (original_root,),
            )
    assert (
        main_mod.database.integrity_problems(main_mod.blob_store.verify_present)
        == []
    )


# --------------------------------------------------------------------------- #
# 发布风暴下的连续取证（真实多线程交错）
# --------------------------------------------------------------------------- #
def test_proof_requests_during_publish_storm(app_client):
    """发布风暴期间连续取两类证明：每个响应都必须 200 且离线可核验。"""
    client, _, _ = app_client
    seed_data = b"station,ph\nSTORM-SEED,7.0\n"
    r = _publish(client, "STORM-SEED", "ORD-STORM-SEED", seed_data)
    assert r.status_code == 201
    old_cp = client.get("/checkpoints/0").json()

    errors: list[Exception] = []

    def publisher():
        # 固定件数、间隔 20ms：风暴窗口约 160ms，与取证循环充分重叠。
        for i in range(8):
            try:
                _publish_via_other_instance(
                    "STORM", f"ORD-STORM-{i:02d}", f"storm-{i}\n".encode()
                )
            except Exception as exc:  # pragma: no cover - 失败即断言
                errors.append(exc)
                return
            time.sleep(0.02)

    thread = threading.Thread(target=publisher)
    thread.start()
    checked = 0
    while thread.is_alive():
        r = client.get(
            "/releases/STORM-SEED/2026-11-16/ORD-STORM-SEED/inclusion-proof"
        )
        assert r.status_code == 200, r.text
        ok, msg = verify_release_bytes(r.json(), seed_data)
        assert ok, msg
        r = client.get("/checkpoints/0/append-proof")
        assert r.status_code == 200, r.text
        proof = r.json()
        new_cp = {"seq": proof["new_seq"], "root_hex": proof["new_root"]}
        ok, msg = verify_append_proof(old_cp, new_cp, proof)
        assert ok, msg
        checked += 1
    thread.join(timeout=10)
    assert not errors
    assert checked >= 3, "风暴窗口内应完成多轮取证"
