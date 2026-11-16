"""故障路径测试：写盘失败不留记录、重启自检、缺件时拒绝证明与下载。"""
from __future__ import annotations

import io
import os

import pytest

from app import main as main_mod
from app.storage import BlobError


def _publish(client, station, day, order, data):
    return client.post(
        "/releases",
        data={"station_id": station, "sample_date": day, "order_no": order},
        files={"file": ("day.csv", io.BytesIO(data), "text/csv")},
    )


def test_disk_write_failure_leaves_no_record(app_client, monkeypatch):
    client, app, settings = app_client
    before = len(client.get("/releases").json())

    def boom_store(self, data):
        # 在写临时文件之前模拟磁盘不可写，等价于 ENOSPC。
        raise BlobError("模拟磁盘不可写")

    monkeypatch.setattr(main_mod.BlobStore, "store", boom_store)
    r = _publish(client, "FAIL", "2026-11-16", "ORD-F", b"x,y\n1,2\n")
    assert r.status_code == 507
    # 数据库无记录，容量未被占用，失败后可原样重试。
    assert len(client.get("/releases").json()) == before

    monkeypatch.undo()
    r2 = _publish(client, "FAIL", "2026-11-16", "ORD-F", b"x,y\n1,2\n")
    assert r2.status_code == 201 and r2.json()["reused"] is False


def test_blob_store_raises_blob_error(app_client, monkeypatch):
    client, app, settings = app_client

    def boom_write(self, data):
        raise BlobError("ENOSPC: 模拟共享卷写满")

    monkeypatch.setattr("app.main.BlobStore.store", boom_write)
    r = _publish(client, "FAIL2", "2026-11-16", "ORD-F2", b"abc")
    assert r.status_code == 507
    assert len(client.get("/releases").json()) == client.get("/health").json()[
        "release_count"
    ]


def test_restart_integrity_check_detects_missing_blob(app_client, monkeypatch):
    client, app, settings = app_client
    data = b"station,ph\nS9,7.0\n"
    r = _publish(client, "S9", "2026-11-16", "ORD-9", data)
    assert r.status_code == 201
    digest = r.json()["release"]["blob_sha256"]

    # 模拟共享卷上原件丢失（异常掉盘/误删）。
    blob_path = main_mod.blob_store.path_for(digest)
    blob_path.unlink()
    assert not main_mod.blob_store.exists(digest)

    problems = main_mod.database.integrity_problems(main_mod.blob_store.verify_present)
    assert any("原件缺失" in p for p in problems)

    # 自检不通过时，证明与下载一律拒绝，绝不输出缺件记录。
    monkeypatch.setattr(main_mod, "_startup_problems", problems)
    assert client.get("/health").json()["status"] == "degraded"
    assert client.get(
        "/releases/S9/2026-11-16/ORD-9/inclusion-proof"
    ).status_code == 503
    assert client.get(
        "/releases/S9/2026-11-16/ORD-9/download"
    ).status_code == 503
    # 发布也被拒绝，避免在缺件状态上继续追加。
    assert _publish(client, "S10", "2026-11-16", "ORD-10", b"z").status_code == 503

    # 恢复原件（等价于从备份卷恢复字节）后自检恢复通过。
    blob_path.parent.mkdir(parents=True, exist_ok=True)
    blob_path.write_bytes(data)
    assert main_mod.database.integrity_problems(
        main_mod.blob_store.verify_present
    ) == []
    monkeypatch.undo()
    assert client.get("/health").json()["status"] == "ok"


def test_orphan_files_are_harmless_and_tamper_is_detected(app_client):
    """崩溃残留：临时文件/未引用原件不影响服务；已引用原件被改则自检失败。"""
    client, app, settings = app_client
    data = b"station,ph\nS8,6.9\n"
    r = _publish(client, "S8", "2026-11-16", "ORD-8", data)
    assert r.status_code == 201
    digest = r.json()["release"]["blob_sha256"]

    # 模拟写盘进程在 os.replace 前崩溃：临时目录残留半截文件。
    orphan_tmp = settings.temp_dir / ".upload-deadbeef-0000"
    orphan_tmp.write_bytes(b"partial")
    # 模拟一个从未被数据库引用的孤立原件。
    orphan_blob = settings.blob_dir / "ab" / "cd" / ("ab" * 32)
    orphan_blob.parent.mkdir(parents=True, exist_ok=True)
    orphan_blob.write_bytes(b"unreferenced")
    assert main_mod.database.integrity_problems(main_mod.blob_store.verify_present) == []

    # 篡改已引用原件的字节（模拟卷上文件被替换）。
    target = main_mod.blob_store.path_for(digest)
    target.write_bytes(data + b"tampered")
    problems = main_mod.database.integrity_problems(main_mod.blob_store.verify_present)
    assert any("摘要不符" in p for p in problems)
    target.write_bytes(data)
    assert main_mod.database.integrity_problems(main_mod.blob_store.verify_present) == []


def test_existing_checkpoints_remain_valid_after_restart(app_client):
    """重启后重算根与库内每个检查点比对，历史证明持续有效。"""
    client, app, settings = app_client
    leaves = main_mod.database.leaf_hashes()
    from app import merkle

    root = merkle.root_from_leaves(leaves, settings.tree_depth)
    latest = main_mod.database.latest_checkpoint()
    assert latest["root_hex"] == root.hex()
    # 全部历史检查点均可被当前叶子集合复现（固定深度树，旧根不变）。
    for cp in main_mod.database.list_checkpoints():
        rebuilt = merkle.root_from_leaves(leaves[: cp["seq"]], settings.tree_depth)
        assert rebuilt.hex() == cp["root_hex"]
