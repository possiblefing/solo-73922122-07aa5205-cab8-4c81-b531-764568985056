"""SQLite 持久层：目录与摘要（不存原件字节）。

并发模型
--------
* WAL 模式 + ``busy_timeout``：两个实例共享同一库文件，读者不阻塞写者；
* 发布事务使用 ``BEGIN IMMEDIATE``，跨实例写操作被数据库锁串行化，
  后到者阻塞等待后重检唯一键，因此不会分叉、不会互相覆盖。
* 无任何 UPDATE/DELETE 路径，发布后记录不可改、不可删。
"""
from __future__ import annotations

import sqlite3
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Sequence

from . import merkle
from .config import Settings

SCHEMA = """
CREATE TABLE IF NOT EXISTS releases (
    id           INTEGER PRIMARY KEY,          -- 1 起稠密序号，叶子下标 = id - 1
    station_id   TEXT NOT NULL,
    sample_date  TEXT NOT NULL,
    order_no     TEXT NOT NULL,
    blob_sha256  TEXT NOT NULL,
    size         INTEGER NOT NULL,
    created_at   TEXT NOT NULL,
    UNIQUE(station_id, sample_date, order_no)
);

CREATE TABLE IF NOT EXISTS checkpoints (
    seq         INTEGER PRIMARY KEY,            -- 该检查点对应的条数
    root_hex    TEXT NOT NULL,
    created_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS meta (
    key    TEXT PRIMARY KEY,
    value  TEXT NOT NULL
);
"""


def _utcnow() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class ConflictError(Exception):
    """业务冲突：异输入重试或超出累计上限。"""


class Database:
    def __init__(self, settings: Settings):
        self._settings = settings
        settings.ensure_dirs()
        self._init_schema()

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(
            self._settings.db_path,
            timeout=15.0,
            isolation_level=None,  # 手动事务，显式 BEGIN/COMMIT
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        conn.execute(f"PRAGMA busy_timeout = 15000")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = FULL")
        conn.execute("PRAGMA foreign_keys = ON")
        try:
            yield conn
        finally:
            conn.close()

    def _init_schema(self) -> None:
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.executescript(SCHEMA)
                # 零号检查点（空树根），幂等创建。
                empty = merkle.empty_root(self._settings.tree_depth).hex()
                conn.execute(
                    "INSERT OR IGNORE INTO checkpoints(seq, root_hex, created_at) "
                    "VALUES (0, ?, ?)",
                    (empty, _utcnow()),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    # ------------------------------------------------------------------ #
    # 发布
    # ------------------------------------------------------------------ #
    def publish(
        self,
        station_id: str,
        sample_date: str,
        order_no: str,
        blob_sha256: str,
        size: int,
    ) -> tuple[dict, bool]:
        """串行化发布。返回 (发布记录, 是否复用既有记录)。

        调用方必须已把原件原子落盘成功后再调用本方法，
        从而保证“库中有记录 => 卷上有完整原件”。
        """
        # 双实例并发时 BEGIN IMMEDIATE 可能撞上写锁；busy_timeout 之外再有限重试。
        for attempt in range(3):
            try:
                return self._publish_once(
                    station_id, sample_date, order_no, blob_sha256, size
                )
            except sqlite3.OperationalError as exc:
                if "locked" in str(exc).lower() and attempt < 2:
                    time.sleep(0.2 * (attempt + 1))
                    continue
                raise
        raise RuntimeError("unreachable")  # pragma: no cover

    def _publish_once(
        self,
        station_id: str,
        sample_date: str,
        order_no: str,
        blob_sha256: str,
        size: int,
    ) -> tuple[dict, bool]:
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                row = conn.execute(
                    "SELECT * FROM releases "
                    "WHERE station_id = ? AND sample_date = ? AND order_no = ?",
                    (station_id, sample_date, order_no),
                ).fetchone()
                if row is not None:
                    record = dict(row)
                    same = (
                        record["blob_sha256"] == blob_sha256
                        and record["size"] == size
                    )
                    conn.rollback()  # 只读复用，不产生写入
                    if not same:
                        raise ConflictError(
                            "同一站号/日期/发布单号已存在且原件内容不同，禁止改写"
                        )
                    return record, True

                count = conn.execute(
                    "SELECT COUNT(*) AS c FROM releases"
                ).fetchone()["c"]
                if count >= self._settings.max_releases:
                    conn.rollback()
                    raise ConflictError(
                        f"累计发布已达上限 {self._settings.max_releases} 件"
                    )

                cursor = conn.execute(
                    "INSERT INTO releases(station_id, sample_date, order_no, "
                    "blob_sha256, size, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        station_id,
                        sample_date,
                        order_no,
                        blob_sha256,
                        size,
                        _utcnow(),
                    ),
                )
                release_id = cursor.lastrowid
                # 无删除路径，rowid 必然稠密且等于新条数。
                assert release_id == count + 1

                leaves = self._leaf_hashes_locked(conn)
                root = merkle.root_from_leaves(leaves, self._settings.tree_depth)
                conn.execute(
                    "INSERT INTO checkpoints(seq, root_hex, created_at) "
                    "VALUES (?, ?, ?)",
                    (len(leaves), root.hex(), _utcnow()),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise

        record = self.get_release_by_id(release_id)
        return record, False

    @staticmethod
    def _leaf_hashes_locked(conn: sqlite3.Connection) -> list[bytes]:
        rows = conn.execute(
            "SELECT station_id, sample_date, order_no, blob_sha256, size "
            "FROM releases ORDER BY id ASC"
        ).fetchall()
        return [
            merkle.leaf_hash(
                r["station_id"],
                r["sample_date"],
                r["order_no"],
                r["blob_sha256"],
                r["size"],
            )
            for r in rows
        ]

    def leaf_hashes(self) -> list[bytes]:
        with self.connect() as conn:
            return self._leaf_hashes_locked(conn)

    # ------------------------------------------------------------------ #
    # 查询
    # ------------------------------------------------------------------ #
    def get_release(self, station_id: str, sample_date: str, order_no: str):
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM releases WHERE station_id = ? AND sample_date = ? "
                "AND order_no = ?",
                (station_id, sample_date, order_no),
            ).fetchone()
            return dict(row) if row else None

    def get_release_by_id(self, release_id: int):
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM releases WHERE id = ?", (release_id,)
            ).fetchone()
            return dict(row) if row else None

    def list_releases(self) -> list[dict]:
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM releases ORDER BY id ASC").fetchall()
            return [dict(r) for r in rows]

    def latest_checkpoint(self) -> dict:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM checkpoints ORDER BY seq DESC LIMIT 1"
            ).fetchone()
            return dict(row)

    def get_checkpoint(self, seq: int):
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM checkpoints WHERE seq = ?", (seq,)
            ).fetchone()
            return dict(row) if row else None

    def list_checkpoints(self) -> list[dict]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM checkpoints ORDER BY seq ASC"
            ).fetchall()
            return [dict(r) for r in rows]

    # ------------------------------------------------------------------ #
    # 启动自检：不得带着缺件记录对外服务
    # ------------------------------------------------------------------ #
    def integrity_problems(self, blob_verifier) -> list[str]:
        problems: list[str] = []
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT id, blob_sha256 FROM releases ORDER BY id"
            ).fetchall()
            expected_ids = list(range(1, len(rows) + 1))
            actual_ids = [r["id"] for r in rows]
            if actual_ids != expected_ids:
                problems.append("releases 主键不稠密，存在缺口或乱序")
            for row in rows:
                if not blob_verifier(row["blob_sha256"]):
                    problems.append(
                        f"release id={row['id']} 的原件缺失或摘要不符 "
                        f"(sha256={row['blob_sha256'][:12]}...)"
                    )
            leaves = self._leaf_hashes_locked(conn)
            root = merkle.root_from_leaves(leaves, self._settings.tree_depth)
            cp_rows = conn.execute(
                "SELECT seq, root_hex FROM checkpoints ORDER BY seq"
            ).fetchall()
            cp_seqs = [r["seq"] for r in cp_rows]
            if cp_seqs != list(range(len(cp_seqs))):
                problems.append("checkpoints 序号不连续")
            latest = cp_rows[-1]
            if latest["seq"] != len(leaves) or latest["root_hex"] != root.hex():
                problems.append("最新检查点与实际叶子集合不一致")
        return problems
