"""SQLite 持久层：目录与摘要（不存原件字节）。

并发模型
--------
* WAL 模式 + ``busy_timeout``：两个实例共享同一库文件，读者不阻塞写者；
* 发布事务使用 ``BEGIN IMMEDIATE``，跨实例写操作被数据库锁串行化，
  后到者阻塞等待后重检唯一键，因此不会分叉、不会互相覆盖。
* 证明与启动自检所需的多表状态（发布记录、叶子集合、检查点）一律在
  **同一个读事务**内读取（``read_snapshot``）：WAL 下读事务看到一致的
  已提交快照，且绝不阻塞另一实例的发布提交；因此另一实例在“读叶子”
  与“读检查点”之间追加时，证明与自检仍然对应同一完整已提交前缀。
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

    @contextmanager
    def read_snapshot(self) -> Iterator[sqlite3.Connection]:
        """一致读快照：同一延迟读事务内的多次查询看到同一已提交状态。

        WAL 模式下读事务不持有写锁、不阻塞其他实例的发布提交；
        事务只读，结束时直接回滚释放。
        """
        with self.connect() as conn:
            conn.execute("BEGIN")
            try:
                yield conn
            finally:
                conn.rollback()

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
    ) -> tuple[dict, bool, dict]:
        """串行化发布。返回 (发布记录, 是否复用既有记录, 本次事务内最新检查点)。

        调用方必须已把原件原子落盘成功后再调用本方法，
        从而保证“库中有记录 => 卷上有完整原件”。
        检查点与发布记录在同一事务内读取/写入，响应的条数与根
        必然对应包含该记录的完整已提交前缀。
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
    ) -> tuple[dict, bool, dict]:
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
                    if not same:
                        conn.rollback()
                        raise ConflictError(
                            "同一站号/日期/发布单号已存在且原件内容不同，禁止改写"
                        )
                    # 只读复用：同事务取最新检查点，必然已覆盖该记录。
                    checkpoint = dict(
                        conn.execute(
                            "SELECT * FROM checkpoints ORDER BY seq DESC LIMIT 1"
                        ).fetchone()
                    )
                    conn.rollback()  # 只读复用，不产生写入
                    return record, True, checkpoint

                count = conn.execute(
                    "SELECT COUNT(*) AS c FROM releases"
                ).fetchone()["c"]
                if count >= self._settings.max_releases:
                    conn.rollback()
                    raise ConflictError(
                        f"累计发布已达上限 {self._settings.max_releases} 件"
                    )

                now = _utcnow()
                cursor = conn.execute(
                    "INSERT INTO releases(station_id, sample_date, order_no, "
                    "blob_sha256, size, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        station_id,
                        sample_date,
                        order_no,
                        blob_sha256,
                        size,
                        now,
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
                    (len(leaves), root.hex(), now),
                )
                record = dict(
                    conn.execute(
                        "SELECT * FROM releases WHERE id = ?", (release_id,)
                    ).fetchone()
                )
                checkpoint = {
                    "seq": len(leaves),
                    "root_hex": root.hex(),
                    "created_at": now,
                }
                conn.commit()
                return record, False, checkpoint
            except Exception:
                conn.rollback()
                raise

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
    # 证明用一致快照：条数、根与叶子集合对应同一完整已提交前缀
    # ------------------------------------------------------------------ #
    def inclusion_snapshot(
        self, station_id: str, sample_date: str, order_no: str
    ) -> tuple[dict, list[bytes], dict] | None:
        """同一读事务内取发布记录、全部叶子哈希与最新检查点。

        另一实例在读取期间追加时，快照仍对应同一已提交前缀，
        据此出具的纳入证明与其检查点必然一致，无需客户端重试。
        记录不存在时返回 None。
        """
        with self.read_snapshot() as conn:
            row = conn.execute(
                "SELECT * FROM releases WHERE station_id = ? AND sample_date = ? "
                "AND order_no = ?",
                (station_id, sample_date, order_no),
            ).fetchone()
            if row is None:
                return None
            leaves = self._leaf_hashes_locked(conn)
            checkpoint = conn.execute(
                "SELECT * FROM checkpoints ORDER BY seq DESC LIMIT 1"
            ).fetchone()
            return dict(row), leaves, dict(checkpoint)

    def append_proof_snapshot(
        self, old_seq: int
    ) -> tuple[dict, dict, list[bytes]] | None:
        """同一读事务内取旧检查点、最新检查点与全部叶子哈希。

        快照内 ``latest.seq == len(leaves)`` 恒成立（发布把记录与检查点
        写在同一事务），并发追加不会再导致条数错位。旧检查点不存在时
        返回 None。
        """
        with self.read_snapshot() as conn:
            old = conn.execute(
                "SELECT * FROM checkpoints WHERE seq = ?", (old_seq,)
            ).fetchone()
            if old is None:
                return None
            new = conn.execute(
                "SELECT * FROM checkpoints ORDER BY seq DESC LIMIT 1"
            ).fetchone()
            leaves = self._leaf_hashes_locked(conn)
            return dict(old), dict(new), leaves

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
        """存储自检。返回问题列表，空列表表示可对外服务。

        库内状态（发布记录、叶子集合、检查点）在同一读事务快照内比对，
        另一实例在自检期间正常追加不会被误判为损坏；快照关闭后再逐件
        校验卷上原件（记录只增不改，快照行内容不会变），自检再慢也不
        阻塞其他实例发布。真实缺件、篡改仍会被检出。
        """
        problems: list[str] = []
        with self.read_snapshot() as conn:
            rows = conn.execute(
                "SELECT id, blob_sha256 FROM releases ORDER BY id"
            ).fetchall()
            leaves = self._leaf_hashes_locked(conn)
            cp_rows = conn.execute(
                "SELECT seq, root_hex FROM checkpoints ORDER BY seq"
            ).fetchall()

        expected_ids = list(range(1, len(rows) + 1))
        actual_ids = [r["id"] for r in rows]
        if actual_ids != expected_ids:
            problems.append("releases 主键不稠密，存在缺口或乱序")

        cp_seqs = [r["seq"] for r in cp_rows]
        if not cp_rows:
            problems.append("缺少检查点记录")
        elif cp_seqs != list(range(len(cp_seqs))):
            problems.append("checkpoints 序号不连续")
        else:
            if cp_seqs[-1] != len(leaves):
                problems.append("最新检查点与实际叶子集合不一致")
            # 逐点复算：每个历史检查点的根都必须等于对应叶子前缀的根。
            # 同一快照内比对，并发追加不会造成误判。
            roots = merkle.prefix_roots(leaves, self._settings.tree_depth)
            for cp in cp_rows:
                if cp["seq"] >= len(roots):
                    # 检查点条数超出发布记录：快照内不一致。
                    problems.append(
                        f"检查点 seq={cp['seq']} 超出已提交发布条数"
                    )
                elif roots[cp["seq"]].hex() != cp["root_hex"]:
                    problems.append(
                        f"检查点 seq={cp['seq']} 的根摘要与对应叶子前缀不一致"
                    )

        # 卷上原件逐件校验（在快照事务外进行，不长时间占用读事务）。
        for row in rows:
            if not blob_verifier(row["blob_sha256"]):
                problems.append(
                    f"release id={row['id']} 的原件缺失或摘要不符 "
                    f"(sha256={row['blob_sha256'][:12]}...)"
                )
        return problems
