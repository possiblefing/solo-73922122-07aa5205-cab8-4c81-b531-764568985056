"""河流水质站日 CSV 发布 API。

流程与不变量
------------
1. 上传先落共享卷（临时文件 fsync + 原子改名），写盘失败返回 507 且无库记录；
2. 数据库事务 BEGIN IMMEDIATE 串行化双实例并发：
   - 同 (站号, 日期, 单号) 同字节 -> 复用既有记录（幂等重试）；
   - 同键异字节 -> 409 拒绝，记录不可改；
   - 超累计上限 -> 409 拒绝；
3. 每次发布追加一个检查点（条数 + Merkle 根），只增不改；
4. 下载与证明只服务已提交记录；启动自检发现缺件则健康检查置为不可用；
5. 证明与自检所需的多表状态在同一读事务快照内读取（WAL 快照隔离），
   另一实例在读取叶子与检查点之间追加时，条数、根与证明仍对应同一
   完整已提交前缀，且读快照绝不阻塞对方发布。
"""
from __future__ import annotations

import re
from contextlib import asynccontextmanager
from datetime import date

from fastapi import FastAPI, File, Form, HTTPException, Response, UploadFile
from fastapi.responses import JSONResponse
from starlette.concurrency import run_in_threadpool

from . import merkle
from .config import settings
from .db import ConflictError, Database
from .schemas import (
    AppendProofOut,
    CheckpointOut,
    HealthOut,
    InclusionProofOut,
    LeafRecordOut,
    PublishResult,
    ReleaseOut,
)
from .storage import BlobError, BlobStore

blob_store = BlobStore(settings)
database = Database(settings)

# 启动自检结果；发现缺件时拒绝出具任何证明与下载。
_startup_problems: list[str] = []


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _startup_problems
    _startup_problems = database.integrity_problems(blob_store.verify_present)
    yield


app = FastAPI(
    title="河流水质站日 CSV 发布服务",
    version="1.0.0",
    description=(
        "不可改的原件发布、SHA-256 Merkle 检查点、单件纳入证明与检查点追加证明。"
        "原件字节存共享卷，数据库只存目录与摘要。"
    ),
    lifespan=lifespan,
)

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_ID_RE = re.compile(r"^[A-Za-z0-9_.\-]{1,64}$")
_ORDER_RE = re.compile(r"^[A-Za-z0-9_.\-]{1,128}$")


def _validate_identity(station_id: str, sample_date: str, order_no: str) -> None:
    if not _ID_RE.match(station_id or ""):
        raise HTTPException(422, "station_id 须为 1-64 位字母数字及 ._- 字符")
    if not _DATE_RE.match(sample_date or ""):
        raise HTTPException(422, "sample_date 须为 YYYY-MM-DD 格式")
    try:
        date.fromisoformat(sample_date)
    except ValueError:
        raise HTTPException(422, "sample_date 不是合法日历日期")
    if not _ORDER_RE.match(order_no or ""):
        raise HTTPException(422, "order_no 须为 1-128 位字母数字及 ._- 字符")


def _require_healthy() -> None:
    if _startup_problems:
        raise HTTPException(
            503,
            detail={
                "message": "存储自检未通过，存在缺件记录，服务拒绝出具证明",
                "problems": _startup_problems,
            },
        )


# --------------------------------------------------------------------------- #
# 发布
# --------------------------------------------------------------------------- #
@app.post(
    "/releases",
    response_model=PublishResult,
    status_code=201,
    summary="发布站日 CSV（同单同输入重试复用，异输入拒绝）",
)
async def publish_release(
    station_id: str = Form(..., description="站号"),
    sample_date: str = Form(..., description="采样日期 YYYY-MM-DD"),
    order_no: str = Form(..., description="发布单号"),
    file: UploadFile = File(..., description="原件，单文件不超过 1 MiB"),
):
    _require_healthy()
    _validate_identity(station_id, sample_date, order_no)

    # 先在请求内读取并强制大小上限，避免超限文件落盘。
    data = await file.read(settings.max_file_bytes + 1)
    if len(data) > settings.max_file_bytes:
        raise HTTPException(
            413, f"原件超过单件上限 {settings.max_file_bytes} 字节（1 MiB）"
        )
    if len(data) == 0:
        raise HTTPException(422, "原件为空文件，拒绝发布")

    try:
        digest, _path = await run_in_threadpool(blob_store.store, data)
    except BlobError as exc:
        # 写盘失败：无任何库记录，客户端可原样重试。
        return JSONResponse(status_code=507, content={"detail": str(exc)})

    try:
        record, reused, checkpoint = await run_in_threadpool(
            database.publish,
            station_id,
            sample_date,
            order_no,
            digest,
            len(data),
        )
    except ConflictError as exc:
        raise HTTPException(409, str(exc))

    return PublishResult(
        reused=reused,
        release=ReleaseOut(**record),
        checkpoint=CheckpointOut(**checkpoint),
    )


# --------------------------------------------------------------------------- #
# 查询与取件
# --------------------------------------------------------------------------- #
@app.get("/releases", response_model=list[ReleaseOut], summary="列出全部发布记录")
async def list_releases():
    rows = await run_in_threadpool(database.list_releases)
    return [ReleaseOut(**r) for r in rows]


@app.get(
    "/releases/{station_id}/{sample_date}/{order_no}",
    response_model=ReleaseOut,
    summary="按站号/日期/单号查询发布目录记录",
)
async def get_release(station_id: str, sample_date: str, order_no: str):
    record = await run_in_threadpool(
        database.get_release, station_id, sample_date, order_no
    )
    if record is None:
        raise HTTPException(404, "未找到该站号/日期/发布单号对应的发布记录")
    return ReleaseOut(**record)


@app.get(
    "/releases/{station_id}/{sample_date}/{order_no}/download",
    summary="下载已提交原件（仅对应完整已提交字节）",
)
async def download_release(station_id: str, sample_date: str, order_no: str):
    _require_healthy()
    record = await run_in_threadpool(
        database.get_release, station_id, sample_date, order_no
    )
    if record is None:
        raise HTTPException(404, "未找到该站号/日期/发布单号对应的发布记录")
    present = await run_in_threadpool(
        blob_store.verify_present, record["blob_sha256"]
    )
    if not present:
        # 理论上启动自检已拦截；此处兜底，绝不返回缺件/错件。
        raise HTTPException(503, "原件缺失或摘要不符，拒绝下载")
    data = await run_in_threadpool(blob_store.read, record["blob_sha256"])
    filename = f"{station_id}_{sample_date}_{order_no}.csv"
    return Response(
        content=data,
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# --------------------------------------------------------------------------- #
# 检查点
# --------------------------------------------------------------------------- #
@app.get(
    "/checkpoints/latest",
    response_model=CheckpointOut,
    summary="最新检查点（条数 + Merkle 根摘要）",
)
async def latest_checkpoint():
    return CheckpointOut(**(await run_in_threadpool(database.latest_checkpoint)))


@app.get(
    "/checkpoints/{seq}",
    response_model=CheckpointOut,
    summary="按条数序号取任意旧检查点",
)
async def get_checkpoint(seq: int):
    checkpoint = await run_in_threadpool(database.get_checkpoint, seq)
    if checkpoint is None:
        raise HTTPException(404, f"不存在序号为 {seq} 的检查点")
    return CheckpointOut(**checkpoint)


@app.get(
    "/checkpoints",
    response_model=list[CheckpointOut],
    summary="列出全部检查点（含零号空树检查点）",
)
async def list_checkpoints():
    rows = await run_in_threadpool(database.list_checkpoints)
    return [CheckpointOut(**c) for c in rows]


# --------------------------------------------------------------------------- #
# 证明
# --------------------------------------------------------------------------- #
@app.get(
    "/releases/{station_id}/{sample_date}/{order_no}/inclusion-proof",
    response_model=InclusionProofOut,
    summary="单件纳入证明（路径长度 = 树深度，O(log n)）",
)
async def inclusion_proof(station_id: str, sample_date: str, order_no: str):
    _require_healthy()
    # 记录、叶子集合与检查点取自同一读事务快照：另一实例在读取期间
    # 追加时，证明与其检查点仍对应同一完整已提交前缀，离线核验通过。
    snapshot = await run_in_threadpool(
        database.inclusion_snapshot, station_id, sample_date, order_no
    )
    if snapshot is None:
        raise HTTPException(404, "未找到该站号/日期/发布单号对应的发布记录")
    record, leaves, checkpoint = snapshot
    index = record["id"] - 1
    if checkpoint["seq"] != len(leaves) or index >= len(leaves):
        # 快照不变量被破坏只可能是真实库存损坏（启动自检会先置 degraded）。
        raise HTTPException(500, "检查点与叶子条数不一致")
    path = merkle.inclusion_path(index, leaves, settings.tree_depth)
    leaf_record = LeafRecordOut(
        v=merkle.LEAF_VERSION,
        station_id=record["station_id"],
        sample_date=record["sample_date"],
        order_no=record["order_no"],
        blob_sha256=record["blob_sha256"],
        size=record["size"],
    )
    return InclusionProofOut(
        index=index,
        checkpoint_seq=checkpoint["seq"],
        checkpoint_root=checkpoint["root_hex"],
        tree_depth=settings.tree_depth,
        leaf_record=leaf_record,
        leaf_hash=leaves[index].hex(),
        path=[step.to_dict() for step in path],
    )


@app.get(
    "/checkpoints/{old_seq}/append-proof",
    response_model=AppendProofOut,
    summary="旧检查点到新检查点的追加证明（条目数 O(log n)）",
)
async def append_proof(old_seq: int):
    _require_healthy()
    # 旧/新检查点与叶子集合取自同一读事务快照：并发追加不会再造成
    # “检查点条数与叶子条数错位”，追加证明稳定可出、可离线核验。
    snapshot = await run_in_threadpool(database.append_proof_snapshot, old_seq)
    if snapshot is None:
        raise HTTPException(404, f"不存在序号为 {old_seq} 的旧检查点")
    old_cp, new_cp, leaves = snapshot
    if new_cp["seq"] != len(leaves):
        # 快照不变量被破坏只可能是真实库存损坏（启动自检会先置 degraded）。
        raise HTTPException(500, "检查点与叶子条数不一致")
    entries = await run_in_threadpool(
        merkle.append_proof, leaves, old_cp["seq"], settings.tree_depth
    )
    return AppendProofOut(
        old_seq=old_cp["seq"],
        new_seq=new_cp["seq"],
        old_root=old_cp["root_hex"],
        new_root=new_cp["root_hex"],
        tree_depth=settings.tree_depth,
        entries=entries,
    )


# --------------------------------------------------------------------------- #
# 健康检查
# --------------------------------------------------------------------------- #
@app.get("/health", response_model=HealthOut, summary="存储自检与健康状态")
async def health():
    problems = list(_startup_problems)
    rows = await run_in_threadpool(database.list_releases)
    return HealthOut(
        status="ok" if not problems else "degraded",
        release_count=len(rows),
        problems=problems,
    )


@app.get("/", include_in_schema=False)
def root():
    return {
        "service": "water-quality-release-service",
        "docs": "/docs",
        "limits": {
            "max_file_bytes": settings.max_file_bytes,
            "max_releases": settings.max_releases,
            "tree_depth": settings.tree_depth,
        },
    }
