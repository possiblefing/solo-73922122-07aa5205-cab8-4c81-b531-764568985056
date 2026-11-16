"""HTTP 响应模型。"""
from __future__ import annotations

from pydantic import BaseModel, Field


class ReleaseOut(BaseModel):
    id: int = Field(..., description="叶子下标 = id - 1")
    station_id: str
    sample_date: str
    order_no: str
    blob_sha256: str
    size: int
    created_at: str


class PublishResult(BaseModel):
    reused: bool = Field(..., description="True 表示同单同输入重试，复用既有记录")
    release: ReleaseOut
    checkpoint: "CheckpointOut"


class CheckpointOut(BaseModel):
    seq: int = Field(..., description="截至该检查点的发布条数")
    root_hex: str
    created_at: str


class LeafRecordOut(BaseModel):
    v: int
    station_id: str
    sample_date: str
    order_no: str
    blob_sha256: str
    size: int


class InclusionProofOut(BaseModel):
    index: int
    checkpoint_seq: int
    checkpoint_root: str
    tree_depth: int
    leaf_record: LeafRecordOut
    leaf_hash: str
    path: list[dict]


class AppendProofOut(BaseModel):
    old_seq: int
    new_seq: int
    old_root: str
    new_root: str
    tree_depth: int
    entries: list[dict] = Field(
        ..., description="完美子树条目 (start, height, hash)，数量 O(log n)"
    )


class HealthOut(BaseModel):
    status: str
    release_count: int
    problems: list[str]


PublishResult.model_rebuild()
