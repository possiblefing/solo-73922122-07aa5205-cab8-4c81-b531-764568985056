"""测试夹具：在导入 app 包之前指向独立的全新临时 DATA_DIR。"""
from __future__ import annotations

import os
import shutil
import sys
import tempfile
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO_ROOT))

# 每次 pytest 会话使用全新空目录，且在导入 app.* 之前完成，
# 避免与模块级 Database/BlobStore 的初始化顺序冲突。
_DATA_DIR = Path(tempfile.mkdtemp(prefix="wq-release-test-"))
os.environ["DATA_DIR"] = str(_DATA_DIR)
os.environ["MAX_RELEASES"] = "16"  # depth=4，足够容纳全部用例的发布
os.environ["MAX_FILE_BYTES"] = str(64 * 1024)


@pytest.fixture(scope="session")
def app_client():
    from app.config import settings
    from app.main import app
    from fastapi.testclient import TestClient

    assert str(settings.data_dir) == str(_DATA_DIR)
    with TestClient(app) as client:
        yield client, app, settings

    shutil.rmtree(_DATA_DIR, ignore_errors=True)
