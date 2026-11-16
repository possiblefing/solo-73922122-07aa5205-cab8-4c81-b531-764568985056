"""双实例共卷并发验证（Docker 不可用环境下的等价集成测试）。

以两个独立 uvicorn 进程启动同一应用、挂载同一 DATA_DIR，
验证：
  1. 并发发布同单：恰有一个写入，另一个幂等复用，不分叉不覆盖；
  2. 并发发布异单：全部成功且序号稠密、检查点连续；
  3. 实例 A 的发布可在实例 B 下载，证明与检查点跨实例一致；
  4. 发布风暴期间跨实例连续取证：纳入/追加证明每个都 200 且离线可核验；
  5. 另一实例持续发布的同时新实例挂同卷启动：自检不误报损坏、
     发布不被阻塞，历史检查点/证明持续有效，可继续追加。

用法：python scripts/dual_instance_check.py
"""
from __future__ import annotations

import http.client
import io
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


# 每次运行随机选取空闲端口，避免环境中无法回收的旧进程占用。
PORT_A = int(os.environ.get("PORT_A", str(_free_port())))
PORT_B = int(os.environ.get("PORT_B", str(_free_port())))
PORT_C = int(os.environ.get("PORT_C", str(_free_port())))


def _wait_ready(port: int, timeout: float = 20.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                with urllib.request.urlopen(
                    f"http://127.0.0.1:{port}/health", timeout=2
                ) as resp:
                    if resp.status == 200:
                        return
        except OSError:
            time.sleep(0.3)
    raise RuntimeError(f"实例端口 {port} 未就绪")


def start_instance(port: int, data_dir: str) -> subprocess.Popen:
    env = dict(os.environ)
    env.update(
        {
            "DATA_DIR": data_dir,
            "MAX_RELEASES": "128",
            "MAX_FILE_BYTES": str(64 * 1024),
        }
    )
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "app.main:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
        ],
        cwd=str(REPO_ROOT),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    _wait_ready(port)
    return proc


def _multipart_body(fields: dict, file_field: str, filename: str, content: bytes):
    boundary = f"----wq{uuid.uuid4().hex}"
    body = io.BytesIO()
    for key, value in fields.items():
        body.write(f"--{boundary}\r\n".encode())
        body.write(
            f'Content-Disposition: form-data; name="{key}"\r\n\r\n{value}\r\n'.encode()
        )
    body.write(f"--{boundary}\r\n".encode())
    body.write(
        f'Content-Disposition: form-data; name="{file_field}"; '
        f'filename="{filename}"\r\nContent-Type: text/csv\r\n\r\n'.encode()
    )
    body.write(content)
    body.write(f"\r\n--{boundary}--\r\n".encode())
    return body.getvalue(), f"multipart/form-data; boundary={boundary}"


def post_release(port: int, station: str, day: str, order: str, content: bytes):
    body, ctype = _multipart_body(
        {"station_id": station, "sample_date": day, "order_no": order},
        "file",
        f"{order}.csv",
        content,
    )
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    conn.request("POST", "/releases", body=body, headers={"Content-Type": ctype})
    resp = conn.getresponse()
    payload = json.loads(resp.read().decode())
    conn.close()
    return resp.status, payload


def get_json(port: int, path: str):
    with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=10) as r:
        return r.status, json.loads(r.read().decode())


def get_bytes(port: int, path: str):
    with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=10) as r:
        return r.status, r.read()


def main() -> int:
    data_dir = tempfile.mkdtemp(prefix="wq-dual-")
    print(f"[setup] 共享 DATA_DIR={data_dir}")
    proc_a = start_instance(PORT_A, data_dir)
    proc_b = start_instance(PORT_B, data_dir)
    failures: list[str] = []

    def check(cond: bool, message: str) -> None:
        print(f"[{'PASS' if cond else 'FAIL'}] {message}")
        if not cond:
            failures.append(message)

    try:
        # 1) 并发同单：两实例同时发同一键、同一字节。
        same = b"station,ph\nSAME,7.1\n"
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(
                pool.map(
                    lambda port: post_release(
                        port, "SAME", "2026-11-16", "ORD-DUP", same
                    ),
                    [PORT_A, PORT_B],
                )
            )
        statuses = [r[0] for r in results]
        reused = sorted(r[1]["reused"] for r in results)
        check(statuses == [201, 201], f"并发同单均返回 201（实际 {statuses}）")
        check(reused == [False, True], f"恰有一次写入、一次复用（实际 {reused}）")
        ids = {r[1]["release"]["id"] for r in results}
        check(ids == {1}, f"同单只有一条记录 id=1（实际 {ids}）")

        # 2) 并发异单：10 个不同键分散到两实例。
        keys = [(f"ST{i:02d}", f"ORD-{i:02d}") for i in range(10)]

        def publish_one(item):
            idx, (station, order) = item
            port = PORT_A if idx % 2 == 0 else PORT_B
            return post_release(
                port, station, "2026-11-16", order, f"row-{idx}\n".encode()
            )

        with ThreadPoolExecutor(max_workers=10) as pool:
            bulk = list(pool.map(publish_one, enumerate(keys)))
        check(all(s == 201 for s, _ in bulk), "10 件并发异单全部 201")
        check(
            all(not p["reused"] for _, p in bulk),
            "10 件均为新写入而非复用",
        )

        status_a, releases_a = get_json(PORT_A, "/releases")
        status_b, releases_b = get_json(PORT_B, "/releases")
        check(releases_a == releases_b, "两实例看到完全一致的发布目录")
        ids_seq = [r["id"] for r in releases_a]
        check(
            ids_seq == list(range(1, 12)),
            f"序号 1..11 稠密无分叉（实际 {ids_seq}）",
        )

        # 3) 跨实例取件与检查点一致。
        status_dl, blob = get_bytes(
            PORT_B, "/releases/ST00/2026-11-16/ORD-00/download"
        )
        check(status_dl == 200 and blob == b"row-0\n", "实例 B 可取实例 A 发布的原件")
        _, cp_a = get_json(PORT_A, "/checkpoints/latest")
        _, cp_b = get_json(PORT_B, "/checkpoints/latest")
        check(cp_a == cp_b and cp_a["seq"] == 11, "两实例最新检查点一致且条数=11")

        # 4) 跨实例证明与离线核验。
        _, proof = get_json(
            PORT_B, "/releases/ST00/2026-11-16/ORD-00/inclusion-proof"
        )
        sys.path.insert(0, str(REPO_ROOT))
        from app.verify import verify_append_proof, verify_release_bytes

        ok, msg = verify_release_bytes(proof, b"row-0\n")
        check(ok, f"跨实例纳入证明离线核验通过：{msg}")
        _, old_cp = get_json(PORT_A, "/checkpoints/1")
        _, append = get_json(PORT_A, "/checkpoints/1/append-proof")
        ok, msg = verify_append_proof(old_cp, cp_a, append)
        check(ok, f"1 号→11 号检查点追加证明离线核验通过：{msg}")
        check(
            len(append["entries"]) <= 2 * 7,
            f"追加证明条目数 O(log n)={len(append['entries'])} ≤ 14",
        )

        # 4b) 发布风暴期间跨实例连续取证：B 持续发布，A 连续出证明，
        #     每个响应都必须 200 且离线可核验（修复前会 200 但验不过或 500）。
        storm_errors: list[str] = []

        def storm_publisher():
            for i in range(8):
                status, payload = post_release(
                    PORT_B,
                    "STORM",
                    "2026-11-16",
                    f"ORD-STORM-{i:02d}",
                    f"storm-{i}\n".encode(),
                )
                if status != 201:
                    storm_errors.append(f"风暴发布失败 {status}: {payload}")
                    return
                time.sleep(0.05)

        _, old_cp1 = get_json(PORT_A, "/checkpoints/1")
        storm_thread = threading.Thread(target=storm_publisher)
        storm_thread.start()
        storm_checks = 0
        while storm_thread.is_alive():
            try:
                _, proof = get_json(
                    PORT_A, "/releases/ST00/2026-11-16/ORD-00/inclusion-proof"
                )
                ok, msg = verify_release_bytes(proof, b"row-0\n")
                if not ok:
                    storm_errors.append(f"纳入证明离线核验失败: {msg}")
                    break
                _, append = get_json(PORT_A, "/checkpoints/1/append-proof")
                ok, msg = verify_append_proof(
                    old_cp1,
                    {"seq": append["new_seq"], "root_hex": append["new_root"]},
                    append,
                )
                if not ok:
                    storm_errors.append(f"追加证明离线核验失败: {msg}")
                    break
            except urllib.error.HTTPError as exc:
                storm_errors.append(f"取证请求返回 HTTP {exc.code}")
                break
            storm_checks += 1
        storm_thread.join()
        check(not storm_errors, f"风暴期间连续取证全部有效（{storm_errors}）")
        check(storm_checks >= 2, f"风暴窗口内完成 {storm_checks} 轮连续取证")

        # 5) 故障转移 + 启动竞态：另一实例持续发布的同时，
        #    新实例挂同卷启动（自检与并发发布重叠），不得误报损坏、
        #    不得阻塞发布；随后可继续追加。
        boot_errors: list[str] = []
        boot_stop = threading.Event()

        def boot_publisher():
            i = 0
            while not boot_stop.is_set():
                status, payload = post_release(
                    PORT_B,
                    "BOOT",
                    "2026-11-16",
                    f"ORD-BOOT-{i:02d}",
                    f"boot-{i}\n".encode(),
                )
                if status != 201:
                    boot_errors.append(f"启动期间发布失败 {status}: {payload}")
                    return
                i += 1
                time.sleep(0.05)

        _, releases_pre_boot = get_json(PORT_A, "/releases")
        boot_thread = threading.Thread(target=boot_publisher)
        boot_thread.start()
        try:
            proc_c = start_instance(PORT_C, data_dir)
        finally:
            boot_stop.set()
            boot_thread.join()
        try:
            check(
                not boot_errors,
                f"新实例启动自检期间另一实例发布未受影响（{boot_errors}）",
            )
            _, health = get_json(PORT_C, "/health")
            check(
                health["status"] == "ok",
                "并发发布期间启动的新实例自检通过、未误报损坏",
            )
            _, releases_c = get_json(PORT_C, "/releases")
            check(
                len(releases_c) > len(releases_pre_boot),
                "新实例看到启动期间的并发追加"
                f"（{len(releases_pre_boot)} -> {len(releases_c)}）",
            )
            _, cp_run = get_json(PORT_A, "/checkpoints/latest")
            _, cp_c = get_json(PORT_C, "/checkpoints/latest")
            check(cp_c == cp_run, "新实例与运行中实例最新检查点一致")
            status_post, new_pub = post_release(
                PORT_C, "ST99", "2026-11-16", "ORD-AFTER-RESTART", b"after\n"
            )
            check(
                status_post == 201
                and new_pub["release"]["id"] == len(releases_c) + 1,
                "新实例可继续追加，序号稠密衔接"
                f"（id={new_pub['release']['id']}）",
            )
            _, proof_new = get_json(
                PORT_B,
                "/releases/ST99/2026-11-16/ORD-AFTER-RESTART/inclusion-proof",
            )
            ok, _ = verify_release_bytes(proof_new, b"after\n")
            check(ok, "新实例追加记录的纳入证明可在另一实例离线核验")
        finally:
            # 无法 kill：改为脱离进程（它绑定临时目录，会话结束后随环境回收）。
            print(f"[note] 环境禁止 kill 信号，第三实例保留运行，端口 {PORT_C}")

    finally:
        for proc in (proc_a, proc_b):
            if proc.poll() is None:
                try:
                    proc.send_signal(signal.SIGTERM)
                    proc.wait(timeout=10)
                except (PermissionError, subprocess.TimeoutExpired):
                    # 本环境 seccomp 可能禁止 kill；进程为临时目录下的演示进程。
                    print(f"[note] 无法终止 pid={proc.pid}（环境限制），保留运行")

    print(f"\n结果：{len(failures)} 项失败")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
