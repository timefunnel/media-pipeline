"""有界 Node 桥接进程池：复用模块和 HTTP 连接，单进程始终串行执行。"""

import atexit
import json
import queue
import subprocess
import threading
import time
from pathlib import Path


class _Worker:
    def __init__(self, bridge):
        self.bridge = bridge
        self.process = None
        self.responses = None
        self.lock = threading.Lock()
        self.closed = False

    def _start(self):
        with self.lock:
            if self.closed:
                raise RuntimeError("native source bridge is closed")
            if self.process is None:
                process = subprocess.Popen(
                    ["node", "--max-old-space-size=192", self.bridge, "--worker"],
                    stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                    text=True, encoding="utf-8", bufsize=1,
                )
                responses = queue.Queue()
                self.process, self.responses = process, responses
                threading.Thread(target=self._read, args=(process, responses), daemon=True).start()
            return self.process, self.responses

    @staticmethod
    def _read(process, responses):
        try:
            while True:
                line = process.stdout.readline(32 * 1024 * 1024 + 1)
                if not line:
                    raise RuntimeError("native source bridge exited without a response")
                if not line.endswith("\n"):
                    raise RuntimeError("native source bridge response exceeds 32 MiB or is incomplete")
                responses.put(json.loads(line))
        except (ValueError, OSError, RuntimeError) as exc:
            responses.put(RuntimeError("native source bridge protocol failed: %s" % exc))
        finally:
            process.stdout.close()

    def reset(self, closing=False):
        with self.lock:
            if closing:
                self.closed = True
            process, self.process = self.process, None
        if process is not None:
            if process.poll() is None:
                process.kill()
            process.wait(timeout=5)
            process.stdin.close()

    def call(self, request, deadline):
        try:
            process, responses = self._start()
            process.stdin.write(json.dumps(request, ensure_ascii=False) + "\n")
            process.stdin.flush()
            response = responses.get(timeout=max(0, deadline - time.monotonic()))
            if isinstance(response, Exception):
                raise response
            if not isinstance(response, dict) or not isinstance(response.get("ok"), bool):
                raise RuntimeError("native source bridge returned an invalid envelope")
        except queue.Empty as exc:
            self.reset()
            raise RuntimeError("native source bridge request timed out") from exc
        except (OSError, RuntimeError) as exc:
            self.reset()
            raise RuntimeError("native source bridge failed: %s" % exc) from exc
        if not response["ok"]:
            self.reset()
            # 上游业务失败显式返回；不重试，也不让本次错误污染下一次请求。
            raise RuntimeError(str(response.get("error") or "native source request failed"))
        if "result" not in response:
            self.reset()
            raise RuntimeError("native source bridge response is missing result")
        return response["result"]


class BridgePool:
    def __init__(self, bridge, size=2):
        self.bridge = str(bridge)
        self.workers = tuple(_Worker(self.bridge) for _ in range(size))
        self.available = queue.LifoQueue(maxsize=size)
        self.closed = threading.Event()
        for worker in self.workers:
            self.available.put(worker)
        atexit.register(self.close)

    def call(self, action, source, data, timeout):
        if self.closed.is_set():
            raise RuntimeError("native source bridge is closed")
        if not Path(self.bridge).is_file():
            raise RuntimeError("danmu_api source bridge is missing")
        deadline = time.monotonic() + timeout
        try:
            worker = self.available.get(timeout=timeout)
        except queue.Empty as exc:
            raise RuntimeError("native source request queue timed out") from exc
        try:
            return worker.call({"action": action, "source": source, "data": data}, deadline)
        finally:
            self.available.put(worker)

    def close(self):
        if not self.closed.is_set():
            self.closed.set()
            atexit.unregister(self.close)
            for worker in self.workers:
                worker.reset(closing=True)
