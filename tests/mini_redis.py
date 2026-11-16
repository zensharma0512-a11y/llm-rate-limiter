"""
A tiny Redis client (RESP2 over a socket), just enough for the tests and the
benchmark, so they need no third-party packages. In real use, pass a redis-py
client instead: RedisRateLimiter only calls methods redis-py also has.

Also a helper that starts a throwaway redis-server on a free port.
"""

import os
import shutil
import socket
import subprocess
import tempfile
import time


class RedisError(Exception):
    pass


class MiniRedis:
    def __init__(self, host="127.0.0.1", port=6379):
        self.sock = socket.create_connection((host, port))
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.f = self.sock.makefile("rb")

    def close(self):
        self.f.close()
        self.sock.close()

    def execute(self, *args):
        out = [b"*%d\r\n" % len(args)]
        for a in args:
            a = a if isinstance(a, bytes) else str(a).encode()
            out.append(b"$%d\r\n%s\r\n" % (len(a), a))
        self.sock.sendall(b"".join(out))
        return self._read()

    def _read(self):
        line = self.f.readline()
        kind, rest = line[:1], line[1:-2]
        if kind == b"+":
            return rest
        if kind == b"-":
            raise RedisError(rest.decode())
        if kind == b":":
            return int(rest)
        if kind == b"$":
            n = int(rest)
            if n < 0:
                return None
            data = self.f.read(n + 2)
            return data[:-2]
        if kind == b"*":
            n = int(rest)
            return None if n < 0 else [self._read() for _ in range(n)]
        raise RedisError(f"bad reply: {line!r}")

    # redis-py-compatible subset
    def eval(self, script, numkeys, *args):
        return self.execute("EVAL", script, numkeys, *args)

    def evalsha(self, sha, numkeys, *args):
        return self.execute("EVALSHA", sha, numkeys, *args)

    def hgetall(self, key):
        flat = self.execute("HGETALL", key) or []
        return dict(zip(flat[::2], flat[1::2]))

    def delete(self, *keys):
        return self.execute("DEL", *keys)

    def flushdb(self):
        return self.execute("FLUSHDB")


class RedisServer:
    """Start redis-server on a free port; stop it on exit."""

    def __init__(self):
        self.binary = shutil.which("redis-server")
        self.proc = None
        self.port = None

    @property
    def available(self):
        return self.binary is not None or os.environ.get("REDIS_PORT")

    def __enter__(self):
        if os.environ.get("REDIS_PORT"):          # e.g. a CI service container
            self.port = int(os.environ["REDIS_PORT"])
            return self
        s = socket.socket()
        s.bind(("127.0.0.1", 0))
        self.port = s.getsockname()[1]
        s.close()
        self.dir = tempfile.mkdtemp()
        self.proc = subprocess.Popen(
            [self.binary, "--port", str(self.port), "--bind", "127.0.0.1",
             "--save", "", "--appendonly", "no", "--dir", self.dir],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        deadline = time.time() + 10
        while time.time() < deadline:
            try:
                MiniRedis(port=self.port).execute("PING")
                return self
            except OSError:
                time.sleep(0.05)
        raise RuntimeError("redis-server did not start")

    def __exit__(self, *exc):
        if self.proc:
            self.proc.terminate()
            self.proc.wait(timeout=5)
            shutil.rmtree(self.dir, ignore_errors=True)
