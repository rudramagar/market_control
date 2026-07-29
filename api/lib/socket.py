"""
Socket transport for SoupBinTCP.

Owns the TCP connection lifecycle and the raw read/write primitives.
protocol.py stays pure (bytes in/out); client.py orchestrates. This file
is the only place that touches the OS socket.

Note: this module is named socket.py but imports the stdlib socket via an
absolute import below, which resolves to the standard library, not this
file - relative imports (from .socket) are what load this module.
"""
import socket as _socket   # stdlib; aliased so the name is unambiguous


class SoupSocket:
    """A thin TCP wrapper with exact-length reads."""

    def __init__(self, host, port, timeout=10.0):
        self.host = host
        self.port = int(port)
        self.timeout = timeout
        self._sock = None

    def connect(self):
        s = _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM)
        s.settimeout(self.timeout)
        s.connect((self.host, self.port))
        self._sock = s

    def send(self, data):
        """Send all bytes."""
        self._sock.sendall(data)

    def read_exact(self, n):
        """Read exactly n bytes, looping across TCP segments. Raises on close."""
        buf = b""
        while len(buf) < n:
            chunk = self._sock.recv(n - len(buf))
            if not chunk:
                raise ConnectionError(
                    "connection closed after %d/%d bytes" % (len(buf), n))
            buf += chunk
        return buf

    def close(self):
        if self._sock:
            try:
                self._sock.close()
            finally:
                self._sock = None

    def set_timeout(self, seconds):
        """Adjust the socket read timeout (e.g. generous for live streaming)."""
        self.timeout = seconds
        if self._sock:
            self._sock.settimeout(seconds)

    def raw(self):
        """The underlying socket, for use with select() in the live loop."""
        return self._sock

    @property
    def connected(self):
        return self._sock is not None
