import socket as _socket

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

    @property
    def connected(self):
        return self._sock is not None
