#!/usr/bin/env python3
"""
Single-threaded live probe: login seq 0, then use select() to read when
data is ready and send a client heartbeat when idle - ALL in one thread,
so send and recv never overlap. This is the pattern a well-behaved C++
SoupBinTCP client uses to hold a live connection open forever.

Usage:
    python3 drop_live_probe.py xnt-dde1api01n 12001 drop01 <pw> [seq]
    (seq defaults to 0 = live only, no replay)
"""
import socket
import struct
import sys
import select
import time


def build_login(user, pw, session="", seq="0"):
    body = (b"L" + user.encode().ljust(6)[:6] + pw.encode().ljust(10)[:10]
            + session.encode().rjust(10)[:10] + seq.encode().rjust(20)[:20])
    return struct.pack(">H", len(body)) + body


def read_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("closed after %d/%d" % (len(buf), n))
        buf += chunk
    return buf


def read_packet(sock):
    length = struct.unpack(">H", read_exact(sock, 2))[0]
    body = read_exact(sock, length)
    return chr(body[0]), body


def main():
    if len(sys.argv) < 5:
        print("usage: drop_live_probe.py <host> <port> <user> <pw> [seq]")
        return 2
    host, port, user, pw = sys.argv[1:5]
    seq = sys.argv[5] if len(sys.argv) > 5 else "0"

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, int(port)))
    sock.sendall(build_login(user, pw, seq=seq))
    ptype, body = read_packet(sock)
    if ptype != "A":
        print("login failed:", ptype); return 1
    print("login accepted (seq=%s). session/seq:" % seq,
          body[1:].decode("latin1").strip())
    print("single-threaded: read when ready, heartbeat when idle (1s)")

    hb = struct.pack(">H", 1) + b"H"
    sock.setblocking(True)
    last_hb = time.time()
    hb_count = 0
    data_count = 0
    started = time.time()

    try:
        while True:
            ready, _, _ = select.select([sock], [], [], 1.0)
            now = time.time()
            if ready:
                ptype, body = read_packet(sock)
                if ptype == "S":
                    data_count += 1
                    sbe = body[1:]
                    tid = struct.unpack("<H", sbe[2:4])[0]
                    s = struct.unpack("<q", sbe[16:24])[0]
                    print("[%5.1fs] DATA templateId=%d seq=%d" % (now-started, tid, s))
                elif ptype == "H":
                    print("[%5.1fs] server heartbeat" % (now-started))
                elif ptype == "Z":
                    print("[%5.1fs] END OF SESSION" % (now-started)); break
                else:
                    print("[%5.1fs] packet %r" % (now-started, ptype))
            # send a client heartbeat ~every second, in THIS thread only
            if now - last_hb >= 1.0:
                sock.sendall(hb)
                last_hb = now
                hb_count += 1
                print("[%5.1fs] -> sent client heartbeat #%d" % (now-started, hb_count))
    except (ConnectionError, OSError) as e:
        print("[%5.1fs] connection ended: %s" % (time.time()-started, e))
    finally:
        print("survived %.1fs, %d data msgs, %d client heartbeats sent"
              % (time.time()-started, data_count, hb_count))
        sock.close()


if __name__ == "__main__":
    sys.exit(main())
