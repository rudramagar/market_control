#!/usr/bin/env python3
"""
Raw DROP wire diagnostic. Prints EVERY soup packet type as it arrives,
so we can see exactly what the server does after replay: heartbeats?
end-of-session? silence? This is what tells us why live updates aren't
showing up.

Usage:
    python3 drop_probe.py xnt-dde1api01n 12001 drop01 <password> [send_hb]

Add "send_hb" as a 6th arg to also send client heartbeats every 1s in a
background thread (tests whether the server needs them to keep feeding us).
"""
import socket
import struct
import sys
import threading
import time


def build_login(user, pw, session="", seq="1"):
    body = (b"L" + user.encode().ljust(6)[:6] + pw.encode().ljust(10)[:10]
            + session.encode().rjust(10)[:10] + seq.encode().rjust(20)[:20])
    return struct.pack(">H", len(body)) + body


def read_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("closed")
        buf += chunk
    return buf


def read_packet(sock):
    length = struct.unpack(">H", read_exact(sock, 2))[0]
    body = read_exact(sock, length)
    return chr(body[0]), body


def heartbeat_loop(sock, stop):
    """Send client 'H' every second until stopped."""
    hb = struct.pack(">H", 1) + b"H"
    while not stop.is_set():
        try:
            sock.sendall(hb)
        except Exception:
            return
        stop.wait(1.0)


def main():
    if len(sys.argv) < 5:
        print("usage: drop_probe.py <host> <port> <user> <pw> [send_hb]")
        return 2
    host, port, user, pw = sys.argv[1:5]
    send_hb = len(sys.argv) > 5 and sys.argv[5] == "send_hb"

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(15.0)
    sock.connect((host, int(port)))
    sock.sendall(build_login(user, pw))

    ptype, body = read_packet(sock)
    if ptype != "A":
        print("login failed:", ptype, body[:40]); return 1
    print("login accepted. session/seq:", body[1:].decode("latin1").strip())

    stop = threading.Event()
    if send_hb:
        threading.Thread(target=heartbeat_loop, args=(sock, stop),
                         daemon=True).start()
        print("[sending client heartbeats every 1s]")

    print("reading packets (Ctrl-C to stop)...")
    print("format: <count> type=<T> len=<n> [templateId if S] [seq]")

    counts = {}
    data_count = 0
    replay_done_announced = False
    last_data_time = time.time()

    try:
        while True:
            try:
                ptype, body = read_packet(sock)
            except socket.timeout:
                # No packet for 15s. If we saw data then silence, replay is done
                # and the server is NOT sending heartbeats or live data.
                gap = time.time() - last_data_time
                print(">>> %.0fs SILENCE (no packets at all, incl. no server heartbeat)" % gap)
                continue

            counts[ptype] = counts.get(ptype, 0) + 1

            if ptype == "S":
                data_count += 1
                last_data_time = time.time()
                # decode SBE header: blockLength(2) templateId(2) schemaId(2) ver(2)
                sbe = body[1:]
                template_id = struct.unpack("<H", sbe[2:4])[0]
                # mercury header: timestamp(8) seq(8) at offset 8
                seq = struct.unpack("<q", sbe[16:24])[0]
                # print only every 500th during replay, but ALL once replay seems done
                if data_count % 500 == 0 and not replay_done_announced:
                    print("  ...%d data msgs (last templateId=%d seq=%d)"
                          % (data_count, template_id, seq))
                if replay_done_announced:
                    print("  LIVE DATA: type=S templateId=%d seq=%d" % (template_id, seq))
            else:
                # Non-data packet: heartbeat H, end-of-session Z, debug +, etc.
                last_data_time = time.time()
                print(">>> NON-DATA PACKET: type=%r len=%d  (H=server heartbeat, "
                      "Z=end of session)" % (ptype, len(body)))
                if not replay_done_announced:
                    replay_done_announced = True
                    print(">>> (replay likely done; now watching for live) <<<")
    except KeyboardInterrupt:
        print("\nstopped.")
    finally:
        stop.set()
        print("packet type counts:", counts)
        print("total data (S) msgs:", data_count)
        sock.close()


if __name__ == "__main__":
    sys.exit(main())
