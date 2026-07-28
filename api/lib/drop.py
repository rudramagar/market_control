"""
DROP feed reader.

The DROP feed is SoupBinTCP carrying SBE-encoded reference-data and state
messages. We log in requesting sequence 1 (full replay), fold every record
into an in-memory snapshot keyed by id, and keep reading so live changes
update the same snapshot.

Each SBE message is:
    SBE header (8) : block_length, template_id, schema_id, version
    Mercury header (16): timestamp_ns, matching_engine_seq_num
    payload (template-specific)
We dispatch on template_id. The snapshot is the source of truth the API
server hands to the browser (initial list) and streams deltas from (live).
"""
import json
import struct

from .socket import SoupSocket
from .protocol import Spec, encode_message


# Snapshot buckets by record kind (template name -> {id: record}).
_ID_FIELD = {
    "user": "user_id",
    "firm": "firm_id",
    "market": "market_id",
    "security": "security_id",
}


class DropSpec:
    """Loads the SBE DROP spec: common header + per-template payloads."""

    def __init__(self, path):
        with open(path) as fh:
            data = json.load(fh)
        self.byte_order = data.get("byte_order", "little")
        self.endian = "<" if self.byte_order == "little" else ">"
        self.messages = data["messages"]
        self.states = data.get("states", {})
        self.header_fields = data["common_header"]["fields"]
        self.header_len = sum(f["length"] for f in self.header_fields)
        self._header_fmt = self._fmt(self.header_fields)
        self._payload_fmt = {
            tid: self._fmt(m["fields"]) for tid, m in self.messages.items()
        }

    def _fmt(self, fields):
        fmt = self.endian
        for f in fields:
            if f["type"] == "alpha":
                fmt += "%ds" % f["length"]
            else:
                char = {1: "b", 2: "h", 4: "i", 8: "q"}[f["length"]]
                fmt += char if f["type"] == "int" else char.upper()
        return fmt

    def decode_header(self, raw):
        vals = struct.unpack(self._header_fmt, raw[:self.header_len])
        return dict(zip((f["name"] for f in self.header_fields), vals))

    def decode_payload(self, template_id, raw):
        tid = str(template_id)
        msg = self.messages[tid]
        fmt = self._payload_fmt[tid]
        size = struct.calcsize(fmt)
        vals = struct.unpack(fmt, raw[self.header_len:self.header_len + size])
        record = {}
        for f, v in zip(msg["fields"], vals):
            if isinstance(v, bytes):
                v = v.decode("ascii", "replace").rstrip("\x00").strip()
            record[f["name"]] = v
        record["_kind"] = msg["name"]
        return record


class DropReader:
    """Owns the DROP connection and the live snapshot."""

    def __init__(self, settings):
        self.s = settings
        self.soup = Spec(settings.soup_spec)          # reuse soup framing
        self.drop = DropSpec(settings.drop_spec)
        self.sock = None
        # snapshot: kind -> {id: record}
        self.snapshot = {kind: {} for kind in _ID_FIELD}
        self.last_seq = 0

    def connect_and_login(self):
        self.sock = SoupSocket(self.s.host, self.s.port, self.s.timeout)
        self.sock.connect()
        frame = encode_message(self.soup, "L", {
            "packet_length": self.soup.message_length("L") - 2,
            "username": self.s.user,
            "password": self.s.password,
            "session": self.s.session,
            "sequence": self.s.sequence,           # "1" for full replay
        })
        self.sock.send(frame)
        pkt_type, body = self._read_soup_packet()
        if pkt_type != "A":
            raise RuntimeError("DROP login failed, got soup type %r" % pkt_type)
        return self.soup and body

    def close(self):
        if self.sock:
            self.sock.close()
            self.sock = None

    def send_heartbeat(self):
        """Send a SoupBinTCP client Heartbeat ('H') to keep the session alive.
        Standard SoupBin servers stop feeding a client that goes silent."""
        frame = encode_message(self.soup, "H", {
            "packet_length": 1,            # just the 'H' type byte
            "packet_type": "H",
        })
        self.sock.send(frame)

    def _read_soup_packet(self):
        length = struct.unpack(">H", self.sock.read_exact(2))[0]
        body = self.sock.read_exact(length)
        return chr(body[0]), body

    def read_message(self):
        """Read one DROP message. Returns (template_name, record) or None
        for non-data soup packets (heartbeat, etc.). Updates the snapshot."""
        pkt_type, body = self._read_soup_packet()
        if pkt_type != "S":
            return None                               # heartbeat / other framing
        sbe = body[1:]                                # strip soup type byte
        header = self.drop.decode_header(sbe)
        self.last_seq = header["matching_engine_seq_num"]
        tid = str(header["template_id"])
        if tid not in self.drop.messages:
            return None                               # template we don't track
        record = self.drop.decode_payload(tid, sbe)
        self._apply(record)
        return record["_kind"], record

    def _apply(self, record):
        """Fold a record into the snapshot.

        Base records (user/firm/market/security) are stored by id. Status
        events (user_status/firm_status) carry only a new state for an
        existing id, so they update the state field of the base record -
        this is how a runtime suspend/activate shows up in the snapshot.
        """
        kind = record["_kind"]

        if kind == "user_status":
            self._update_state("user", record["user_id"], record["state"])
        elif kind == "firm_status":
            self._update_state("firm", record["firm_id"], record["state"])
        else:
            id_field = _ID_FIELD[kind]
            self.snapshot[kind][record[id_field]] = record

    def _update_state(self, kind, record_id, state):
        """Apply a state change to an existing base record (or stub one)."""
        bucket = self.snapshot[kind]
        if record_id in bucket:
            bucket[record_id]["state"] = state
        else:
            # status arrived before the base record; stub it so state isn't lost
            bucket[record_id] = {_ID_FIELD[kind]: record_id, "state": state,
                                 "_kind": kind}

    def users(self):
        """Current snapshot of users as a list."""
        return list(self.snapshot["user"].values())

    def run_live(self, on_replay_done=None, on_change=None, stop_event=None,
                 heartbeat_interval=1.0):
        """Connect, replay to build the snapshot, then stream live changes.

        Verified against the live feed: the DROP server replays from seq 1,
        sends server heartbeats ('H') when idle, and pushes live 'S' messages
        as state changes occur - all on the same held connection. We must send
        client heartbeats or the server eventually drops us.

        Callbacks:
          on_replay_done(users) - fired once when replay catches up (snapshot ready)
          on_change(kind, record, users) - fired on each live state change
        Blocks until stop_event is set or the connection drops (raises).
        """
        import threading

        self.reset()
        self.connect_and_login()

        # Client heartbeat sender in the background.
        hb_stop = threading.Event()

        def _hb():
            while not hb_stop.is_set():
                try:
                    self.send_heartbeat()
                except Exception:
                    return
                hb_stop.wait(heartbeat_interval)

        hb_thread = threading.Thread(target=_hb, name="drop-hb", daemon=True)
        hb_thread.start()

        live = False
        try:
            while stop_event is None or not stop_event.is_set():
                pkt_type, body = self._read_soup_packet()

                if pkt_type == "H":
                    # Server heartbeat. First one after data => replay caught up.
                    if not live:
                        live = True
                        if on_replay_done:
                            on_replay_done(self.users())
                    continue

                if pkt_type == "Z":
                    # End of session - server is done with this session.
                    break

                if pkt_type != "S":
                    continue                       # debug/other framing, ignore

                changed = self._handle_data(body)
                if live and changed and on_change:
                    on_change(changed[0], changed[1], self.users())
        finally:
            hb_stop.set()
            self.close()

    def _handle_data(self, body):
        """Decode one 'S' data packet, apply to snapshot. Returns (kind, record)
        if it was a tracked template, else None (unknown templates skipped)."""
        sbe = body[1:]
        header = self.drop.decode_header(sbe)
        self.last_seq = header["matching_engine_seq_num"]
        tid = str(header["template_id"])
        if tid not in self.drop.messages:
            return None                            # unknown template - skip safely
        record = self.drop.decode_payload(tid, sbe)
        self._apply(record)
        return record["_kind"], record

    def reset(self):
        """Clear the snapshot (called before each fresh replay)."""
        for bucket in self.snapshot.values():
            bucket.clear()
        self.last_seq = 0

    def snapshot_once(self, max_messages=1000000):
        """One full refresh: connect, replay to end, close, return users.

        Because the DROP feed only reflects state changes on a fresh replay,
        the supervisor calls this repeatedly to poll current state. Reads
        until the feed goes quiet (socket timeout = replay caught up).
        """
        self.reset()
        try:
            self.connect_and_login()
            count = 0
            try:
                while count < max_messages:
                    if self.read_message() is not None:
                        count += 1
            except (ConnectionError, OSError):
                pass                          # timeout = replay caught up
            return self.users()
        finally:
            self.close()
