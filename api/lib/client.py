"""
MercuryClient - talks SoupBinTCP to the ME admin API.

Read path (list users / entry points) uses a persistent login. Write path
(suspend / activate) follows the ME's one-command-per-connection rule:
connect, login, send, read the ack, close.

All framing goes through codec.py; this module only orchestrates.
"""
import socket
import struct
import itertools

from .protocol import Spec, encode_message, decode_message
from .socket import SoupSocket
from .models import User, EntryPoint, CommandResult

class MercuryError(Exception):
    pass

class MercuryClient:
    def __init__(self, settings):
        self.s = settings
        self.soup = Spec(settings.soup_spec)
        self.api = Spec(settings.api_spec)
        self._corr = itertools.count(1001)   # correlation id generator
        self.sock = None

    # Connection
    def connect(self):
        self.sock = SoupSocket(self.s.host, self.s.port, self.s.timeout)
        self.sock.connect()

    def close(self):
        if self.sock:
            self.sock.close()
            self.sock = None

    def login(self):
        """Send Login Request, expect Login Accepted. Raise on reject/close."""
        frame = encode_message(self.soup, "L", {
            "packet_length": self.soup.message_length("L") - 2,  # excludes len field
            "username": self.s.user,
            "password": self.s.password,
            "session": self.s.session,
            "sequence": self.s.sequence,
        })
        self.sock.send(frame)
        pkt_type, body = self._read_soup_packet()
        if pkt_type == "A":
            return decode_message(self.soup, "A", body)
        if pkt_type == "J":
            info = decode_message(self.soup, "J", body)
            raise MercuryError("login rejected: reason=%r" % info["reject_reason_code"])
        raise MercuryError("unexpected login reply type %r" % pkt_type)

    # Soup Framing
    def _read_soup_packet(self):
        """Read one soup packet. Returns (packet_type_char, full_body_bytes).
        body includes the 1-byte packet type followed by the payload."""
        length_bytes = self.sock.read_exact(2)
        length = struct.unpack(">H", length_bytes)[0]
        body = self.sock.read_exact(length)           # length counts type + payload
        packet_type = chr(body[0])
        return packet_type, body

    def _send_unsequenced(self, api_msg_key, values):
        """Wrap an API message in a soup 'U' packet and send it."""
        payload = encode_message(self.api, api_msg_key, values)
        header = encode_message(self.soup, "U", {
            "packet_length": 1 + len(payload),        # 'U' type byte + payload
        })
        self.sock.send(header + payload)

    # Read Path: Listing
    def _list(self, reference_data_type):
        """Send List Request (14), read paged List Reply (5), return raw rows."""
        corr = next(self._corr)
        self._send_unsequenced("14", {
            "correlation_id": corr,
            "reference_data_type": reference_data_type,
        })
        rows = []
        while True:
            pkt_type, body = self._read_soup_packet()
            if pkt_type != "S":
                raise MercuryError("expected sequenced data, got %r" % pkt_type)
            payload = body[1:]                        # strip soup type byte
            header = decode_message(self.api, "5", payload)
            row_type = str(header["list_msg_type"])
            row_len = header["list_msg_length"]
            count = header["message_count"]

            # Runtime guard: our spec's row size must match the ME's.
            expected = self.api.message_length(row_type)
            if expected != row_len:
                raise MercuryError(
                    "row size mismatch type %s: spec=%d ME=%d"
                    % (row_type, expected, row_len))

            header_len = self.api.message_length("5")
            block = payload[header_len:]
            for i in range(count):
                raw = block[i * row_len:(i + 1) * row_len]
                rows.append((row_type, decode_message(self.api, row_type, raw)))

            if header["next_page"] == -1:
                break
        return rows

    def list_users(self):
        ref = self.api.raw["reference_data_types"]["user"]
        return [
            User(user_id=r["user_id"], user_name=r["user_name"],
                 firm_id=r["firm_id"], firm_code=r["firm_code"],
                 suspension_status=r["suspension_status"],
                 user_type_name=r["user_type_name"])
            for _t, r in self._list(ref)
        ]

    def list_entry_points(self):
        ref = self.api.raw["reference_data_types"]["entry_point"]
        return [
            EntryPoint(host_user_id=r["host_user_id"],
                       client_user_id=r["client_user_id"],
                       protocol=r["protocol"],
                       host_user_name=r["host_user_name"],
                       client_user_name=r["client_user_name"],
                       logon_count=r["logon_count"],
                       logon_status=r["logon_status"])
            for _t, r in self._list(ref)
        ]

    # Write path: Suspend/Active
    def _update_user_state(self, user_id, status, action):
        """Send Update User State (29), read Accept (0) or Reject (8).
        Follows one-command-per-connection: caller connects+logs in fresh."""
        corr = next(self._corr)
        self._send_unsequenced("29", {
            "correlation_id": corr,
            "user_id": user_id,
            "suspension_status": status,
        })
        pkt_type, body = self._read_soup_packet()
        payload = body[1:]
        msg_type = payload[0]
        if msg_type == 0:
            return CommandResult(ok=True, user_id=user_id, action=action)
        if msg_type == 8:
            info = decode_message(self.api, "8", payload)
            reason = self.api.raw["reject_reasons"].get(
                str(info["reject_reason"]), "reason_%d" % info["reject_reason"])
            return CommandResult(ok=False, user_id=user_id, action=action, reason=reason)
        raise MercuryError("unexpected reply msg_type %d" % msg_type)

    def suspend(self, user_id):
        return self._update_user_state(user_id, "S", "suspend")

    def activate(self, user_id):
        return self._update_user_state(user_id, "A", "activate")
