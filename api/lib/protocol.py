import json
import struct

_INT_CHAR = {1: "b", 2: "h", 4: "i", 8: "q"}

def _field_char(field):
    """struct format char(s) for one field, endian-independent."""
    t, n = field["type"], field["length"]
    if t == "alpha":
        return "%ds" % n
    char = _INT_CHAR[n]
    return char if t == "int" else char.upper()

class Spec:
    """A loaded message spec (one JSON file)."""

    def __init__(self, path):
        with open(path) as fh:
            data = json.load(fh)
        self.byte_order = data.get("byte_order", "big")
        self.endian = "<" if self.byte_order == "little" else ">"
        self.messages = data["messages"]
        self.raw = data  # keep the rest (reject_reasons, ref types, etc.)

    def struct_format(self, msg_key):
        """Build the struct format string for a message's fields."""
        return self.endian + "".join(
            _field_char(f) for f in self.messages[msg_key]["fields"]
        )

    def field_names(self, msg_key):
        return [f["name"] for f in self.messages[msg_key]["fields"]]

    def message_length(self, msg_key):
        return sum(f["length"] for f in self.messages[msg_key]["fields"])

def encode_field(field, value, endian):
    """Encode one field to bytes per its spec entry."""
    t, n = field["type"], field["length"]
    if t == "alpha":
        s = str(value)
        pad = field.get("pad", "right")
        # pad "right" => left-justified text (spaces on the right);
        # pad "left"  => right-justified text (spaces on the left).
        s = s.rjust(n) if pad == "left" else s.ljust(n)
        return s.encode("ascii", "replace")[:n]

    byteorder = "little" if endian == "<" else "big"
    signed = (t == "int")
    return int(value).to_bytes(n, byteorder=byteorder, signed=signed)

def encode_message(spec, msg_key, values):
    """Pack a whole message. `values` is a dict of field name -> value;
    fields with a spec "value" default are auto-filled if absent."""
    out = b""
    for f in spec.messages[msg_key]["fields"]:
        if f["name"] in values:
            v = values[f["name"]]
        elif "value" in f:
            v = f["value"]
        else:
            raise KeyError("missing field %r for message %r" % (f["name"], msg_key))
        out += encode_field(f, v, spec.endian)
    return out

def decode_message(spec, msg_key, raw):
    """Unpack raw bytes into a dict of field name -> value, cleaning strings."""
    fmt = spec.struct_format(msg_key)
    size = struct.calcsize(fmt)
    values = struct.unpack(fmt, raw[:size])
    result = {}
    for name, val in zip(spec.field_names(msg_key), values):
        if isinstance(val, bytes):
            val = val.decode("ascii", "replace").rstrip("\x00").strip()
        result[name] = val
    return result
