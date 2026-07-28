import os
from dataclasses import dataclass
from pathlib import Path

# specs/ sits next to the package directory (backend/specs).
_BACKEND_ROOT = Path(__file__).resolve().parent.parent
_SPECS = _BACKEND_ROOT / "specs"


@dataclass
class Settings:
    host: str
    port: int
    user: str
    password: str
    session: str = ""
    sequence: str = "1"
    timeout: float = 10.0
    soup_spec: str = str(_SPECS / "soup_spec.json")
    api_spec: str = str(_SPECS / "soup_api_spec.json")
    drop_spec: str = str(_SPECS / "soup_drop_spec.json")

    @classmethod
    def from_env(cls, **overrides):
        """Build settings from env vars, letting explicit overrides win."""
        base = dict(
            host=os.environ.get("ME_HOST", ""),
            port=int(os.environ.get("ME_PORT", "11005")),
            user=os.environ.get("ME_USER", ""),
            password=os.environ.get("ME_PASSWORD", ""),
            session=os.environ.get("ME_SESSION", ""),
            sequence=os.environ.get("ME_SEQUENCE", "1"),
            timeout=float(os.environ.get("ME_TIMEOUT", "10.0")),
        )
        base.update({k: v for k, v in overrides.items() if v is not None})
        return cls(**base)
