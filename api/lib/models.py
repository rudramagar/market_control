from dataclasses import dataclass, asdict
from typing import Optional

@dataclass
class User:
    """A matching-engine user with its suspension state."""
    user_id: int
    user_name: str
    firm_id: int
    firm_code: str
    suspension_status: str          # "A" = active, "S" = suspended
    user_type_name: str

    @property
    def is_suspended(self) -> bool:
        return self.suspension_status == "S"

    def to_dict(self) -> dict:
        d = asdict(self)
        d["is_suspended"] = self.is_suspended
        return d


@dataclass
class EntryPoint:
    """A live connection entry point (host/client session)."""
    host_user_id: int
    client_user_id: int
    protocol: int
    host_user_name: str
    client_user_name: str
    logon_count: int
    logon_status: int               # 0 = logged off, 1 = logged on

    @property
    def is_logged_on(self) -> bool:
        return self.logon_status == 1

    def to_dict(self) -> dict:
        d = asdict(self)
        d["is_logged_on"] = self.is_logged_on
        return d


@dataclass
class CommandResult:
    """Result of a suspend/activate command."""
    ok: bool
    user_id: int
    action: str                     # "suspend" or "activate"
    reason: Optional[str] = None    # reject reason name when ok is False

    def to_dict(self) -> dict:
        return asdict(self)
