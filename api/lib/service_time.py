"""
Operating-hours schedule for the ME, loaded from config/schedule.json.

Hours are market-agnostic: whatever start_time/end_time the config gives
for the selected market/env is the operating window. The schedule makes
no assumption about DAY vs NIGHT hours - it just reads the config. This
module answers one question: should the pod be maintaining a DROP
connection right now?

Times are computed in the configured timezone explicitly, because the
container clock is usually UTC.
"""
import json
from datetime import datetime, time, timedelta, timezone
from pathlib import Path

_DEFAULT_PATH = Path(__file__).resolve().parent.parent / "config" / "schedule.json"

# Named timezones we support (name -> UTC offset hours). JST for the ME.
_TZ_OFFSETS = {"JST": 9, "UTC": 0}


class Schedule:
    def __init__(self, path=None):
        self.path = str(path or _DEFAULT_PATH)
        self._load()

    def _load(self):
        with open(self.path) as fh:
            cfg = json.load(fh)
        oh = cfg["operating_hours"]
        self.env = oh.get("env", "")
        self.market = oh.get("market", "")
        self.tz = timezone(timedelta(hours=_TZ_OFFSETS[oh.get("timezone", "JST")]))
        self.start_time = _parse_hhmm(oh["start_time"])
        self.end_time = _parse_hhmm(oh["end_time"])
        self.operating_days = _parse_days(oh["operating_days"])
        self.ignore_holidays = bool(oh.get("ignore_holidays", False))
        # Flatten year-keyed holidays into one set of "YYYY-MM-DD" strings.
        self.holidays = set()
        for _year, dates in oh.get("holidays", {}).items():
            self.holidays.update(dates)
        rc = cfg.get("reconnect", {})
        self.backoff_seconds = rc.get("backoff_seconds", 5)
        self.max_backoff_seconds = rc.get("max_backoff_seconds", 60)

    def _now(self, now=None):
        return (now or datetime.now(self.tz)).astimezone(self.tz)

    def is_open(self, now=None):
        """True if the pod should be maintaining a DROP connection now."""
        now = self._now(now)
        if not self.ignore_holidays and now.strftime("%Y-%m-%d") in self.holidays:
            return False
        if now.isoweekday() not in self.operating_days:   # Mon=1 .. Sun=7
            return False
        return self.start_time <= now.time() <= self.end_time

    def seconds_until_open(self, now=None):
        """Seconds until the next open window begins (for idle sleeping)."""
        now = self._now(now)
        for day_offset in range(0, 9):
            day = now + timedelta(days=day_offset)
            if not self.ignore_holidays and day.strftime("%Y-%m-%d") in self.holidays:
                continue
            if day.isoweekday() not in self.operating_days:
                continue
            start_dt = day.replace(hour=self.start_time.hour,
                                   minute=self.start_time.minute,
                                   second=0, microsecond=0)
            if start_dt > now:
                return int((start_dt - now).total_seconds())
        return 3600

    def status(self, now=None):
        """Human-readable status for the web view / health endpoint."""
        state = "open" if self.is_open(now) else "closed (outside operating hours)"
        return "%s/%s %s" % (self.env, self.market, state)


def _parse_hhmm(s):
    hh, mm = s.split(":")
    return time(int(hh), int(mm))


def _parse_days(spec):
    """Parse a cron-style day spec into a set of ISO weekdays (Mon=1..Sun=7).

    Supports '1-5', '1,3,5', '1-5,7'. Uses cron-ish numbering where
    1=Mon..5=Fri; 0 or 7 = Sunday (both map to ISO 7).
    """
    days = set()
    for part in str(spec).split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            lo, hi = part.split("-")
            for d in range(int(lo), int(hi) + 1):
                days.add(_norm_day(d))
        else:
            days.add(_norm_day(int(part)))
    return days


def _norm_day(d):
    """Map cron day (0 or 7 = Sunday) to ISO weekday (Mon=1..Sun=7)."""
    return 7 if d == 0 else d
