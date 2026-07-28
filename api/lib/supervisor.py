"""
Supervisor: the always-on engine that keeps the session snapshot fresh.

The DROP feed only reflects state changes on a fresh replay (verified: it
does not push to a held connection), so we poll by reconnecting on an
interval. During operating hours the supervisor repeatedly takes a full
snapshot and publishes it; outside hours it idles without connecting.

Runs in a background thread. A publish callback receives each new snapshot
(the WebSocket layer hooks into this to push to browsers). Connection
status is exposed for the health endpoint and the web view.
"""
import threading
import time

from .drop import DropReader
from .service_time import Schedule


# Connection/service states surfaced to the web view + health endpoint.
STATUS_STARTING = "starting"
STATUS_OPEN = "open"                 # actively refreshing snapshots
STATUS_CONNECTING = "connecting"     # in window but connection failing
STATUS_CLOSED = "closed"             # outside operating hours, idle


class Supervisor:
    def __init__(self, settings, schedule=None, on_snapshot=None):
        self.settings = settings
        self.schedule = schedule or Schedule()
        self.on_snapshot = on_snapshot           # callback(list_of_users)
        self._stop = threading.Event()
        self._thread = None
        # shared state (read by the server for /health and initial page load)
        self.lock = threading.Lock()
        self.status = STATUS_STARTING
        self.users = []
        self.last_refresh = None
        self.last_error = None

    # --- lifecycle ----------------------------------------------------------

    def start(self):
        self._thread = threading.Thread(target=self._run, name="supervisor",
                                        daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)

    # --- the loop -----------------------------------------------------------

    def _run(self):
        backoff = self.schedule.backoff_seconds
        while not self._stop.is_set():
            if not self.schedule.is_open():
                self._set_status(STATUS_CLOSED)
                # sleep until near the next open, but wake periodically so a
                # config/override change or shutdown is noticed reasonably soon.
                nap = min(self.schedule.seconds_until_open(), 60)
                self._sleep(max(nap, 5))
                continue

            try:
                users = self._refresh_once()
                self._publish(users)
                backoff = self.schedule.backoff_seconds       # reset on success
                self._sleep(self.schedule.refresh_interval_seconds)
            except Exception as e:                            # connection failed
                self._set_status(STATUS_CONNECTING, error=str(e))
                self._sleep(backoff)
                backoff = min(backoff * 2, self.schedule.max_backoff_seconds)

    def _refresh_once(self):
        reader = DropReader(self.settings)
        return reader.snapshot_once()

    def _publish(self, users):
        with self.lock:
            self.status = STATUS_OPEN
            self.users = users
            self.last_refresh = time.time()
            self.last_error = None
        if self.on_snapshot:
            try:
                self.on_snapshot(users)
            except Exception:
                pass                              # never let a subscriber break the loop

    # --- helpers ------------------------------------------------------------

    def _set_status(self, status, error=None):
        with self.lock:
            self.status = status
            if error is not None:
                self.last_error = error

    def _sleep(self, seconds):
        # interruptible sleep so stop() is responsive
        self._stop.wait(timeout=seconds)

    def health(self):
        """Snapshot of supervisor state for the /health endpoint."""
        with self.lock:
            return {
                "status": self.status,
                "schedule": self.schedule.status(),
                "user_count": len(self.users),
                "last_refresh": self.last_refresh,
                "last_error": self.last_error,
            }
