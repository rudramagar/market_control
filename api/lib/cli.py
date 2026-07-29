"""
Command-line entry point for local testing against the ME.

Run from the api/ directory:
    python -m lib.cli login   -H 10.68.72.94 -u XBAND1 -p <pw>

Password may also come from the ME_PASSWORD env var instead of -p.
"""
import argparse
import sys

from .config import Settings
from .client import MercuryClient, MercuryError
from .drop import DropReader


def build_parser():
    p = argparse.ArgumentParser(prog="lib.cli", description="ME market control")
    p.add_argument("action",
                   choices=["login", "list-users", "list-entry-points",
                            "drop-snapshot", "drop-watch", "drop-live",
                            "suspend", "activate"],
                   help="command to run")
    p.add_argument("-H", "--host", help="ME host / IP")
    p.add_argument("-s", "--port", type=int, help="port (API 11005; DROP differs)")
    p.add_argument("-u", "--user", help="user, e.g. XBAND1 (API) or drop user")
    p.add_argument("-p", "--password", help="password (or ME_PASSWORD env)")
    p.add_argument("--user-id", type=int,
                   help="target user id for suspend/activate")
    p.add_argument("--max-messages", type=int, default=100000,
                   help="drop-snapshot: stop after this many messages")
    return p


def main(argv=None):
    args = build_parser().parse_args(argv)
    settings = Settings.from_env(
        host=args.host, port=args.port, user=args.user, password=args.password)

    if not settings.host or not settings.user or not settings.password:
        print("ERROR: host, user and password are required "
              "(via flags or ME_HOST/ME_USER/ME_PASSWORD env vars)")
        return 2

    # DROP feed has its own connection flow (SBE, snapshot), separate from
    # the API client used for login/list/update.
    if args.action == "drop-snapshot":
        return _drop_snapshot(settings, args.max_messages)
    if args.action == "drop-watch":
        return _drop_watch(settings)
    if args.action == "drop-live":
        return _drop_live(settings)

    client = MercuryClient(settings)
    try:
        client.connect()
        print("connected to %s:%d" % (settings.host, settings.port))

        if args.action == "login":
            accepted = client.login()
            print("LOGIN OK")
            print("  session :", accepted["session"])
            print("  sequence:", accepted["sequence"])

        elif args.action == "list-users":
            client.login()
            users = client.list_users()
            print("%d users:" % len(users))
            for u in users:
                state = "SUSPENDED" if u.is_suspended else "active"
                print("  %-6d %-16s %-8s %s"
                      % (u.user_id, u.user_name, u.firm_code, state))

        elif args.action == "list-entry-points":
            client.login()
            eps = client.list_entry_points()
            print("%d entry points:" % len(eps))
            for e in eps:
                state = "logged_on" if e.is_logged_on else "logged_off"
                print("  host=%-6d client=%-6d proto=%-3d %s"
                      % (e.host_user_id, e.client_user_id, e.protocol, state))

        elif args.action in ("suspend", "activate"):
            if args.user_id is None:
                print("ERROR: --user-id is required for %s" % args.action)
                return 2
            client.login()
            if args.action == "suspend":
                result = client.suspend(args.user_id)
            else:
                result = client.activate(args.user_id)
            if result.ok:
                print("%s OK: user %d" % (args.action.upper(), result.user_id))
            else:
                print("%s REJECTED: user %d reason=%s"
                      % (args.action.upper(), result.user_id, result.reason))
                return 1

        return 0
    except MercuryError as e:
        print("ERROR:", e)
        return 1
    finally:
        client.close()


def _drop_snapshot(settings, max_messages):
    """Connect to the DROP feed, replay into a snapshot, print users."""
    reader = DropReader(settings)
    try:
        reader.connect_and_login()
        print("connected to DROP %s:%d, replaying..." % (settings.host, settings.port))
        count = 0
        try:
            while count < max_messages:
                msg = reader.read_message()
                if msg is not None:
                    count += 1
        except (ConnectionError, OSError):
            # socket timeout / feed quiet = replay caught up
            pass
        users = reader.users()
        print("read %d messages, last ME seq %d" % (count, reader.last_seq))
        print("%d users in snapshot:" % len(users))
        for u in sorted(users, key=lambda r: r["user_id"]):
            state = reader.drop.states.get(u["state"], u["state"])
            print("  %-6d %-16s %-10s %s"
                  % (u["user_id"], u["user_name"], u.get("executing_firm", ""), state))
        return 0
    finally:
        reader.close()


def _drop_watch(settings):
    """Connect, replay to build snapshot, then STAY OPEN and print live
    changes as they stream in. Use this to test whether a suspend/activate
    done elsewhere shows up on an already-connected reader.

    Leave this running, then in another terminal:
        run.py suspend --user-id <id> -H <api_host> -s 11005 -u XBAND1 -p <pw>
    and watch whether a user_status change prints here live.
    """
    import time as _time
    reader = DropReader(settings)
    try:
        reader.connect_and_login()
        print("connected to DROP %s:%d, replaying (seq 1)..."
              % (settings.host, settings.port))
        replay_count = 0
        live = False
        while True:
            try:
                msg = reader.read_message()
            except (ConnectionError, OSError) as e:
                # During replay, a timeout means replay caught up -> go live.
                # During live, a timeout is just idle; keep waiting.
                if not live:
                    live = True
                    print("--- replay done (%d msgs, last seq %d). "
                          "%d users in snapshot. now LIVE, watching... ---"
                          % (replay_count, reader.last_seq,
                             len(reader.users())))
                    continue
                # live idle timeout: loop again to keep waiting
                continue
            if msg is None:
                continue
            kind, record = msg
            if not live:
                replay_count += 1
                # print a heartbeat every 1000 replay msgs so it's not silent
                if replay_count % 1000 == 0:
                    print("  ...replayed %d msgs" % replay_count)
                continue
            # LIVE phase: print every change as it arrives
            ts = _time.strftime("%H:%M:%S")
            if kind in ("user_status", "firm_status"):
                state = reader.drop.states.get(record["state"], record["state"])
                idf = "user_id" if kind == "user_status" else "firm_id"
                print("[%s] LIVE %s: %s=%s -> %s"
                      % (ts, kind, idf, record[idf], state))
            elif kind == "user":
                state = reader.drop.states.get(record["state"], record["state"])
                print("[%s] LIVE user record: %d %s -> %s"
                      % (ts, record["user_id"], record["user_name"], state))
            else:
                print("[%s] LIVE %s" % (ts, kind))
    except KeyboardInterrupt:
        print("\nstopped.")
        return 0
    finally:
        reader.close()


def _drop_live(settings):
    """Connect, replay, then stream live changes on a held connection
    (with client heartbeats). This is the real display path.

    Leave running, then suspend/activate a user from another terminal -
    the change should print here instantly.
    """
    import time as _time
    reader = DropReader(settings)

    def on_replay_done(users):
        print("--- replay done: %d users in snapshot. now LIVE (held connection "
              "+ heartbeats), watching... ---" % len(users))

    def on_change(kind, record, users):
        ts = _time.strftime("%H:%M:%S")
        if kind in ("user_status", "firm_status"):
            state = reader.drop.states.get(record["state"], record["state"])
            idf = "user_id" if kind == "user_status" else "firm_id"
            print("[%s] LIVE %s: %s=%s -> %s"
                  % (ts, kind, idf, record[idf], state))
        elif kind == "user":
            state = reader.drop.states.get(record["state"], record["state"])
            print("[%s] LIVE user: %d %s -> %s"
                  % (ts, record["user_id"], record["user_name"], state))
        else:
            print("[%s] LIVE %s" % (ts, kind))

    print("connecting to DROP %s:%d..." % (settings.host, settings.port))
    try:
        reader.run_live(on_replay_done=on_replay_done, on_change=on_change,
                        debug=True)
        return 0
    except KeyboardInterrupt:
        print("\nstopped.")
        return 0
    except (ConnectionError, OSError) as e:
        print("connection ended:", e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
