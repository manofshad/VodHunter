import os
import signal
import sys


def main() -> None:
    while True:
        sys.stdout.write("READY\n")
        sys.stdout.flush()
        header = sys.stdin.readline()
        if not header:
            return
        fields = dict(item.split(":", 1) for item in header.split())
        payload = sys.stdin.read(int(fields["len"]))
        event = dict(item.split(":", 1) for item in payload.split())
        sys.stdout.write("RESULT 2\nOK")
        sys.stdout.flush()
        if event.get("processname") in {"api", "nginx"}:
            print(f"Required process {event['processname']} exited; stopping container", file=sys.stderr)
            os.kill(os.getppid(), signal.SIGTERM)
            return


if __name__ == "__main__":
    main()
