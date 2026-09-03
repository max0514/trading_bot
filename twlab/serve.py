"""Publish the materialized store over HTTP for research machines.

    python -m twlab.serve --store ~/.twlab/store --port 8787

A read-only static file server: clients set TWLAB_SERVER_URL=http://<host>:8787
and `data.get()` syncs frames through its local cache. In Docker Compose the
same role is played by the `store` (nginx) service.
"""
from __future__ import annotations

import argparse
import functools
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer

from twlab import config


class _Handler(SimpleHTTPRequestHandler):
    def do_POST(self):  # noqa: N802 — read-only
        self.send_error(405)

    def log_message(self, fmt, *args):  # quieter default log
        print(f"{self.address_string()} {fmt % args}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Serve the twlab Parquet store read-only")
    parser.add_argument("--store", default=str(config.store_dir()))
    parser.add_argument("--bind", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8787)
    args = parser.parse_args(argv)
    handler = functools.partial(_Handler, directory=args.store)
    server = ThreadingHTTPServer((args.bind, args.port), handler)
    print(f"serving {args.store} on http://{args.bind}:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
