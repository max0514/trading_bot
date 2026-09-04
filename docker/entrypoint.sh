#!/bin/sh
# twlab container entrypoint.
#
#   cron            → schedule the Orchestrator (nightly run, weekly Witness) and
#                     stay in the foreground; this is the `orchestrator` service
#   anything else   → run it (e.g. `python -m twlab.orchestrator status`)
set -e

if [ -n "$TZ" ] && [ -f "/usr/share/zoneinfo/$TZ" ]; then
    ln -sf "/usr/share/zoneinfo/$TZ" /etc/localtime
    echo "$TZ" > /etc/timezone
fi

if [ "$1" = "cron" ]; then
    RUN_SCHEDULE="${TWLAB_CRON_RUN:-30 22 * * *}"
    WITNESS_SCHEDULE="${TWLAB_CRON_WITNESS:-30 23 * * 0}"
    MAX_CATCHUP="${TWLAB_MAX_CATCHUP:-14}"

    # cron jobs do not inherit the container environment: persist it for them,
    # shell-quoted so URIs with passwords or odd characters survive intact.
    python3 - > /etc/twlab.env <<'PY'
import os, shlex
for key, value in sorted(os.environ.items()):
    if key == "TZ" or key.startswith(("MONGODB_URI", "TWLAB_", "FINMIND_")):
        print(f"export {key}={shlex.quote(value)}")
PY
    chmod 0600 /etc/twlab.env

    cat > /etc/cron.d/twlab <<EOF
SHELL=/bin/sh
PATH=/usr/local/bin:/usr/bin:/bin
$RUN_SCHEDULE root . /etc/twlab.env; cd /app && python -m twlab.orchestrator run --max-catchup $MAX_CATCHUP >> /var/log/twlab/orchestrator.log 2>&1
$WITNESS_SCHEDULE root . /etc/twlab.env; cd /app && python -m twlab.orchestrator witness >> /var/log/twlab/witness.log 2>&1
EOF
    chmod 0644 /etc/cron.d/twlab
    touch /var/log/twlab/orchestrator.log /var/log/twlab/witness.log

    echo "twlab orchestrator: run at '$RUN_SCHEDULE', witness at '$WITNESS_SCHEDULE' (TZ=${TZ:-UTC})"
    echo "current status:"
    python -m twlab.orchestrator status || true

    tail -n 0 -F /var/log/twlab/orchestrator.log /var/log/twlab/witness.log &
    exec cron -f
fi

exec "$@"
