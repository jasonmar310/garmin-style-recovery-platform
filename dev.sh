# source dev.sh
source .venv/bin/activate
set -a; source .env; set +a
echo "ready: venv + env (PGPORT=$PGPORT, user=$TIMESCALE_USER)"
