#!/usr/bin/env bash
# Provisions a Codespace into a working SDP install: copies pre-baked configs
# from tests/demo/ into the workspace, pre-creates data dirs, installs sdp via
# pipx. Idempotent — re-running skips files that already exist.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

DEMO="$REPO_ROOT/tests/demo"

# Bind-mounted host docker socket; relax permissions so the non-root vscode
# user can talk to the daemon without matching the host's docker group GID.
sudo chmod 666 /var/run/docker.sock 2>/dev/null || true

copy_if_missing() {
    local src="$1"
    local dst="$2"
    if [ ! -f "$dst" ]; then
        mkdir -p "$(dirname "$dst")"
        cp "$src" "$dst"
        echo "  [seed] $dst"
    fi
}

echo "[1/4] Seeding workspace from tests/demo/..."
copy_if_missing "$DEMO/env.template" .env
copy_if_missing "$DEMO/config/db/postgres.yaml" config/db/postgres.yaml
copy_if_missing "$DEMO/config/db/mongo.yaml" config/db/mongo.yaml
copy_if_missing "$DEMO/config/db/starrocks.yaml" config/db/starrocks.yaml
copy_if_missing "$DEMO/config/db/mcp.yaml" config/db/mcp.yaml
copy_if_missing "$DEMO/config/jobs/config.local.yaml" config/jobs/config.local.yaml
copy_if_missing "$DEMO/docker-compose.override.yml" docker-compose.override.yml
copy_if_missing "$DEMO/config/starrocks/fe.local.conf" config/starrocks/fe.local.conf
copy_if_missing "$DEMO/config/starrocks/be.local.conf" config/starrocks/be.local.conf
copy_if_missing "$DEMO/config/sources/reddit/platform.yaml" config/sources/reddit/platform.yaml
copy_if_missing "$DEMO/config/sources/reddit/parse.yaml" config/sources/reddit/parse.yaml
copy_if_missing "$DEMO/config/sources/reddit/postgres.yaml" config/sources/reddit/postgres.yaml
copy_if_missing "$DEMO/config/sources/reddit/mongo.yaml" config/sources/reddit/mongo.yaml
copy_if_missing "$DEMO/config/sources/reddit/starrocks.yaml" config/sources/reddit/starrocks.yaml
copy_if_missing "$DEMO/config/sources/reddit/lingua.yaml" config/sources/reddit/lingua.yaml
copy_if_missing "$DEMO/vscode/mcp.json" .vscode/mcp.json

# Pre-create host data dirs so the docker daemon doesn't auto-create them
# as root:root the first time `db start` runs a bind mount.
echo "[2/4] Pre-creating data directories..."
for d in \
    data/dumps/reddit \
    data/extracted/reddit \
    data/parsed/reddit \
    data/output/reddit \
    data/database/postgres \
    data/database/mongo \
    data/database/starrocks \
    data/jobs-results; do
    mkdir -p "$d"
done

echo "[3/4] Installing pipx if missing..."
if ! command -v pipx >/dev/null 2>&1; then
    python3 -m pip install --user --quiet pipx
fi
python3 -m pipx ensurepath >/dev/null 2>&1 || true
export PATH="$HOME/.local/bin:$PATH"

echo "[4/4] Installing sdp (editable)..."
pipx install --editable --force "$REPO_ROOT" >/dev/null
echo "  sdp -> $(pipx list --short 2>/dev/null | grep '^social-data-pipeline' || echo 'installed')"

cat <<'EOF'

  Codespace ready. tests/demo/WELCOME.md should auto-open.
  Quick path: `sdp db start postgres`, then bring data and run the pipeline.

EOF
