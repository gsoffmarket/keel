# Deploying the read-only recorder

The recorder holds no keys and sends no orders. These steps only copy code, create an
unprivileged user and run the recorder and its daily job.

## 1. Benchmark the host first (Contabo VPS or any Linux server)

From the Mac, copy the code (not the data) to the server and run a 10-minute benchmark:

    rsync -av --exclude data --exclude '__pycache__' ~/Documents/keel/ USER@SERVER:~/keel/
    ssh USER@SERVER 'cd ~/keel && python3 --version && PYTHONPATH=src python3 -m keel doctor && PYTHONPATH=src python3 -m keel bench --duration 10m'

Send back the printed benchmark table (also saved as `data/bench/reports/bench-*.md`). It
measures CPU, memory, disk growth, download volume, Hyperliquid and Polymarket latency
and reconnects with targeted fast mode and live references on.

## 2. Install as a service (after the benchmark is accepted)

    sudo useradd --system --home /opt/keel --shell /usr/sbin/nologin keel
    sudo mkdir -p /opt/keel && sudo rsync -a ~/keel/ /opt/keel/ && sudo mkdir -p /opt/keel/data
    sudo -u keel bash -c 'cd /opt/keel && PYTHONPATH=src python3 -m keel init'
    sudo chown -R keel:keel /opt/keel
    sudo cp /opt/keel/deploy/systemd/keel-*.service /opt/keel/deploy/systemd/keel-daily.timer /etc/systemd/system/
    sudo systemctl daemon-reload
    sudo systemctl enable --now keel-recorder.service keel-daily.timer

Check: `systemctl status keel-recorder`, `sudo -u keel bash -c 'cd /opt/keel && PYTHONPATH=src python3 -m keel status'`,
`journalctl -u keel-recorder -f`.

## 3. External health monitoring (optional, free)

Create a check at a monitoring service such as healthchecks.io (free tier; needs an
account, so it is the owner's choice), then set in `/opt/keel/keel.json`:

    "health_ok_url": "https://hc-ping.com/<uuid>",
    "health_fail_url": "https://hc-ping.com/<uuid>/fail"

The recorder pings every 30 s while the Hyperliquid stream is connected and delivering
data, and the fail URL otherwise; the service alerts by email if pings stop, which
also covers the machine being down. The daily job pings the fail URL if any quality
check fails.

## 4. Mac alternative

`deploy/launchd/xyz.keel.recorder.plist` (replace YOU). A laptop that sleeps produces
masked minutes, so the 7-day test belongs on an always-on host.
