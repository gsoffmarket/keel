# Security policy

## Scope

Keel's released code is read-only: it holds no private keys, has no signing code and never calls Hyperliquid's exchange endpoint. Security reports are still welcome, in particular about:

- anything that could make the recorder or research code sign, send or cancel orders, or reach an exchange endpoint;
- the Health page (it must bind to loopback only and serve only GET);
- path handling in storage, compaction and retention (deletion must be limited to verified data inside the data folder);
- supply-chain issues in the CI workflow or optional dependencies.

## Reporting

Please report privately through [GitHub security advisories](https://github.com/gsoffmarket/keel/security/advisories/new), not in a public issue. Include steps to reproduce and the affected version or commit. You should receive an acknowledgement within 72 hours.

## Operator guidance

- Never put keys, seed phrases or API secrets in `keel.json`, the data folder or an issue. Keel does not need any.
- Keep the Health page on loopback and use an SSH tunnel to view it remotely.
- Run the recorder as an unprivileged user; the systemd unit in `deploy/` applies sandboxing.
