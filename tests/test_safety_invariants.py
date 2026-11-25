"""Milestone 1 must be read-only: no key handling, no signing, no order or transfer code,
and no network destinations beyond the public read-only ones."""
import ast
import re
from pathlib import Path

import pytest

from keel.config import RecorderConfig
from keel.hl.info import InfoClient

SRC = Path(__file__).resolve().parents[1] / "src" / "keel"
FILES = sorted(SRC.rglob("*.py"))
ALLOWED_HOSTS = {"api.hyperliquid.xyz", "gamma-api.polymarket.com", "clob.polymarket.com", "ws-subscriptions-clob.polymarket.com", "api.elections.kalshi.com", "github.com"}
FORBIDDEN_IMPORTS = {"eth_account", "eth_keys", "web3", "coincurve", "ecdsa", "nacl", "cryptography", "hyperliquid", "secp256k1", "bip_utils", "mnemonic"}
FORBIDDEN_WORDS = re.compile(r"private[_ ]?key|secret|mnemonic|seed phrase|sign_l1|signTypedData|eip712|/exchange|withdraw|usdSend|spotSend|subAccountTransfer|vaultTransfer|place_order|\"order\"\s*:", re.I)


def test_no_signing_or_key_libraries_imported():
    for f in FILES:
        tree = ast.parse(f.read_text())
        for node in ast.walk(tree):
            names = []
            if isinstance(node, ast.Import):
                names = [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module:
                names = [node.module]
            for n in names:
                assert n.split(".")[0] not in FORBIDDEN_IMPORTS, f"{f.name} imports {n}"


def test_no_key_signing_order_or_transfer_code():
    for f in FILES:
        hits = FORBIDDEN_WORDS.findall(f.read_text())
        assert not hits, f"{f.name}: {hits}"


def test_only_public_read_only_hosts():
    for f in FILES:
        for host in re.findall(r"(?:https?|wss?)://([a-zA-Z0-9.-]+)", f.read_text()):
            assert host in ALLOWED_HOSTS, f"{f.name} references {host}"
    cfg = RecorderConfig()
    for r in cfg.references:
        assert re.match(r"https://([^/]+)/", r["url"]).group(1) in ALLOWED_HOSTS


def test_info_client_refuses_anything_but_info():
    with pytest.raises(ValueError):
        InfoClient("https://api.hyperliquid.xyz/exchange")
    InfoClient("https://api.hyperliquid.xyz/info")


def test_recorder_posts_only_read_only_info_types():
    src = (SRC / "recorder" / "app.py").read_text() + (SRC / "hl" / "info.py").read_text() + (SRC / "doctor.py").read_text()
    types = set(re.findall(r'"type": "([A-Za-z]+)"', src))
    assert types <= {"outcomeMeta", "l2Book", "recentTrades", "metaAndAssetCtxs", "allMids", "trades", "settledOutcome"}, types
