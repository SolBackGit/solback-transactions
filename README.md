# SolBack transaction source

Compact recovery for all wallets through the website and Telegram links.

The wallet signs and sends each checked transaction, including sponsored recoveries.
Temporary lookup tables compress addresses; their deposits are recovered after use.
Progress, finalized receipts and Telegram referral attribution are preserved.
The existing 20% service fee remains unchanged.

Program: 38cMAQ5AJUiXvJd5ce6MjFGgS7wtEXoRWipGCnGnxmjV
Rust source: programs/compact-close/src/lib.rs
Library: solback_compact_close
Build information: programs/compact-close/deployment.json

Public verified-build status: pending.

Tests: python -m unittest discover -s tests -v
Compact tests: python tests/check_compact_integration.py
Browser tests: node tests/check_compact_frontend.cjs

Private keys, credentials and databases are excluded.
Existing attribution is retained in third_party/claimsol.
