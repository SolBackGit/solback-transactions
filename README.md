# SolBack transaction source

Public source for SolBack recovery through the website and personal Telegram links.

Selected eligible Dust balances are burned and token accounts are closed. The compact Solana program batches account closures; burns and fee transfers are separate instructions in the same atomic transaction. Temporary address lookup tables compress larger recoveries.

The server estimates each transaction fee with getFeeForMessage. When the wallet has enough SOL, it signs and sends through signAndSendTransaction. Otherwise the wallet signs first through signTransaction; the server validates the instructions, co-signs as fee payer, simulates and submits. Recovery operations, account permissions, fee payer, blockhash and lookup addresses must match the checked quote. Supported Lighthouse assertions and bounded ComputeBudget changes are permitted; additional transfers are rejected.

Fresh scans start fresh recovery sessions. Existing personal links resume their own sessions. Uncertain signed or sent transactions block competing sends for the same wallet until reconciled. Finalized confirmations and Telegram referral attribution are retained.

## Program source

programs/compact-close contains the Rust source, Cargo lockfile and deployment metadata. Program ID: 38cMAQ5AJUiXvJd5ce6MjFGgS7wtEXoRWipGCnGnxmjV.

Publishing this source does not establish a verified reproducible build. Public build verification is a separate step.

## Offline checks

python tests/check_sponsor_baseline.py
python tests/check_fresh_sessions.py
python tests/check_sponsor_fees.py
node tests/check_sponsor_frontend.cjs

Checks use generated test keys, isolated temporary databases and mocked RPC. They do not send real transactions. Sponsored Phantom behavior must still be verified in the real wallet.

## Private configuration

Private keys, environment files, live databases, runtime state and transaction ledgers are excluded. Dependencies are listed in requirements.txt.
