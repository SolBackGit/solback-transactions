# SolBack Web V2 — recovery candidate

Separate web service, port 8790, mounted at `/web-test/`. The existing Telegram bot, claim server and home are not replaced. This package contains real transaction construction and submission, not a mock claim button. It has passed offline checks; real wallet approval and Phantom warning behavior still require a wallet test before a general launch.

## Flow

1. Scan any wallet without connecting: initial empty-account and remaining-balance deposit estimate.
2. A progressive Jupiter / launchpad / Raydium / Meteora pipeline determines eligible Dust. Strictly less than $0.50 total wallet value per mint. Unknown prices, protected assets, frozen accounts, unsupported extensions, NFT-like balances and active mint authority are excluded. Dust arrives selected; users may deselect.
3. Connect the owner wallet; sign a nonce-bound, five-minute ownership message. The message transfers nothing. Selection becomes a resumable recovery session.
4. Prepare fresh inventory checks and simulated packets: empty accounts first, then BurnChecked plus CloseAccount. Up to ten empty accounts or five burn accounts per versioned (v0) transaction; actual wire size is capped at 850 bytes, splitting further when required. The ENTIRE list is prepared and passed to the wallet together. Empty-account packets come before burn packets in the list. Submission and confirmation order are not guaranteed by a direct-send wallet.
5. Review checked net recovery including the full 20% service fee, paid to the configured treasury without referral attribution, and network fees. Approve in the wallet. Unsponsored Phantom uses a single signAndSendAllTransactions call for the entire checked list. Sponsored recovery uses one wallet-first signAllTransactions call for that list, followed by strict server verification, sponsor cosigning, simulation and broadcast. A separate ownership-message signature still comes first. An explicit one-at-a-time fallback remains available; it is never silently selected after a grouped-signing error.
6. Save each confirmed result, show progress, real net SOL/USD and explorer links. Resume in the same browser or through the URL fragment containing the private session token. Confirmation is checked before generating another transaction; an uncertain submission is never blindly replaced.

Prices are calculated in preparation and are not fetched again before each approval. Current ownership, balances, mint properties and whole-mint quantities are rechecked before constructing a packet and before server submission. The checked preparation price is used for this balance check. New scans do not use a price cache. Resume preserves the chosen price snapshot; it is not a promise that a market price remains unchanged.

All packets receive one fresh blockhash after construction/simulation. Before wallet approval, unsigned blockhashes are refreshed after 25 seconds without repeating price discovery or silently changing the quote. A transaction already displayed in a wallet still has Solana's normal blockhash lifetime; this cannot be removed. An expired unsigned quote can be rebuilt, but a sent transaction is first reconciled. Canceling a direct wallet-send batch may require waiting for reconciliation because the wallet could have submitted part of it.

## Configuration

Private server environment:
- SOLBACK_TEST_RPC_URL
- JUPITER_API_KEY
- SOLBACK_WEB_TREASURY: service-fee recipient
- SOLBACK_WEB_SPONSOR_PATH: existing sponsor key file; no wallet is generated
- SOLBACK_WEB_STATE: private directory for recovery.sqlite3
- SOLBACK_WEB_TEST_WALLETS: space-separated wallets allowed to create/approve recovery; updater defaults to the existing owner test wallet Co7NARbxwbUCX6KHD2yCjdcXFUvxjoYvFX5unZqEhvyk. Read-only scans remain public. An empty value disables this trial restriction only when intentionally configured.
- SOLBACK_TEST_PROTECTED_MINTS: optional additional protected mint addresses

The updater structurally reads the existing treasury and privately uses the existing sponsor path. It never imports the Telegram bot, publishes its credentials or copies its database. It creates an isolated durable state directory, a web-service-only write exception and raises only the `/web-test/` request-body limit to 4 MB for the complete signed list. Previous web files and private config are backed up. The original home and bot process remain running.

Recovery sessions persist across service restarts. Unfinished scan/preparation sessions are temporary and require a new scan after restarting. The URL session token must be treated as private; it exposes progress and quotes but does not permit a transaction without a valid owner signature.

## Validation and release status

Run `python tests/test_recovery.py`, `python tests/test_preparation.py` when included, and `node tests/test_frontend.cjs`. Offline mocks cover grouping, 20% fees, changed instructions/permissions, wallet signature verification, sponsor order, duplicate submission, expiry, lost wallet-send results, confirmation, whole-mint threshold and progressive preparation.

No real transaction is submitted by these checks. These checks do not certify that Phantom removes a warning. The public repository must contain the final tested transaction code before requesting Phantom review.

Bundled browser dependency: @solana/web3.js 1.98.4; upstream license in static/solana.LICENSE. No external runtime script CDN is needed.

## Protected assets and presentation

Exact mint registry: WSOL, USDC, USDT, CASH, PYUSD, USDG, EURC, USDS. It does not identify every bridged stablecoin variant. Empty protected-mint accounts may be closed. SOL itself is never burned. Names use on-chain Token-2022 or Metaplex metadata. USD conversions are approximate. Each checked wallet quote is authoritative over the initial deposit estimate.

Three cards show empty accounts, selected Dust and total net recovery. X: https://x.com/SolBack_X; Telegram: https://t.me/SolBackSupport. Referral advertising links to the existing Telegram referral program. Direct web claims carry no referral attribution.

## Upstream adaptation

The instruction functions ix_close and ix_burn are adapted from https://github.com/SimuZSkyNeT/claimsol-tx at commit c8e9ba7afdb7800eda9afca2338a5e6d3e1a6440. Upstream MIT license and attribution are retained in third_party/claimsol. We also follow its versioned-message construction, group sizes, 850-byte cap and full-list Phantom signing pattern. The pricing pipeline, strict wallet-value threshold, protected-asset checks, service-fee allocation, durable ledger and submission validation remain SolBack's implementation. Swaps and NFT burns from upstream are not enabled. Upstream's extra sponsor percentage is not adopted: SolBack charges 20% plus reimbursement of quoted network fees.

Large full batches are built asynchronously with progress updates. No incomplete subset is exposed to the wallet. Signed bytes are durably persisted before background submission; signature statuses are read in batches up to 256. If the wallet loses the direct-send response, matching on-chain messages are sought for the whole batch before rebuilding anything.

A single API invocation does not guarantee a particular number of Phantom screens or successful execution of arbitrarily large lists. RPC delays, wallet batch limits and Solana blockhash expiry can require resuming unconfirmed packets with another approval. The builder rejects more than 2048 transactions instead of silently approving only a subset. Real Phantom desktop/mobile tests remain outstanding.
