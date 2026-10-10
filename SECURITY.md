# Security scope

Report issues privately through https://t.me/SolBackSupport. Never post credentials, private keys or unfinished-session tokens.

The backend checks wallet ownership, selected eligible accounts, exact signed operations and account permissions, the service-fee recipient, fee payer and blockhash. Only the existing narrow Lighthouse read assertions are allowed in addition to the checked message. A sponsor signs only after verifying the owner signature, checking the exact quoted message. Current eligibility and signed simulations are checked before broadcasting.

Wallet direct-send signatures are reconciled against the actual executed transaction before progress or recovery is credited. The entire approved list is durably saved before background server broadcast. Unknown wallet-send results are matched to executed messages for the whole list. Unknown submissions block replacement until confirmation or checked expiry.

This release candidate has offline tests. Real-wallet testing, especially Phantom mobile and multiple-transaction warnings, remains required. No Phantom approval is claimed. Protected stablecoins use an explicit mint registry; unknown prices remain excluded. Prices are preparation snapshots, not guaranteed executable sale values.

Private configuration, databases, sponsor keys, Telegram bot source and credentials must never be committed.

The default VPS updater restricts recovery approval to configured owner test wallets; read-only scans remain public. v0 messages use no address lookup tables, and wallet-added lookup tables are refused.
