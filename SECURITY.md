# Security Policy

lo2cin4bt is a local research and backtesting platform. It does not place live
orders, move funds, or require broker credentials for the supported workflow.

## Report A Vulnerability

Please do not open a public issue containing secrets, credentials, private
datasets, or exploit details. Contact the project maintainer privately and
include:

- the affected version or commit;
- a minimal reproduction;
- the expected and observed behavior;
- whether local files, network access, or credentials are involved.

## Safety Boundary

- Never commit `.env` files, API keys, broker passwords, private datasets, or
  generated run outputs.
- Treat external data and custom providers as untrusted input.
- Keep local services bound to `127.0.0.1` unless a separate security review
  explicitly approves broader exposure.
- Any future live-trading or account-mutation capability requires a separate,
  owner-approved design and security review.
