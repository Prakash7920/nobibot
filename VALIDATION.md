# Validation — v0.1

Completed on the supplied Linux/Python 3.12 environment:

- 15 Python unit tests passed.
- Python compilation and JavaScript syntax checks passed.
- HTTP integration: load replay, save settings, advance candles, run backtest passed.
- A mutation without the per-process request token returned HTTP 403.
- Optional pinned PyQuotex package installed successfully in an isolated virtual environment; imports and public method signatures checked against inspected source.
- Chromium UI smoke test: sample load, next candle, light/dark toggle, paper auto setting, engine start/pause, template preview, and every navigation section passed. No page-level JavaScript errors.
- Desktop light/dark renders visually inspected at 1440px. Narrow 800px layout checked: no horizontal document overflow.

Not validated:

- Windows launcher execution (batch files supplied, not run on Windows).
- Quotex authentication, exact live tick/candle matching, payout interpretation, challenge flows or long-running reconnect behavior on a user account.
- Telegram external delivery (no token or destination supplied).
- Broker order execution: no implementation is included in this release.
- Predictive quality or profitability: no such claim is made.

Tests can be rerun with `python -m unittest discover -s tests -v`.
