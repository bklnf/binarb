# Commission model and validation

The scanner previously deducted every fee from the received asset. With
BNB-paid commissions this reduced the simulated intermediate quantity even
though Binance would leave it intact. A small deduction from a whole lot could
therefore prevent selling that lot on the next leg. Fee rates could also differ
from the startup account estimate, particularly for symbol promotions.

The implementation now reads per-symbol account commission profiles, adds
taker plus buyer/seller charges, and refreshes side-specific order/test profiles
before entry. Order/test already includes the side, so it is not added twice.
Only standard commissions receive the reported BNB multiplier. These semantics
follow the [Binance commission FAQ](https://developers.binance.com/en/docs/products/spot/faqs/commission_faq)
and [account commission endpoint](https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/rest-api/account).
A read-only live check on September 6, 2026 returned standard taker rates of
0.001 for BTCUSDT and 0.00095 for ZECUSDC, with a 0.75 BNB multiplier on both.
Those observations are account-specific, not permanent exchange-wide rates.

The TKG reference at `/home/bk1nf/containers/tkg` was inspected for its treatment
of commissions by asset. No source code was copied. Its fill accounting separates
source, destination, and third-asset fees; its simplified screening and reported
results alone do not establish a profitable strategy.

Simulation keeps physical inventory and economic fees distinct:

- Received-asset commissions reduce the next leg's output.
- Source-asset commissions reserve part of the order budget before rounding.
- External BNB commissions preserve the route output, require an existing
  reserve, and reduce economic profit using a conservative replacement value.
- BNB-start routes reserve external fees outside the selected trading amount.
  Zero BNB availability uses full received-asset commissions. Insufficient
  positive BNB reserves reject that size rather than assuming a discount.
- Fresh route books and one available BNB bridge determine fee conversion and
  valuation. Missing paths reject the candidate. Both conversions include a
  10 bps allowance; fees are rounded up to eight decimal places. Intermediate
  residual assets receive no liquidation credit.

The live per-symbol rate cache expires after 60 seconds and allows at most 30
refreshes per minute. `COMMISSION_REFRESH_BUDGET` means confirmation was deferred,
not that the market was unprofitable. Gross-price screening avoids excluding
routes using stale estimated fees; exact depth confirmation still enforces the
configured net-profit threshold. Price-disagreement guards now compare prices
without mixing in commission, size, or rounding changes.

Before every external-fee order attempt, the executor checks the available fee
asset again. Actual fill commissions remain authoritative. If the fee asset
changes, the confirmed fill is persisted before recovery. Realized external-fee
cost uses the stored pre-entry replacement valuation when available; this is a
stated cash-flow valuation basis, not account-wide mark-to-market PnL.

The regression fixture buys 0.009 units at 1000, sells the same 0.009 at 1004,
then converts at 1, with 7.5 bps fees and BNB valued at 600. The historical model
loses almost a whole 0.001 lot; the corrected model keeps it and yields more than
0.015 start units after conservative BNB valuation. An execution fake verifies
that all three fills preserve those quantities and recorded net PnL agrees with
the simulation. This establishes correctness of that fixture, not a historical
missed trade or guaranteed live profitability.

Private bounded captures support deterministic comparisons on the same books,
fee rates, reserve, and size grid. The received-asset replay intentionally keeps
the newly observed rates so that the comparison isolates fee-asset propagation;
it is not a replay of the entire historical strategy. A separate observation-only
sampler can examine ticker signals up to 30 seconds old while enforcing the
unchanged live depth freshness checks. Captures identify the original strict
ticker rejection and price disagreement. Neither replay nor sampling executes
orders. Capture retention is applied when a new capture is written.

Pre-deployment live validation on September 6, 2026 produced 14 depth
confirmations from 38 requests in a 60-second observation sample, with zero
eligible opportunities. Three retained BONK route captures had stale ticker
timestamps but unchanged reference prices; both models remained unprofitable
(about -20.6 to -24.8 bps). This sample does not justify relaxing live freshness.

Two targeted ZEC route captures used identical books and actual free-balance
sizing for both models. USDC → USDT → ZEC → USDC improved from -1097.8653 to
-21.1274 bps after removing artificial lot loss. USDT → ZEC → USDC → USDT changed
from all sizes failing pair rules to a best valid return of -170.1244 bps.
Neither was eligible. All six order/test profiles matched the account profiles,
including zero standard fees on USDCUSDT. These are controlled accounting
comparisons, not realized profits or a throughput benchmark. Raw captures remain
local under `/tmp/binarb-commission-shadow` and `/tmp/binarb-commission-targeted`.
