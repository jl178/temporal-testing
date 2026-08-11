"""The contract service — a DIFFERENT team's code on its own queue.

The order lifecycle calls `lookup_contract` by name on
`settlement-contracts`; this module is what that fleet registers. In
production this is its own image and deployable (same recipe as every
tenant: queue + profile + registered code); the pricing team never
imports it — the queue is the interface.

Stands in for whatever system owns negotiated rates: swap the static
table for a real store or an HTTP client without touching any workflow.
"""
from temporalio import activity
from temporalio.exceptions import ApplicationError

CONTRACTS = {
    "acme": {"unit_rate": 40.0, "discount_pct": 5.0},
    "globex": {"unit_rate": 25.0, "discount_pct": 0.0},
    "initech": {"unit_rate": 12.5, "discount_pct": 10.0},
}


@activity.defn
def lookup_contract(vendor: str) -> dict:
    contract = CONTRACTS.get(vendor)
    if contract is None:
        # No contract = a business fact, not a transient fault.
        raise ApplicationError(f"no contract on file for vendor {vendor!r}",
                               non_retryable=True)
    return contract
