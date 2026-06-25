"""Static FX rates to EUR for AML normalization (demo rates)."""

FX_TO_EUR: dict[str, float] = {
    "EUR": 1.0,
    "USD": 0.92,
    "TRY": 0.027,
    "GBP": 1.17,
    "CHF": 1.05,
}

SUPPORTED_CURRENCIES = list(FX_TO_EUR.keys())


def to_eur(amount: float, currency: str) -> float:
    rate = FX_TO_EUR.get(currency.upper(), 1.0)
    return round(amount * rate, 2)
