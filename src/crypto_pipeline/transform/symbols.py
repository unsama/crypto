"""Best-effort exchange-native symbol -> unified `BASE-QUOTE` normalization.

This is a static lookup, not a call to each exchange's instruments/assets
endpoint, so it will not correctly split every symbol - particularly
Kraken's legacy asset codes and any newly-listed base asset that happens
to share a prefix with a known quote asset. Known gaps are called out
inline; extend the tables here as new symbols are ingested rather than
trying to special-case every asset globally.
"""

from __future__ import annotations

# Ordered longest-first so e.g. "USDT" is tried before "USD".
_KNOWN_QUOTES = ["USDT", "USDC", "BUSD", "TUSD", "FDUSD", "USD", "EUR", "GBP", "BTC", "ETH"]

# Kraken's legacy ("X"/"Z"-prefixed) asset codes for the pairs we ingest.
# Source: Kraken AssetPairs `altname` mapping, hardcoded for the common
# majors rather than fetched dynamically from the AssetPairs endpoint.
_KRAKEN_ASSET_ALIASES = {
    "XXBT": "BTC",
    "XXDG": "DOGE",
    "XETH": "ETH",
    "XBT": "BTC",
    "XDG": "DOGE",
    "ZUSD": "USD",
    "ZEUR": "EUR",
    "ZGBP": "GBP",
    "ZJPY": "JPY",
    "ZCAD": "CAD",
}


def normalize_symbol(raw_symbol: str, exchange: str) -> str:
    """Best-effort `RAWSYMBOL` -> `BASE-QUOTE` normalization for the given exchange."""
    symbol = raw_symbol.upper()

    if exchange == "kraken":
        symbol = _dealias_kraken(symbol)

    if "-" in symbol or "/" in symbol:
        base, _, quote = symbol.replace("/", "-").partition("-")
        return f"{base}-{quote}"

    for quote in _KNOWN_QUOTES:
        if symbol.endswith(quote) and len(symbol) > len(quote):
            base = symbol[: -len(quote)]
            return f"{base}-{quote}"

    # No known quote suffix matched; return unchanged rather than guess.
    return symbol


def _dealias_one(segment: str) -> str:
    for alias, canonical in sorted(_KRAKEN_ASSET_ALIASES.items(), key=lambda kv: -len(kv[0])):
        if segment.startswith(alias):
            return canonical + segment[len(alias) :]
    return segment


def _dealias_kraken(symbol: str) -> str:
    """De-alias the base-asset prefix, then the quote-asset prefix of what's left.

    e.g. "XXBTZUSD" -> base "XXBT"->"BTC", remainder "ZUSD"->"USD" -> "BTCUSD".
    """
    for alias, canonical in sorted(_KRAKEN_ASSET_ALIASES.items(), key=lambda kv: -len(kv[0])):
        if symbol.startswith(alias):
            remainder = symbol[len(alias) :]
            return canonical + _dealias_one(remainder)
    return symbol
