"""Massive market-data tools exposed to worker agents."""

import json
import os

import httpx
from dotenv import load_dotenv

load_dotenv()

MASSIVE_KEY = os.environ["MASSIVE_API_KEY"]
massive = httpx.AsyncClient(
    base_url="https://api.massive.com", timeout=30, params={"apiKey": MASSIVE_KEY}
)


async def massive_get(path: str, params: dict | None = None) -> dict:
    """GET a Massive API path and return the JSON, raising on HTTP errors."""
    r = await massive.get(path, params=params or {})
    r.raise_for_status()
    return r.json()


TOOLS = [
    {
        "name": "get_previous_close",
        "description": "Previous trading day's open/high/low/close/volume for a US stock or ETF ticker.",
        "input_schema": {
            "type": "object",
            "properties": {"ticker": {"type": "string", "description": "e.g. NVDA"}},
            "required": ["ticker"],
        },
    },
    {
        "name": "get_price_history",
        "description": "Daily closing prices for a ticker between two dates (YYYY-MM-DD). Returns a summary plus closes.",
        "input_schema": {
            "type": "object",
            "properties": {
                "ticker": {"type": "string"},
                "from_date": {"type": "string"},
                "to_date": {"type": "string"},
            },
            "required": ["ticker", "from_date", "to_date"],
        },
    },
    {
        "name": "get_ticker_details",
        "description": "Company/ETF reference data: name, description, market cap, sector, listing info.",
        "input_schema": {
            "type": "object",
            "properties": {"ticker": {"type": "string"}},
            "required": ["ticker"],
        },
    },
    {
        "name": "get_news",
        "description": "Recent news headlines for a ticker.",
        "input_schema": {
            "type": "object",
            "properties": {
                "ticker": {"type": "string"},
                "limit": {"type": "integer", "description": "1-10, default 5"},
            },
            "required": ["ticker"],
        },
    },
]


async def run_tool(name: str, args: dict) -> str:
    """Execute a named market-data tool and return its result as a JSON string."""
    t = str(args.get("ticker", "")).upper()
    if name == "get_previous_close":
        data = await massive_get(f"/v2/aggs/ticker/{t}/prev")
        return json.dumps(data.get("results", data))
    if name == "get_price_history":
        data = await massive_get(
            f"/v2/aggs/ticker/{t}/range/1/day/{args['from_date']}/{args['to_date']}",
            {"adjusted": "true", "sort": "asc", "limit": 5000},
        )
        bars = data.get("results") or []
        if not bars:
            return json.dumps({"ticker": t, "bars": 0})
        closes = [b["c"] for b in bars]
        out = {
            "ticker": t,
            "bars": len(bars),
            "first_close": closes[0],
            "last_close": closes[-1],
            "pct_change": round((closes[-1] / closes[0] - 1) * 100, 2),
            "high": max(b["h"] for b in bars),
            "low": min(b["l"] for b in bars),
        }
        if len(closes) <= 60:
            out["closes"] = closes
        return json.dumps(out)
    if name == "get_ticker_details":
        data = await massive_get(f"/v3/reference/tickers/{t}")
        r = data.get("results", {})
        keep = ["ticker", "name", "description", "market_cap", "sic_description", "primary_exchange", "type", "list_date"]
        return json.dumps({k: r.get(k) for k in keep})
    if name == "get_news":
        limit = max(1, min(int(args.get("limit", 5)), 10))
        data = await massive_get("/v2/reference/news", {"ticker": t, "limit": limit, "order": "desc"})
        return json.dumps(
            [
                {"title": a.get("title"), "published": a.get("published_utc"), "publisher": (a.get("publisher") or {}).get("name")}
                for a in data.get("results", [])
            ]
        )
    raise ValueError(f"unknown tool {name}")
