"""Pure trade filtering. Returns auditable TRADE, WATCH, or SKIP decisions."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
import math
from zoneinfo import ZoneInfo

from edge import buy_edge
from probability import parse_time


@dataclass(frozen=True)
class Policy:
    allowed_sides: tuple[str, ...] = ("YES", "NO")
    min_net_edge: float = 0.06
    max_spread: float = 0.05
    min_ask_depth: float = 1.0
    max_book_age_seconds: float = 120.0
    max_forecast_age_hours: float = 12.0
    max_model_spread_c: float = 3.0
    min_models: int = 3
    min_calibration_days: int = 14
    require_verified_rules: bool = True
    watch_on_target_day: bool = True
    weather_fee_rate: float = 0.05
    slippage_per_share: float = 0.01
    min_no_probability: float = 0.0
    max_model_market_gap: float = 1.0

    @classmethod
    def from_config(cls, cfg: dict) -> "Policy":
        overrides = cfg.get("decision", {})
        unknown = set(overrides) - set(cls.__dataclass_fields__)
        if unknown:
            raise ValueError(f"Unknown decision settings: {sorted(unknown)}")
        if "allowed_sides" in overrides:
            overrides = {**overrides, "allowed_sides": tuple(overrides["allowed_sides"])}
        policy = cls(**overrides)
        if not policy.allowed_sides or set(policy.allowed_sides) - {"YES", "NO"}:
            raise ValueError("allowed_sides must contain YES and/or NO")
        for key in ("min_net_edge", "max_spread", "min_ask_depth", "max_book_age_seconds",
                    "max_forecast_age_hours", "max_model_spread_c", "min_calibration_days",
                    "weather_fee_rate", "slippage_per_share", "min_no_probability",
                    "max_model_market_gap"):
            value = getattr(policy, key)
            if not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
                raise ValueError(f"{key} must be a nonnegative finite number")
        if policy.min_models < 1:
            raise ValueError("min_models must be positive")
        return policy


def assess(state: dict, outcome: dict, side: str, quote: dict | None,
           as_of: datetime, timezone_name: str, rules_verified: bool,
           policy: Policy) -> dict:
    """No DB/network calls, no orders, no assumptions about position sizing."""
    result = {"event_id": state["event_id"], "market_id": outcome["market_id"],
              "side": side, "as_of": as_of.isoformat(), "action": "SKIP",
              "reasons": [], "model_probability": outcome["p_yes"] if side == "YES" else outcome["p_no"],
              "best_bid": None, "best_ask": None, "ask_size": None, "ask_levels": None,
              "book_age_seconds": None, "quote_source": None, "depth_verified": False,
              "raw_edge": None, "fee_per_share": None,
              "slippage_per_share": None, "entry_cost": None, "net_edge": None}
    hard, soft = [], []
    if side not in policy.allowed_sides:
        hard.append("SIDE_DISABLED")
    if quote is None:
        hard.append("NO_RECENT_QUOTE")
    else:
        for key in ("best_bid", "best_ask", "ask_size", "book_age_seconds"):
            result[key] = quote[key]
        result["quote_source"] = quote.get("quote_source")
        result["ask_levels"] = quote.get("ask_levels")
        result["depth_verified"] = bool(quote.get("depth_verified", quote.get("ask_size") is not None))
        if quote["book_age_seconds"] > policy.max_book_age_seconds:
            hard.append("STALE_BOOK")
        if quote["best_ask"] - quote["best_bid"] > policy.max_spread:
            hard.append("WIDE_SPREAD")
        if quote["ask_size"] is None:
            soft.append("DEPTH_UNVERIFIED")
        elif quote["ask_size"] < policy.min_ask_depth:
            hard.append("LOW_DEPTH")
        try:
            result.update(buy_edge(result["model_probability"], quote["best_ask"],
                                   policy.weather_fee_rate, policy.slippage_per_share))
            if result["entry_cost"] >= 1:
                hard.append("COST_AT_OR_ABOVE_PAYOUT")
            elif result["net_edge"] <= 0:
                hard.append("NO_POSITIVE_NET_EDGE")
            elif result["net_edge"] < policy.min_net_edge:
                soft.append("BELOW_MIN_NET_EDGE")
            if side == "NO":
                if result["model_probability"] < policy.min_no_probability:
                    soft.append("NO_PROBABILITY_TOO_LOW")
                model_market_gap = result["model_probability"] - float(quote["best_ask"])
                result["model_market_gap"] = model_market_gap
                if model_market_gap > policy.max_model_market_gap:
                    soft.append("EXTREME_MODEL_MARKET_DIVERGENCE")
        except ValueError:
            hard.append("INVALID_PRICE")
    components = state.get("components", [])
    if len(components) < policy.min_models:
        hard.append("MISSING_WEATHER_MODELS")
    elif components:
        means = [float(c["mean"]) for c in components]
        spread_c = max(means) - min(means)
        if state["unit"] == "F":
            spread_c *= 5 / 9
        result["model_spread_c"] = spread_c
        if spread_c > policy.max_model_spread_c:
            soft.append("MODEL_DISAGREEMENT")
        if any(int(c.get("calibration_days", 0)) < policy.min_calibration_days for c in components):
            soft.append("UNCALIBRATED_MODELS")
        if any((as_of - parse_time(c["fetched_at"])).total_seconds() >
               policy.max_forecast_age_hours * 3600 for c in components):
            hard.append("STALE_FORECAST")
    if policy.require_verified_rules and not rules_verified:
        soft.append("RULES_UNVERIFIED")
    local_day = as_of.astimezone(ZoneInfo(timezone_name)).date()
    target = date.fromisoformat(state["target_date"])
    if local_day > target:
        hard.append("EVENT_DAY_OVER")
    elif policy.watch_on_target_day and local_day == target and not state.get("in_day_observations_modeled"):
        soft.append("IN_DAY_OBSERVATIONS_NOT_MODELED")
    result["reasons"] = hard + soft
    result["action"] = "SKIP" if hard else "WATCH" if soft else "TRADE"
    return result
