"""Pure trade filtering. Returns auditable TRADE, WATCH, or SKIP decisions."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
import math
from zoneinfo import ZoneInfo

from edge import buy_edge
from probability import bracket as parse_bracket, parse_time


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
    # Weight on our model vs the market's own price (NO mid). 1.0 = ignore market.
    # The market is usually better informed than a 3-model average, so edges are
    # computed on the blend; this stops "edge" coming purely from model error.
    model_weight: float = 0.5
    # Brackets already ruled out by today's observed high ("locked NO").
    trade_locked_no: bool = True
    min_locked_net_edge: float = 0.01
    locked_slippage_per_share: float = 0.0
    # Don't bet NO on a bracket the market prices at/above this YES probability
    # (i.e. the favourite) unless the observation has locked it. Relax only after
    # evaluate_model.py shows the model beats the market.
    max_fade_market_yes: float = 0.4
    # Only trade brackets already ruled out by the observed high. Set while the
    # forecast model is not beating the market (see evaluate_model.py).
    locked_only: bool = False
    # Only trade forecasts where the best model's historical error (1 standard
    # deviation, after bias correction) is at most this many degrees C.
    max_forecast_sigma_c: float = 1.5
    # Forecast (non-locked) bets only this many days or more before the target day.
    # backtest_early.py: 2 days ahead was the best (+6-8%), 1 day ahead marginal.
    min_forecast_lead_days: int = 0
    # NO forecast bets only when the forecast high sits at least this many sigmas
    # (typical forecast errors) outside the bracket. check_losse.py: bets under 1 sigma
    # lost 5 of 9 (-$30), bets at 1+ sigma lost 3 of 41 (+$96). 0 = off.
    min_forecast_distance_sigma: float = 0.0

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
                    "max_model_market_gap", "model_weight", "min_locked_net_edge",
                    "locked_slippage_per_share", "max_fade_market_yes",
                    "max_forecast_sigma_c", "min_forecast_distance_sigma"):
            value = getattr(policy, key)
            if not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
                raise ValueError(f"{key} must be a nonnegative finite number")
        if policy.model_weight > 1:
            raise ValueError("model_weight must be between 0 and 1")
        if policy.min_models < 1:
            raise ValueError("min_models must be positive")
        return policy


def forecast_distance_sigma(state: dict, bracket_text: str) -> float | None:
    """How many sigmas the forecast high sits outside the bracket (negative = inside)."""
    parsed = parse_bracket(bracket_text)
    components = state.get("components") or []
    combined = state.get("combined")
    if not parsed or not (combined or components):
        return None
    lo, hi, _ = parsed
    if combined:
        mean, sigma = float(combined["mean"]), float(combined["sigma"])
    else:
        mean = sum(float(c["mean"]) for c in components) / len(components)
        sigma = sum(float(c.get("sigma") or 0) for c in components) / len(components)
    if sigma <= 0:
        return None
    low = -math.inf if lo is None else lo - 0.5
    high = math.inf if hi is None else hi + 0.5
    if mean < low:
        gap = low - mean
    elif mean > high:
        gap = mean - high
    else:
        gap = -min(mean - low, high - mean)
    return gap / sigma


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
              "slippage_per_share": None, "entry_cost": None, "net_edge": None,
              "market_probability": None, "blended_probability": None,
              "locked_no": bool(outcome.get("locked_no")) and side == "NO"}
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
            model_p = result["model_probability"]
            locked = bool(outcome.get("locked_no")) and side == "NO"
            result["locked_no"] = locked
            mid = (float(quote["best_bid"]) + float(quote["best_ask"])) / 2
            result["market_probability"] = mid
            if locked:
                # Physics beats the market: the observed high already rules this bracket out.
                blended = model_p
                slippage = policy.locked_slippage_per_share
                min_edge = policy.min_locked_net_edge
            else:
                blended = policy.model_weight * model_p + (1 - policy.model_weight) * mid
                slippage = policy.slippage_per_share
                min_edge = policy.min_net_edge
            result["blended_probability"] = blended
            result.update(buy_edge(blended, quote["best_ask"],
                                   policy.weather_fee_rate, slippage))
            if result["entry_cost"] >= 1:
                hard.append("COST_AT_OR_ABOVE_PAYOUT")
            elif result["net_edge"] <= 0:
                hard.append("NO_POSITIVE_NET_EDGE")
            elif result["net_edge"] < min_edge:
                soft.append("BELOW_MIN_NET_EDGE")
            if locked and not policy.trade_locked_no:
                soft.append("LOCKED_NO_DISABLED")
            if side == "NO" and not locked:
                if result["model_probability"] < policy.min_no_probability:
                    soft.append("NO_PROBABILITY_TOO_LOW")
                model_market_gap = result["model_probability"] - float(quote["best_ask"])
                result["model_market_gap"] = model_market_gap
                if model_market_gap > policy.max_model_market_gap:
                    soft.append("EXTREME_MODEL_MARKET_DIVERGENCE")
                if 1 - mid >= policy.max_fade_market_yes:
                    soft.append("FADES_MARKET_FAVOURITE")
            if policy.locked_only and not locked:
                soft.append("LOCKED_ONLY_MODE")
        except ValueError:
            hard.append("INVALID_PRICE")
    components = state.get("components", [])
    if result["locked_no"]:
        # Forecast quality is irrelevant once the observation rules the bracket out.
        components = []
    if not result["locked_no"] and len(state.get("components", [])) < policy.min_models:
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
        combined = state.get("combined")
        best_sigma_c = (float(combined["sigma"]) if combined
                        else min(float(c.get("sigma", 99)) for c in components))
        if state["unit"] == "F":
            best_sigma_c *= 5 / 9
        result["best_sigma_c"] = best_sigma_c
        if best_sigma_c > policy.max_forecast_sigma_c + 1e-9:  # tolerance: F<->C rounding
            soft.append("FORECAST_TOO_UNCERTAIN")
        if side == "NO" and not state.get("in_day_observations_modeled"):
            distance = forecast_distance_sigma(state, outcome.get("bracket") or "")
            result["forecast_distance_sigma"] = distance
            if (policy.min_forecast_distance_sigma > 0 and distance is not None
                    and distance < policy.min_forecast_distance_sigma):
                soft.append("FORECAST_TOO_CLOSE_TO_BRACKET")
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
    lead_days = (target - local_day).days
    result["lead_days"] = lead_days
    if not result["locked_no"] and lead_days < policy.min_forecast_lead_days:
        soft.append("TOO_CLOSE_TO_TARGET_DAY")
    result["reasons"] = hard + soft
    result["action"] = "SKIP" if hard else "WATCH" if soft else "TRADE"
    return result
