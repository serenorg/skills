"""Mode scoring engine for Kraken Money Mode Router."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Tuple


MODE_COPY: Dict[str, Dict[str, Any]] = {
    "payments": {
        "label": "Krak Payments",
        "summary": "Use Kraken for everyday money movement and fast transfers.",
        "actions": [
            "Open Krak and complete profile setup.",
            "Add primary funding method and test a small transfer.",
            "Set your default send path for repeat payments.",
            "Enable alerts for inbound and outbound transfers."
        ]
    },
    "investing": {
        "label": "Multi-Asset Investing",
        "summary": "Build a diversified portfolio across available Kraken asset classes.",
        "actions": [
            "Define your target allocation and rebalance cadence.",
            "Fund your base portfolio positions in your core assets.",
            "Set recurring buys for long-term compounding.",
            "Track drift and rebalance on schedule."
        ]
    },
    "active-trading": {
        "label": "Active Trading",
        "summary": "Execute directional or tactical trades with high control.",
        "actions": [
            "Select a tight watchlist and define setup criteria.",
            "Set order templates with entry, stop, and take-profit levels.",
            "Run one session with strict sizing and journal every trade.",
            "Review execution quality after each session."
        ]
    },
    "onchain": {
        "label": "On-Chain Operations",
        "summary": "Use Kraken funding and wallet APIs to move assets on-chain.",
        "actions": [
            "Select asset/network using DepositMethods and WithdrawMethods.",
            "Create or verify destination details with DepositAddresses or WithdrawInfo.",
            "Submit a controlled transfer using Withdraw and track status until settlement.",
            "Use WalletTransfer for internal account routing when available."
        ]
    },
    "automation": {
        "label": "Automation",
        "summary": "Automate recurring execution so your strategy runs consistently.",
        "actions": [
            "Choose one repeatable strategy to automate first.",
            "Define entry/exit logic, sizing rules, and cooldown intervals.",
            "Run a dry-run cycle and confirm expected order behavior.",
            "Deploy with monitoring and weekly parameter review."
        ]
    }
}


@dataclass
class Recommendation:
    mode_id: str
    score: float
    label: str
    summary: str
    reasons: List[str]


class ModeEngine:
    """Scores user intent against Kraken product modes."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.mode_order: List[str] = config.get("mode_order", list(MODE_COPY.keys()))
        self.weights: Dict[str, Dict[str, Dict[str, float]]] = config.get("score_weights", {})
        self.publisher_requirements: Dict[str, List[str]] = config.get("publisher_requirements", {})
        self.mode_endpoint_catalog: Dict[str, List[Dict[str, str]]] = config.get("mode_endpoint_catalog", {})

    def recommend(self, answers: Dict[str, str]) -> Tuple[List[Recommendation], Dict[str, Any]]:
        available_publishers = set(self.config.get("available_publishers", []))
        supported_modes, hidden_modes = self._resolve_mode_support(available_publishers)

        modes_to_score = supported_modes if supported_modes else list(self.mode_order)
        scores = {mode_id: 0.0 for mode_id in modes_to_score}
        reasons_by_mode: Dict[str, List[str]] = {mode_id: [] for mode_id in self.mode_order}

        for dimension, answer_value in answers.items():
            mappings = self.weights.get(dimension, {})
            mode_boosts = mappings.get(answer_value, {})
            for mode_id, boost in mode_boosts.items():
                if mode_id not in scores:
                    continue
                scores[mode_id] += float(boost)
                reasons_by_mode[mode_id].append(
                    f"{dimension}={answer_value} (+{boost:g})"
                )

        ranked = sorted(scores.items(), key=lambda item: item[1], reverse=True)
        recommendations: List[Recommendation] = []
        for mode_id, score in ranked:
            mode_copy = MODE_COPY.get(mode_id, {})
            recommendations.append(
                Recommendation(
                    mode_id=mode_id,
                    score=score,
                    label=mode_copy.get("label", mode_id),
                    summary=mode_copy.get("summary", ""),
                    reasons=reasons_by_mode.get(mode_id, [])[:4],
                )
            )

        gap_report = self._build_gap_report(
            available_publishers=available_publishers,
            supported_modes=modes_to_score,
            hidden_modes=hidden_modes,
        )
        return recommendations, gap_report

    def build_action_plan(self, mode_id: str) -> List[str]:
        return MODE_COPY.get(mode_id, {}).get("actions", [])

    def _build_gap_report(
        self,
        available_publishers: set[str],
        supported_modes: List[str],
        hidden_modes: Dict[str, List[str]],
    ) -> Dict[str, Any]:
        return {
            "available_publishers": sorted(available_publishers),
            "supported_modes": supported_modes,
            "hidden_modes": hidden_modes,
            "publisher_requirements": self.publisher_requirements,
            "supported_mode_endpoints": {
                mode_id: self.mode_endpoint_catalog.get(mode_id, [])
                for mode_id in supported_modes
            },
        }

    def _resolve_mode_support(
        self,
        available_publishers: set[str],
    ) -> Tuple[List[str], Dict[str, List[str]]]:
        supported: List[str] = []
        hidden: Dict[str, List[str]] = {}

        for mode_id in self.mode_order:
            required = self.publisher_requirements.get(mode_id, [])
            missing = [slug for slug in required if slug not in available_publishers]
            if missing:
                hidden[mode_id] = missing
            else:
                supported.append(mode_id)

        return supported, hidden
