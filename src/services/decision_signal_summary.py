# -*- coding: utf-8 -*-
"""Low-sensitive DecisionSignal summaries for notifications and risk views."""

from __future__ import annotations

from typing import Any, Dict, Optional

from src.utils.sanitize import sanitize_decision_signal_payload, sanitize_decision_signal_text


SUMMARY_FIELDS = (
    "id",
    "stock_code",
    "stock_name",
    "market",
    "action",
    "action_label",
    "account_action",
    "policy_mode",
    "policy_decision",
    "would_block",
    "policy_reasons",
    "horizon",
    "status",
    "source_type",
    "source_report_id",
    "reason",
    "watch_conditions",
    "risk_summary",
    "created_at",
    "expires_at",
)

_ACCOUNT_ACTION_TO_PRIMARY_ACTION = {
    "observe": "watch",
    "open_candidate": "buy",
    "add_candidate": "add",
    "hold": "hold",
    "reduce_candidate": "reduce",
    "exit_candidate": "sell",
}


def resolve_decision_signal_primary_action(item: Any) -> tuple[Optional[str], Optional[str]]:
    """Resolve the execution-facing action and its authoritative source.

    Formal ``account_action`` is Policy-adjusted and therefore wins whenever it
    is present. Legacy summaries continue to fall back to ``action``. An
    unknown formal value fails closed instead of silently reviving the legacy
    action.
    """

    if not isinstance(item, dict):
        return None, None
    raw_account_action = item.get("account_action")
    if raw_account_action not in (None, ""):
        account_action = str(raw_account_action).strip().lower()
        return _ACCOUNT_ACTION_TO_PRIMARY_ACTION.get(account_action), "account_action"
    raw_action = item.get("action")
    if raw_action in (None, ""):
        return None, None
    return str(raw_action).strip().lower() or None, "legacy_action"


def summarize_decision_signal(item: Any) -> Optional[Dict[str, Any]]:
    """Return a low-sensitive summary from a serialized DecisionSignal item."""

    if not isinstance(item, dict):
        return None
    summary: Dict[str, Any] = {}
    for field_name in SUMMARY_FIELDS:
        value = item.get(field_name)
        if value in (None, "", [], {}):
            continue
        summary[field_name] = sanitize_decision_signal_payload(value)
    primary_action, primary_action_source = resolve_decision_signal_primary_action(item)
    if primary_action is not None:
        summary["primary_action"] = sanitize_decision_signal_payload(primary_action)
    if primary_action_source is not None:
        summary["primary_action_source"] = primary_action_source
    return summary or None


def format_decision_signal_excerpt(summary: Any, report_language: str = "zh") -> str:
    """Format a compact public DecisionSignal excerpt for notification text."""

    if not isinstance(summary, dict) or not summary:
        return ""
    language = "en" if str(report_language or "").lower().startswith("en") else "zh"
    labels = {
        "zh": {
            "heading": "AI 决策信号",
            "action": "动作",
            "horizon": "周期",
            "reason": "理由",
            "watch_conditions": "观察条件",
            "risk_summary": "风险",
            "source_report_id": "报告",
        },
        "en": {
            "heading": "AI decision signal",
            "action": "Action",
            "horizon": "Horizon",
            "reason": "Reason",
            "watch_conditions": "Watch",
            "risk_summary": "Risk",
            "source_report_id": "Report",
        },
    }[language]

    parts = []
    action_label = _public_scalar(summary.get("action_label") or summary.get("action"), max_length=32)
    if action_label:
        parts.append(f"{labels['action']}: {action_label}")
    horizon = _public_scalar(summary.get("horizon"), max_length=16)
    if horizon:
        parts.append(f"{labels['horizon']}: {horizon}")
    source_report_id = _public_scalar(summary.get("source_report_id"), max_length=24)
    if source_report_id:
        parts.append(f"{labels['source_report_id']}: #{source_report_id}")

    lines = [f"**{labels['heading']}**"]
    if parts:
        lines.append(" | ".join(parts))
    for key in ("reason", "watch_conditions", "risk_summary"):
        max_length = None if key == "reason" else 120
        text = _public_text(summary.get(key), max_length=max_length)
        if text:
            lines.append(f"- {labels[key]}: {text}")
    return "\n".join(lines)


def _public_scalar(value: Any, *, max_length: int) -> str:
    if value in (None, ""):
        return ""
    return sanitize_decision_signal_text(value)[:max_length]


def _public_text(value: Any, *, max_length: Optional[int]) -> str:
    if value in (None, "", [], {}):
        return ""
    if isinstance(value, (list, tuple)):
        text = "；".join(str(item).strip() for item in value if str(item or "").strip())
    elif isinstance(value, dict):
        text = "；".join(
            f"{key}: {item}"
            for key, item in value.items()
            if str(key or "").strip() and str(item or "").strip()
        )
    else:
        text = str(value).strip()
    sanitized = sanitize_decision_signal_text(text)
    return sanitized if max_length is None else sanitized[:max_length]
