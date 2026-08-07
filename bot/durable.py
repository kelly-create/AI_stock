"""Small, secret-safe helpers for submitting bot work to the durable queue."""

from __future__ import annotations

from typing import Any, Dict, Optional

from bot.models import BotMessage, BotResponse, ChatType


def get_durable_bot_queue() -> Optional[Any]:
    """Return the restart-latched durable queue without touching the legacy path."""

    from src.config import get_config

    if getattr(get_config(), "durable_jobs_enabled", False) is not True:
        return None

    from src.services.task_queue import get_task_queue

    queue = get_task_queue()
    if queue.durable_enabled is not True:
        raise RuntimeError(
            "DURABLE_JOBS_ENABLED changed after process startup; restart is required"
        )
    return queue


def build_bot_target(message: BotMessage) -> Dict[str, str]:
    """Persist only the stable, non-secret fields needed for an asynchronous reply."""

    platform = getattr(message, "platform", "")
    if hasattr(platform, "value"):
        platform = platform.value
    target = {
        "platform": str(platform or "").strip().lower(),
        "chat_id": str(getattr(message, "chat_id", "") or "").strip(),
        "message_id": str(getattr(message, "message_id", "") or "").strip(),
    }
    if target["platform"] == "dingtalk":
        raise ValueError("DingTalk 临时会话暂不支持持久任务回推，任务未提交")
    missing = [key for key, value in target.items() if not value]
    if missing:
        raise ValueError(
            "durable bot delivery requires " + ", ".join(sorted(missing))
        )
    return target


def bot_idempotency_key(target: Dict[str, str], command: str) -> str:
    """Build the stable platform/message/command idempotency contract."""

    normalized_command = str(command or "").strip().lower()
    if not normalized_command:
        raise ValueError("command is required for durable bot idempotency")
    return "bot:{platform}:{message_id}:{command}".format(
        platform=target["platform"],
        message_id=target["message_id"],
        command=normalized_command,
    )


def accepted_bot_response(task_id: str, label: str) -> BotResponse:
    """Return the immediate acknowledgement shared by durable bot commands."""

    return BotResponse.markdown_response(
        f"✅ **{label}已接受**\n\n任务 ID: `{task_id}`\n\n完成后将自动回复结果。"
    )


def bot_message_from_target(target: Any) -> BotMessage:
    """Reconstruct the minimal runtime message used only for outbox routing."""

    if hasattr(target, "model_dump"):
        target = target.model_dump(mode="python")
    if not isinstance(target, dict):
        raise TypeError("bot target must be an object")
    return BotMessage(
        platform=str(target.get("platform") or ""),
        message_id=str(target.get("message_id") or ""),
        user_id="",
        user_name="",
        chat_id=str(target.get("chat_id") or ""),
        chat_type=ChatType.UNKNOWN,
        content="",
        raw_content="",
        raw_data={},
    )


__all__ = [
    "accepted_bot_response",
    "bot_idempotency_key",
    "bot_message_from_target",
    "build_bot_target",
    "get_durable_bot_queue",
]
