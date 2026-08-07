"""Bot commands enqueue secret-safe typed jobs when durable mode is active."""

from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace
from unittest import mock

from bot.commands.analyze import AnalyzeCommand
from bot.commands.ask import AskCommand
from bot.commands.batch import BatchCommand
from bot.commands.market import MarketCommand
from bot.commands.research import ResearchCommand
from bot.models import BotMessage, ChatType


class _Queue:
    durable_enabled = True

    def __init__(self) -> None:
        self.calls: list[tuple[tuple, dict]] = []

    def submit_typed_job(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        return SimpleNamespace(task_id="durable-bot-task")


def _message(command: str = "analyze", *, platform: str = "feishu") -> BotMessage:
    return BotMessage(
        platform=platform,
        message_id="message-123",
        user_id="sensitive-user",
        user_name="Sensitive Name",
        chat_id="chat-456",
        chat_type=ChatType.PRIVATE,
        content=f"/{command} user supplied content",
        raw_content="raw user supplied content",
        raw_data={"sessionWebhook": "https://secret.invalid/token"},
        timestamp=datetime.now(),
    )


def _assert_safe_target(payload: dict) -> None:
    target = payload.get("bot_target") or payload.get("target")
    assert target == {
        "platform": "feishu",
        "chat_id": "chat-456",
        "message_id": "message-123",
    }
    serialized = repr(payload)
    assert "sensitive-user" not in serialized
    assert "Sensitive Name" not in serialized
    assert "sessionWebhook" not in serialized
    assert "secret.invalid" not in serialized
    assert "raw user supplied content" not in serialized


def test_analyze_enqueues_stock_job_without_task_service() -> None:
    queue = _Queue()
    command = AnalyzeCommand()

    with (
        mock.patch("bot.commands.analyze.get_durable_bot_queue", return_value=queue),
        mock.patch.dict(
            "sys.modules",
            {"src.services.task_service": mock.Mock(side_effect=AssertionError("legacy task service used"))},
        ),
    ):
        response = command.execute(_message(), ["600519", "full"])

    args, kwargs = queue.calls[0]
    assert args[0] == "stock_analysis"
    assert args[1]["stock_code"] == "600519"
    _assert_safe_target(args[1])
    assert kwargs["idempotency_key"] == "bot:feishu:message-123:analyze"
    assert kwargs["notify"] is True
    assert "durable-bot-task" in response.text


def test_batch_enqueues_scheduled_orchestration_without_daemon_thread() -> None:
    queue = _Queue()
    config = SimpleNamespace(
        stock_list=["600519", "000858"],
        refresh_stock_list=mock.Mock(),
    )
    command = BatchCommand()

    with (
        mock.patch("src.config.get_config", return_value=config),
        mock.patch("bot.commands.batch.get_durable_bot_queue", return_value=queue),
        mock.patch(
            "bot.commands.batch.threading.Thread",
            side_effect=AssertionError("durable command started a thread"),
        ),
    ):
        response = command.execute(_message("batch"), ["1"])

    args, kwargs = queue.calls[0]
    assert args[0] == "scheduled_analysis"
    assert args[1]["stock_codes"] == ["600519"]
    assert args[1]["single_notify"] is True
    _assert_safe_target(args[1])
    assert kwargs["idempotency_key"] == "bot:feishu:message-123:batch"
    assert "durable-bot-task" in response.text


def test_market_enqueues_worker_owned_review_without_command_lock() -> None:
    queue = _Queue()
    config = SimpleNamespace(
        market_review_region="both",
        trading_day_check_enabled=True,
    )
    command = MarketCommand()

    with (
        mock.patch.object(command, "_get_config", return_value=config),
        mock.patch("bot.commands.market.get_durable_bot_queue", return_value=queue),
        mock.patch.object(
            command,
            "_try_acquire_market_review_lock",
            side_effect=AssertionError("durable command acquired the market lock"),
        ),
    ):
        response = command.execute(_message("market"), [])

    args, kwargs = queue.calls[0]
    assert args[0] == "market_review"
    assert args[1]["trigger_source"] == "bot"
    assert args[1]["apply_trading_day_filter"] is True
    _assert_safe_target(args[1])
    assert kwargs["idempotency_key"] == "bot:feishu:message-123:market"
    assert "durable-bot-task" in response.text


def test_ask_enqueues_versioned_job_without_agent_execution() -> None:
    queue = _Queue()
    command = AskCommand()

    with (
        mock.patch("bot.commands.ask.get_config", return_value=SimpleNamespace(agent_mode=True)),
        mock.patch("bot.commands.ask.get_durable_bot_queue", return_value=queue),
        mock.patch.object(command, "_get_default_skill_id", return_value="bull_trend"),
        mock.patch.object(
            command,
            "_execute_parsed",
            side_effect=AssertionError("durable command executed the agent"),
        ),
    ):
        response = command.execute(_message("ask"), ["600519"])

    args, kwargs = queue.calls[0]
    assert args[0] == "bot_ask"
    assert args[1]["stock_codes"] == ["600519"]
    assert args[1]["skill_id"] == "bull_trend"
    _assert_safe_target(args[1])
    assert kwargs["idempotency_key"] == "bot:feishu:message-123:ask"
    assert "durable-bot-task" in response.text


def test_research_enqueues_required_query_without_research_agent() -> None:
    queue = _Queue()
    command = ResearchCommand()

    with (
        mock.patch("bot.commands.research.get_config", return_value=SimpleNamespace(agent_mode=True)),
        mock.patch("bot.commands.research.get_durable_bot_queue", return_value=queue),
        mock.patch.object(
            command,
            "_run_research",
            side_effect=AssertionError("durable command executed research"),
        ),
    ):
        response = command.execute(_message("research"), ["600519", "近期风险"])

    args, kwargs = queue.calls[0]
    assert args[0] == "bot_research"
    assert args[1]["stock_code"] == "600519"
    assert args[1]["question"] == "[Stock: 600519] 近期风险"
    _assert_safe_target(args[1])
    assert kwargs["idempotency_key"] == "bot:feishu:message-123:research"
    assert "durable-bot-task" in response.text


def test_analyze_flag_off_preserves_task_service_path() -> None:
    service = mock.Mock()
    service.submit_analysis.return_value = {
        "success": True,
        "task_id": "legacy-task",
    }
    command = AnalyzeCommand()

    with mock.patch("bot.commands.analyze.get_durable_bot_queue", return_value=None), \
         mock.patch("src.services.task_service.get_task_service", return_value=service):
        response = command.execute(_message(), ["600519"])

    service.submit_analysis.assert_called_once()
    assert service.submit_analysis.call_args.kwargs["source_message"].user_id == "sensitive-user"
    assert "legacy-task" in response.text


def test_batch_flag_off_preserves_daemon_thread_path() -> None:
    config = SimpleNamespace(
        stock_list=["600519"],
        refresh_stock_list=mock.Mock(),
    )
    fake_thread = mock.Mock()
    command = BatchCommand()

    with mock.patch("src.config.get_config", return_value=config), \
         mock.patch("bot.commands.batch.get_durable_bot_queue", return_value=None), \
         mock.patch("bot.commands.batch.threading.Thread", return_value=fake_thread) as thread_cls:
        response = command.execute(_message("batch"), [])

    thread_cls.assert_called_once()
    assert thread_cls.call_args.kwargs["daemon"] is True
    fake_thread.start.assert_called_once_with()
    assert "已启动" in response.text


def test_dingtalk_durable_command_fails_before_enqueue() -> None:
    queue = _Queue()
    command = AnalyzeCommand()

    with mock.patch("bot.commands.analyze.get_durable_bot_queue", return_value=queue):
        response = command.execute(
            _message(platform="dingtalk"),
            ["600519"],
        )

    assert queue.calls == []
    assert "DingTalk 临时会话暂不支持持久任务回推" in response.text
