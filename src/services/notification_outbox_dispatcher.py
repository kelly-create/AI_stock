"""Serial durable notification delivery with at-most-once ambiguity handling."""

from __future__ import annotations

import argparse
import hashlib
import logging
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Mapping, Optional

from src.notification import NotificationChannel, NotificationService
from src.services.durable_jobs import (
    OUTBOX_STATUS_DELIVERY_UNKNOWN,
    OUTBOX_STATUS_FAILED,
    OUTBOX_STATUS_PENDING,
    OUTBOX_STATUS_SENT,
    ClaimedOutboxMessage,
    NotificationOutboxStore,
    StaleLeaseError,
)
from src.utils.sanitize import sanitize_diagnostic_text

logger = logging.getLogger(__name__)


class InvalidOutboxPayloadError(ValueError):
    """Raised before delivery when a persisted envelope is not v1-compatible."""


class NotificationDeliveryError(RuntimeError):
    """Explicit provider rejection whose retry safety is known by the caller.

    Setting ``retryable=True`` is an assertion that no request could have been
    accepted externally. Ambiguous transport/provider outcomes must use a
    generic exception so the dispatcher records ``delivery_unknown``.
    """

    def __init__(
        self,
        message: str,
        *,
        error_code: str = "provider_error",
        retryable: bool = False,
        retry_after: Any = None,
    ) -> None:
        super().__init__(message)
        self.error_code = str(error_code or "provider_error")
        self.retryable = bool(retryable)
        self.retry_after = retry_after


@dataclass(frozen=True)
class OutboxDispatchResult:
    """One serial claim/delivery outcome."""

    status: str
    outbox_id: Optional[int] = None
    channel: Optional[str] = None
    attempt: Optional[int] = None
    message: Optional[str] = None


@dataclass(frozen=True)
class _FailureDisposition:
    status: str
    error_code: str
    message: str
    retry_after: Any = None


class NotificationOutboxDispatcher:
    """Claim and deliver one channel row at a time.

    A claimed row is already considered externally ambiguous. If this process
    exits after the provider call but before ``mark_sent``, its expired lease is
    terminally converted to ``delivery_unknown`` and is never reclaimed.
    """

    def __init__(
        self,
        store: Optional[NotificationOutboxStore] = None,
        *,
        worker_id: Optional[str] = None,
        service_factory: Callable[[], NotificationService] = NotificationService,
        after_external_send: Optional[Callable[[ClaimedOutboxMessage], None]] = None,
        health_recorder: Optional[Any] = None,
    ) -> None:
        self.store = store or NotificationOutboxStore()
        self.worker_id = worker_id or f"notification-outbox-{uuid.uuid4().hex[:12]}"
        self.service_factory = service_factory
        self.after_external_send = after_external_send
        self.health_recorder = health_recorder

    def dispatch_once(self, *, now: Optional[datetime] = None) -> OutboxDispatchResult:
        """Expire old ambiguity, then claim and process at most one row."""
        self.store.expire_ambiguous_leases(now=now)
        claimed = self.store.claim_next(self.worker_id, now=now)
        if claimed is None:
            return OutboxDispatchResult(status="idle")

        try:
            service = self.service_factory()
            delivered = self._deliver(service, claimed)
        except InvalidOutboxPayloadError as exc:
            return self._mark_known_failure(
                claimed,
                _FailureDisposition(
                    status=OUTBOX_STATUS_FAILED,
                    error_code="invalid_outbox_payload",
                    message=sanitize_diagnostic_text(exc),
                ),
                now=now,
            )
        except NotificationDeliveryError as exc:
            return self._mark_known_failure(
                claimed,
                _FailureDisposition(
                    status=OUTBOX_STATUS_PENDING if exc.retryable else OUTBOX_STATUS_FAILED,
                    error_code=exc.error_code,
                    message=sanitize_diagnostic_text(exc),
                    retry_after=exc.retry_after,
                ),
                now=now,
            )
        except Exception as exc:
            disposition = self._classify_provider_exception(exc)
            if disposition.status == OUTBOX_STATUS_DELIVERY_UNKNOWN:
                self.store.mark_delivery_unknown(
                    claimed.id,
                    claimed.worker_id,
                    claimed.lease_token,
                    disposition.error_code,
                    disposition.message,
                    now=now,
                )
                return self._result(
                    claimed,
                    OUTBOX_STATUS_DELIVERY_UNKNOWN,
                    disposition.message,
                    error_code=disposition.error_code,
                )
            return self._mark_known_failure(claimed, disposition, now=now)

        if not delivered:
            # Legacy channel senders commonly collapse connection failures,
            # provider errors, and post-acceptance response loss into ``False``.
            # That result cannot prove the notification was not accepted, so
            # strict at-most-once delivery makes it terminally ambiguous.
            self.store.mark_delivery_unknown(
                claimed.id,
                claimed.worker_id,
                claimed.lease_token,
                "provider_outcome_unknown",
                "Notification provider returned no positive acknowledgement.",
                now=now,
            )
            return self._result(
                claimed,
                OUTBOX_STATUS_DELIVERY_UNKNOWN,
                "Notification provider returned no positive acknowledgement.",
                error_code="provider_outcome_unknown",
            )

        # Test/runtime fault injection deliberately lives outside the provider
        # exception block. A crash here must leave the processing lease intact.
        if self.after_external_send is not None:
            self.after_external_send(claimed)

        try:
            self.store.mark_sent(
                claimed.id,
                claimed.worker_id,
                claimed.lease_token,
                now=now,
            )
        except StaleLeaseError:
            try:
                self.store.mark_delivery_unknown(
                    claimed.id,
                    claimed.worker_id,
                    claimed.lease_token,
                    "delivery_commit_lease_lost",
                    "Notification was sent, but its delivery lease expired before confirmation.",
                    now=now,
                )
            except StaleLeaseError:
                pass
            return self._result(
                claimed,
                OUTBOX_STATUS_DELIVERY_UNKNOWN,
                "delivery confirmation lease was lost",
                error_code="delivery_commit_lease_lost",
            )
        return self._result(claimed, OUTBOX_STATUS_SENT)

    def dispatch_available(
        self,
        *,
        max_messages: int = 100,
        now: Optional[datetime] = None,
    ) -> list[OutboxDispatchResult]:
        """Drain currently claimable rows serially up to a strict bound."""
        if isinstance(max_messages, bool) or not isinstance(max_messages, int) or max_messages < 1:
            raise ValueError("max_messages must be a positive integer")
        results: list[OutboxDispatchResult] = []
        for _ in range(max_messages):
            result = self.dispatch_once(now=now)
            if result.status == "idle":
                break
            results.append(result)
        return results

    def run_forever(
        self,
        *,
        stop_event: Optional[threading.Event] = None,
        poll_interval_seconds: float = 1.0,
    ) -> None:
        """Run the serial dispatcher until the supplied event is set."""
        if poll_interval_seconds <= 0:
            raise ValueError("poll_interval_seconds must be positive")
        stop = stop_event or threading.Event()
        while not stop.is_set():
            result = self.dispatch_once()
            if result.status == "idle":
                stop.wait(poll_interval_seconds)

    def _deliver(
        self,
        service: NotificationService,
        claimed: ClaimedOutboxMessage,
    ) -> bool:
        payload = self._validate_payload(claimed.payload)
        kind = payload["kind"]
        content = payload["content"]
        if kind == "context":
            return self._deliver_context(service, claimed, payload, content)

        try:
            channel = NotificationChannel(claimed.channel)
        except ValueError as exc:
            raise InvalidOutboxPayloadError(
                f"unsupported static notification channel {claimed.channel!r}"
            ) from exc
        target = payload.get("delivery_target") or {}
        expected_recipient = service._static_recipient_identity(channel, target)
        if expected_recipient != claimed.recipient:
            raise NotificationDeliveryError(
                "Notification destination changed after enqueue; refusing to reroute it.",
                error_code="recipient_configuration_changed",
                retryable=False,
            )

        image_bytes = None
        delivery_mode = payload.get("delivery_mode") or "text"
        if delivery_mode == "image":
            try:
                from src.md2img import markdown_to_image

                image_bytes = markdown_to_image(
                    content,
                    max_chars=int(payload.get("image_max_chars") or 15000),
                    structured_payload=payload.get("structured_payload"),
                )
            except Exception as exc:
                logger.warning(
                    "Durable notification image rendering failed; using text: %s",
                    sanitize_diagnostic_text(exc),
                )

        delivered = bool(
            service._send_to_static_channel(
                channel,
                content,
                image_bytes=image_bytes,
                email_stock_codes=None,
                email_send_to_all=False,
                route_type=claimed.route,
                delivery_target=target,
                delivery_mode=delivery_mode,
            )
        )
        if delivered:
            service.record_durable_noise_control(payload.get("noise_control"))
        return delivered

    @staticmethod
    def _deliver_context(
        service: NotificationService,
        claimed: ClaimedOutboxMessage,
        payload: Mapping[str, Any],
        content: str,
    ) -> bool:
        target = payload.get("target")
        if not isinstance(target, Mapping):
            raise InvalidOutboxPayloadError("context target must be an object")
        if set(target) - {"platform", "chat_id", "message_id"}:
            raise InvalidOutboxPayloadError("context target contains forbidden fields")
        platform = str(target.get("platform") or "").strip().lower()
        chat_id = str(target.get("chat_id") or "").strip()
        if not chat_id:
            raise InvalidOutboxPayloadError("context target chat_id is required")
        if NotificationService._context_recipient_identity(target) != claimed.recipient:
            raise InvalidOutboxPayloadError("context recipient fingerprint does not match")

        if claimed.channel == "context_feishu" and platform == "feishu":
            return bool(service._send_feishu_stream_reply(chat_id, content))
        if claimed.channel == "context_telegram" and platform == "telegram":
            return bool(service.send_to_telegram(content, chat_id=chat_id))
        raise InvalidOutboxPayloadError("context channel and platform do not match")

    @classmethod
    def _validate_payload(cls, value: Any) -> Mapping[str, Any]:
        if not isinstance(value, Mapping):
            raise InvalidOutboxPayloadError("outbox payload must be an object")
        if value.get("version") != 1:
            raise InvalidOutboxPayloadError("unsupported outbox payload version")
        if value.get("kind") not in {"static", "context"}:
            raise InvalidOutboxPayloadError("unsupported outbox payload kind")
        if not isinstance(value.get("content"), str):
            raise InvalidOutboxPayloadError("outbox content must be text")
        expected_hash = hashlib.sha256(value["content"].encode("utf-8")).hexdigest()
        if value.get("content_sha256") != expected_hash:
            raise InvalidOutboxPayloadError("outbox content hash does not match")
        cls._reject_forbidden_keys(value)
        return value

    @classmethod
    def _reject_forbidden_keys(cls, value: Any) -> None:
        if isinstance(value, Mapping):
            for key, item in value.items():
                compact = "".join(ch for ch in str(key).lower() if ch.isalnum())
                if compact in {
                    "rawdata",
                    "token",
                    "bottoken",
                    "apitoken",
                    "accesstoken",
                    "refreshtoken",
                    "webhook",
                    "webhookurl",
                    "password",
                    "secret",
                    "authorization",
                    "cookie",
                }:
                    raise InvalidOutboxPayloadError(
                        f"outbox payload contains forbidden field {key!r}"
                    )
                cls._reject_forbidden_keys(item)
        elif isinstance(value, list):
            for item in value:
                cls._reject_forbidden_keys(item)

    def _mark_known_failure(
        self,
        claimed: ClaimedOutboxMessage,
        disposition: _FailureDisposition,
        *,
        now: Optional[datetime],
    ) -> OutboxDispatchResult:
        retryable = disposition.status == OUTBOX_STATUS_PENDING
        status = self.store.mark_failed(
            claimed.id,
            claimed.worker_id,
            claimed.lease_token,
            disposition.error_code,
            disposition.message,
            retryable=retryable,
            retry_after=disposition.retry_after,
            now=now,
        )
        return self._result(
            claimed,
            status,
            disposition.message,
            error_code=disposition.error_code,
        )

    @staticmethod
    def _classify_provider_exception(exc: Exception) -> _FailureDisposition:
        response = getattr(exc, "response", None)
        status_code = getattr(exc, "status_code", None)
        if status_code is None and response is not None:
            status_code = getattr(response, "status_code", None)
        retry_after = getattr(exc, "retry_after", None)
        if retry_after is None and response is not None:
            headers = getattr(response, "headers", {}) or {}
            retry_after = headers.get("Retry-After")
        message = sanitize_diagnostic_text(exc) or "Notification provider failure."

        if isinstance(status_code, int):
            if status_code == 429:
                return _FailureDisposition(
                    status=OUTBOX_STATUS_PENDING,
                    error_code=f"provider_http_{status_code}",
                    message=message,
                    retry_after=retry_after,
                )
            if status_code >= 500:
                # A 5xx can be observed after the provider accepted the body;
                # replaying it would violate the outbox at-most-once contract.
                return _FailureDisposition(
                    status=OUTBOX_STATUS_DELIVERY_UNKNOWN,
                    error_code=f"provider_http_{status_code}",
                    message=message,
                )
            return _FailureDisposition(
                status=OUTBOX_STATUS_FAILED,
                error_code=f"provider_http_{status_code}",
                message=message,
            )
        # An exception without an explicit provider rejection can occur after
        # bytes left the process, so at-most-once policy makes it terminally
        # ambiguous rather than automatically retrying it.
        return _FailureDisposition(
            status=OUTBOX_STATUS_DELIVERY_UNKNOWN,
            error_code="provider_outcome_unknown",
            message=message,
        )

    def _result(
        self,
        claimed: ClaimedOutboxMessage,
        status: str,
        message: Optional[str] = None,
        *,
        error_code: Optional[str] = None,
    ) -> OutboxDispatchResult:
        self._record_health(
            claimed,
            status=status,
            error_code=error_code,
            message=message,
        )
        return OutboxDispatchResult(
            status=status,
            outbox_id=claimed.id,
            channel=claimed.channel,
            attempt=claimed.attempt,
            message=message,
        )

    def _record_health(
        self,
        claimed: ClaimedOutboxMessage,
        *,
        status: str,
        error_code: Optional[str],
        message: Optional[str],
    ) -> None:
        """Project delivery attempts without influencing routing or retries."""
        recorder = self.health_recorder
        if recorder is None:
            return
        success = status == OUTBOX_STATUS_SENT
        health_status = "healthy" if success else (
            "degraded" if status == OUTBOX_STATUS_PENDING else "failing"
        )
        try:
            recorder.record_health(
                kind="notification",
                provider_key=claimed.channel,
                scope=claimed.route,
                status=health_status,
                success=success,
                error_code=None if success else (error_code or f"outbox_{status}"),
                error_message_sanitized=None if success else (message or "Notification failed."),
                metadata={
                    "attempt": claimed.attempt,
                    "outbox_status": status,
                },
            )
        except Exception as exc:  # noqa: BLE001 - telemetry must never affect delivery.
            logger.warning(
                "Notification provider-health projection failed: channel=%s message=%s",
                claimed.channel,
                sanitize_diagnostic_text(exc),
            )


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Run the durable notification outbox dispatcher")
    parser.add_argument("--once", action="store_true", help="Process currently available rows and exit")
    parser.add_argument("--poll-interval", type=float, default=1.0)
    args = parser.parse_args(argv)

    from src.config import get_config

    if not bool(getattr(get_config(), "durable_jobs_enabled", False)):
        raise RuntimeError("DURABLE_JOBS_ENABLED must be true for the outbox dispatcher")
    dispatcher = NotificationOutboxDispatcher()
    if args.once:
        dispatcher.dispatch_available()
        return 0
    dispatcher.run_forever(poll_interval_seconds=args.poll_interval)
    return 0


if __name__ == "__main__":  # pragma: no cover - exercised through the service API.
    raise SystemExit(main())
