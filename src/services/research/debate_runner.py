"""Bounded execution edge for the frozen research Debate request.

The runner performs at most one high-level completion for each missing stance.
Provider-internal retries or route fallbacks remain behind the injected
completion callable and do not alter the immutable request or its call count.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Mapping, Optional, Protocol, Sequence

from .debate_security import (
    strict_bounded_text,
    strict_error_code,
    strict_fingerprint,
    strict_model_identifier,
    validate_secret_safe_text,
)
from .debate_service import (
    DEBATE_STANCES,
    DebateBuildInput,
    DebateFailure,
    DebateTurnBuildInput,
    DebateTurnRequest,
    FrozenDebateRequest,
    FrozenDebateSnapshot,
    FrozenDebateTurn,
    build_debate_snapshot,
    build_debate_turn,
    validate_debate_request,
    validate_debate_snapshot,
    validate_debate_turn,
)


_MAX_ERROR_MESSAGE_CHARS = 500


@dataclass(frozen=True)
class DebateCompletionRequest:
    """Exact text-only provider request for one stance."""

    request_hash: str
    stance: str
    messages: tuple[Mapping[str, str], ...]
    prompt_fingerprint: str
    model_route_fingerprint: str
    output_schema_version: str

    def __post_init__(self) -> None:
        request_hash = strict_fingerprint(self.request_hash, field="request_hash")
        route_fingerprint = strict_fingerprint(
            self.model_route_fingerprint,
            field="model_route_fingerprint",
        )
        schema_version = strict_bounded_text(
            self.output_schema_version,
            field="output_schema_version",
            max_chars=64,
        )
        validated = DebateTurnRequest(
            stance=self.stance,
            messages=tuple(self.messages),
            prompt_fingerprint=self.prompt_fingerprint,
        )
        object.__setattr__(self, "request_hash", request_hash)
        object.__setattr__(self, "stance", validated.stance)
        object.__setattr__(self, "messages", validated.messages)
        object.__setattr__(
            self,
            "prompt_fingerprint",
            validated.prompt_fingerprint,
        )
        object.__setattr__(self, "model_route_fingerprint", route_fingerprint)
        object.__setattr__(self, "output_schema_version", schema_version)

    @classmethod
    def from_frozen_request(
        cls,
        request: FrozenDebateRequest,
        stance: str,
    ) -> "DebateCompletionRequest":
        validate_debate_request(request)
        turn_request = request.request_for(stance)
        return cls(
            request_hash=request.request_hash,
            stance=turn_request.stance,
            messages=turn_request.messages,
            prompt_fingerprint=turn_request.prompt_fingerprint,
            model_route_fingerprint=request.model_route_fingerprint,
            output_schema_version=request.output_schema_version,
        )


@dataclass(frozen=True)
class DebateCompletionResult:
    """Successful provider response; output is validated by the domain layer."""

    output: Any
    model_used: str

    def __post_init__(self) -> None:
        model = strict_model_identifier(
            self.model_used,
            field="model_used",
        )
        object.__setattr__(self, "model_used", model)


class DebateCompletionError(RuntimeError):
    """Base class for deliberately classified completion-edge failures."""

    retryable = False

    def __init__(self, error_code: str, message: Optional[str] = None) -> None:
        self.error_code = strict_error_code(error_code, field="error_code")
        detail = message if message is not None else self.error_code
        detail = strict_bounded_text(
            detail,
            field="error_message",
            max_chars=_MAX_ERROR_MESSAGE_CHARS,
        )
        validate_secret_safe_text(detail, field="error_message")
        super().__init__(detail)


class DebateTransientError(DebateCompletionError):
    """Retryable provider failure; the durable job must retry later."""

    retryable = True


class DebateTerminalError(DebateCompletionError):
    """Non-retryable stance failure; the other stance still gets one call."""


class DebateCompletion(Protocol):
    def __call__(
        self,
        request: DebateCompletionRequest,
    ) -> DebateCompletionResult:
        ...


@dataclass(frozen=True)
class DebateRunResult:
    request: FrozenDebateRequest
    snapshot: FrozenDebateSnapshot
    turns: tuple[FrozenDebateTurn, ...]
    new_turns: tuple[FrozenDebateTurn, ...]
    failures: tuple[DebateFailure, ...]
    new_failures: tuple[DebateFailure, ...]
    calls_made: int

    def __post_init__(self) -> None:
        validate_debate_request(self.request)
        validate_debate_snapshot(self.snapshot)
        turns = tuple(self.turns)
        new_turns = tuple(self.new_turns)
        failures = tuple(self.failures)
        new_failures = tuple(self.new_failures)
        if turns != self.snapshot.turns:
            raise ValueError("run result turns must match the final Debate snapshot")
        if failures != self.snapshot.failures:
            raise ValueError("run result failures must match the final Debate snapshot")
        if self.snapshot.request_hash != self.request.request_hash:
            raise ValueError("run result snapshot belongs to another Debate request")
        if isinstance(self.calls_made, bool) or not isinstance(self.calls_made, int):
            raise TypeError("calls_made must be an integer")
        if not 0 <= self.calls_made <= len(DEBATE_STANCES):
            raise ValueError("calls_made must be between zero and two")
        for turn in turns:
            validate_debate_turn(turn)
        final_hashes = {turn.turn_hash for turn in turns}
        if any(turn.turn_hash not in final_hashes for turn in new_turns):
            raise ValueError("new_turns must be a subset of final turns")
        if len({turn.stance for turn in new_turns}) != len(new_turns):
            raise ValueError("new_turns cannot contain duplicate stances")
        failure_stances = {failure.stance for failure in failures}
        if len(failure_stances) != len(failures):
            raise ValueError("failures cannot contain duplicate stances")
        if any(failure.stance not in failure_stances for failure in new_failures):
            raise ValueError("new_failures must be a subset of final failures")
        if len({failure.stance for failure in new_failures}) != len(new_failures):
            raise ValueError("new_failures cannot contain duplicate stances")
        if self.calls_made != len(new_turns) + len(new_failures):
            raise ValueError("every high-level completion must resolve to a turn or failure")
        object.__setattr__(self, "turns", turns)
        object.__setattr__(self, "new_turns", new_turns)
        object.__setattr__(self, "failures", failures)
        object.__setattr__(self, "new_failures", new_failures)


def _existing_turns_by_stance(
    request: FrozenDebateRequest,
    existing_turns: Sequence[FrozenDebateTurn],
) -> dict[str, FrozenDebateTurn]:
    by_stance: dict[str, FrozenDebateTurn] = {}
    for turn in existing_turns:
        if not isinstance(turn, FrozenDebateTurn):
            raise TypeError("existing_turns must contain FrozenDebateTurn values")
        validate_debate_turn(turn)
        if turn.request_hash != request.request_hash:
            raise ValueError("existing Debate turn belongs to another request")
        if turn.stance in by_stance:
            raise ValueError(f"duplicate existing Debate stance: {turn.stance}")
        by_stance[turn.stance] = turn
    return by_stance


def _existing_failures_by_stance(
    existing_failures: Sequence[DebateFailure],
) -> dict[str, DebateFailure]:
    by_stance: dict[str, DebateFailure] = {}
    for failure in existing_failures:
        if not isinstance(failure, DebateFailure):
            raise TypeError("existing_failures must contain DebateFailure values")
        if failure.stance in by_stance:
            raise ValueError(f"duplicate existing Debate failure: {failure.stance}")
        by_stance[failure.stance] = failure
    return by_stance


def _has_citable_evidence(request: FrozenDebateRequest) -> bool:
    evidence = request._evidence_snapshot
    if evidence is None:
        raise ValueError("running Debate requires the frozen Evidence object")
    return any(claim.citation_ids for claim in evidence.claims)


def run_research_debate(
    request: FrozenDebateRequest,
    completion: DebateCompletion,
    *,
    existing_turns: Sequence[FrozenDebateTurn] = (),
    existing_failures: Sequence[DebateFailure] = (),
    on_turn: Optional[Callable[[FrozenDebateTurn], None]] = None,
    on_failure: Optional[Callable[[DebateFailure], None]] = None,
) -> DebateRunResult:
    """Run Bull then Bear, once each at most, from the exact frozen messages."""

    validate_debate_request(request)
    if not callable(completion):
        raise TypeError("completion must be callable")
    if on_turn is not None and not callable(on_turn):
        raise TypeError("on_turn must be callable")
    if on_failure is not None and not callable(on_failure):
        raise TypeError("on_failure must be callable")

    by_stance = _existing_turns_by_stance(request, tuple(existing_turns))
    failures_by_stance = _existing_failures_by_stance(tuple(existing_failures))
    overlap = set(by_stance).intersection(failures_by_stance)
    if overlap:
        raise ValueError(
            "Debate stances cannot have both a persisted turn and failure: "
            + ",".join(sorted(overlap))
        )
    if not _has_citable_evidence(request):
        if by_stance or failures_by_stance:
            raise ValueError("empty Evidence cannot resume persisted Debate resolutions")
        snapshot = build_debate_snapshot(DebateBuildInput(request=request))
        return DebateRunResult(
            request=request,
            snapshot=snapshot,
            turns=snapshot.turns,
            new_turns=(),
            failures=(),
            new_failures=(),
            calls_made=0,
        )

    new_turns: list[FrozenDebateTurn] = []
    new_failures: list[DebateFailure] = []
    calls_made = 0

    def _record_failure(failure: DebateFailure) -> None:
        if on_failure is not None:
            on_failure(failure)
        failures_by_stance[failure.stance] = failure
        new_failures.append(failure)

    for stance in DEBATE_STANCES:
        if stance in by_stance or stance in failures_by_stance:
            continue
        completion_request = DebateCompletionRequest.from_frozen_request(
            request,
            stance,
        )
        calls_made += 1
        try:
            result = completion(completion_request)
        except DebateTransientError:
            raise
        except DebateTerminalError as exc:
            _record_failure(
                DebateFailure(stance=stance, error_code=exc.error_code)
            )
            continue

        if not isinstance(result, DebateCompletionResult):
            _record_failure(
                DebateFailure(
                    stance=stance,
                    error_code="invalid_completion_result",
                )
            )
            continue
        try:
            turn = build_debate_turn(
                DebateTurnBuildInput(
                    request=request,
                    stance=stance,
                    output=result.output,
                    model_used=result.model_used,
                )
            )
        except (TypeError, ValueError):
            _record_failure(
                DebateFailure(stance=stance, error_code="invalid_debate_output")
            )
            continue
        if on_turn is not None:
            on_turn(turn)
        by_stance[stance] = turn
        new_turns.append(turn)

    final_turns = tuple(
        by_stance[stance] for stance in DEBATE_STANCES if stance in by_stance
    )
    final_failures = tuple(
        failures_by_stance[stance]
        for stance in DEBATE_STANCES
        if stance in failures_by_stance
    )
    snapshot = build_debate_snapshot(
        DebateBuildInput(
            request=request,
            turns=final_turns,
            failures=final_failures,
        )
    )
    return DebateRunResult(
        request=request,
        snapshot=snapshot,
        turns=snapshot.turns,
        new_turns=tuple(new_turns),
        failures=snapshot.failures,
        new_failures=tuple(new_failures),
        calls_made=calls_made,
    )


__all__ = [
    "DebateCompletion",
    "DebateCompletionError",
    "DebateCompletionRequest",
    "DebateCompletionResult",
    "DebateRunResult",
    "DebateTerminalError",
    "DebateTransientError",
    "run_research_debate",
]
