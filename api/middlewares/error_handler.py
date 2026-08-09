# -*- coding: utf-8 -*-
"""
===================================
全局异常处理中间件
===================================

职责：
1. 捕获未处理的异常
2. 统一错误响应格式
3. 记录错误日志
"""

import logging
import re
import traceback
from typing import Any, Callable, Dict, List

from fastapi import Request, Response
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

logger = logging.getLogger(__name__)

_SAFE_VALIDATION_SOURCES = frozenset(
    {"body", "cookie", "header", "path", "query", "response"}
)
_SAFE_VALIDATION_TYPE_RE = re.compile(r"^[a-z][a-z0-9_.-]{0,127}$")
_SAFE_VALIDATION_LOCATION_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_.-]{0,127}$")
_SENSITIVE_VALIDATION_TEXT_RE = re.compile(
    r"(?i)(?:api[_-]?key|access[_-]?key|client[_-]?secret|password|passwd|"
    r"private[_-]?key|secret|session[_-]?id|token|bearer)"
)
_TOKEN_LIKE_VALIDATION_TEXT_RE = re.compile(
    r"(?i)(?:sk-[A-Za-z0-9_-]{16,}|gh[pousr]_[A-Za-z0-9_-]{16,})"
)
_RAW_URL_VALIDATION_TEXT_RE = re.compile(r"(?i)(?:https?://|www\.)")
_MAX_REQUEST_VALIDATION_ERRORS = 1_000


def _safe_validation_metadata(exc: Any) -> Dict[str, Any]:
    """Return bounded diagnostics without rejected values or dynamic field names."""

    try:
        errors = exc.errors()
    except Exception:
        errors = []
    sources = sorted(
        {
            location[0]
            for error in errors
            if isinstance(error, dict)
            and isinstance((location := error.get("loc")), (list, tuple))
            and location
            and isinstance(location[0], str)
            and location[0] in _SAFE_VALIDATION_SOURCES
        }
    )
    return {
        "error_count": min(len(errors), 10_000),
        "sources": sources,
    }


def _unsafe_validation_text(value: str) -> bool:
    return bool(
        _SENSITIVE_VALIDATION_TEXT_RE.search(value)
        or _TOKEN_LIKE_VALIDATION_TEXT_RE.search(value)
        or _RAW_URL_VALIDATION_TEXT_RE.search(value)
    )


def _safe_validation_type(raw_type: Any) -> str:
    value = str(raw_type or "validation_error")
    if _SAFE_VALIDATION_TYPE_RE.fullmatch(value):
        return value
    return "validation_error"


def _safe_validation_location(raw_location: Any, error_type: str) -> List[Any]:
    if not isinstance(raw_location, (list, tuple)):
        return []

    location: List[Any] = []
    for index, raw_segment in enumerate(raw_location[:32]):
        if isinstance(raw_segment, int) and raw_segment >= 0:
            location.append(raw_segment)
            continue
        if not isinstance(raw_segment, str):
            location.append("[redacted]")
            continue
        if index == 0 and raw_segment in _SAFE_VALIDATION_SOURCES:
            location.append(raw_segment)
            continue
        if error_type == "extra_forbidden":
            location.append("[redacted]")
            continue
        if (
            _SAFE_VALIDATION_LOCATION_RE.fullmatch(raw_segment)
            and not _unsafe_validation_text(raw_segment)
        ):
            location.append(raw_segment)
        else:
            location.append("[redacted]")
    return location


def _safe_validation_message(raw_message: Any, error_type: str) -> str:
    if error_type in {"assertion_error", "value_error"}:
        return "Invalid value"
    value = str(raw_message or "Invalid value")
    if len(value) > 300 or _unsafe_validation_text(value):
        return "Invalid value"
    return value


def _safe_request_validation_errors(exc: Any) -> List[Dict[str, Any]]:
    """Preserve FastAPI's 422 detail shape without reflecting rejected input."""

    try:
        errors = exc.errors()
    except Exception:
        errors = []

    safe_errors: List[Dict[str, Any]] = []
    for error in errors[:_MAX_REQUEST_VALIDATION_ERRORS]:
        if not isinstance(error, dict):
            continue
        error_type = _safe_validation_type(error.get("type"))
        safe_errors.append(
            {
                "type": error_type,
                "loc": _safe_validation_location(error.get("loc"), error_type),
                "msg": _safe_validation_message(error.get("msg"), error_type),
            }
        )
    return safe_errors


class ErrorHandlerMiddleware(BaseHTTPMiddleware):
    """
    全局异常处理中间件
    
    捕获所有未处理的异常，返回统一格式的错误响应
    """
    
    async def dispatch(
        self, 
        request: Request, 
        call_next: Callable
    ) -> Response:
        """
        处理请求，捕获异常
        
        Args:
            request: 请求对象
            call_next: 下一个处理器
            
        Returns:
            Response: 响应对象
        """
        try:
            response = await call_next(request)
            return response
            
        except Exception as e:
            # 记录错误日志
            logger.error(
                f"未处理的异常: {e}\n"
                f"请求路径: {request.url.path}\n"
                f"请求方法: {request.method}\n"
                f"堆栈: {traceback.format_exc()}"
            )
            
            # 返回统一格式的错误响应
            return JSONResponse(
                status_code=500,
                content={
                    "error": "internal_error",
                    "message": "服务器内部错误，请稍后重试",
                    "detail": str(e) if logger.isEnabledFor(logging.DEBUG) else None
                }
            )


def add_error_handlers(app) -> None:
    """
    添加全局异常处理器
    
    为 FastAPI 应用添加各类异常的处理器
    
    Args:
        app: FastAPI 应用实例
    """
    from fastapi import HTTPException
    from fastapi.exceptions import RequestValidationError, ResponseValidationError
    
    @app.exception_handler(HTTPException)
    async def http_exception_handler(request: Request, exc: HTTPException):
        """处理 HTTP 异常"""
        # 如果 detail 已经是 ErrorResponse 格式的 dict，直接使用
        if isinstance(exc.detail, dict) and "error" in exc.detail and "message" in exc.detail:
            return JSONResponse(
                status_code=exc.status_code,
                content=exc.detail
            )
        # 否则将 detail 包装成 ErrorResponse 格式
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "error": "http_error",
                "message": str(exc.detail) if exc.detail else "HTTP Error",
                "detail": None
            }
        )
    
    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(request: Request, exc: RequestValidationError):
        """处理请求验证异常"""
        metadata = _safe_validation_metadata(exc)
        safe_errors = _safe_request_validation_errors(exc)
        logger.info(
            "Request validation failed method=%s error_count=%d sources=%s; "
            "payload omitted",
            request.method,
            metadata["error_count"],
            ",".join(metadata["sources"]),
        )
        return JSONResponse(
            status_code=422,
            content={
                "error": "validation_error",
                "message": "请求参数验证失败",
                "detail": safe_errors,
            }
        )

    @app.exception_handler(ResponseValidationError)
    async def response_validation_exception_handler(
        request: Request,
        exc: ResponseValidationError,
    ):
        """Fail closed without logging or returning invalid response values."""

        metadata = _safe_validation_metadata(exc)
        logger.error(
            "Response validation failed method=%s error_count=%d sources=%s; "
            "payload omitted",
            request.method,
            metadata["error_count"],
            ",".join(metadata["sources"]),
        )
        return JSONResponse(
            status_code=500,
            content={
                "error": "internal_error",
                "message": "Response validation failed",
                "detail": metadata,
            },
        )
    
    @app.exception_handler(Exception)
    async def general_exception_handler(request: Request, exc: Exception):
        """处理通用异常"""
        logger.error(
            f"未处理的异常: {exc}\n"
            f"请求路径: {request.url.path}\n"
            f"堆栈: {traceback.format_exc()}"
        )
        return JSONResponse(
            status_code=500,
            content={
                "error": "internal_error",
                "message": "服务器内部错误",
                "detail": None
            }
        )
